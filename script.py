import argparse
import copy
import json
import logging
import os
import re
from datetime import datetime
from os import path
from openpyxl import Workbook
from openpyxl.styles import Alignment
import xlsxwriter

from data_uploader_dev.data_uploader import model_interface
from txt_report_gen.parse_fortis import generate_text_report

output_location = "txt_report_gen/cxrjsons/"
now = datetime.now()
log_file = path.join(output_location, f'get_{now.strftime("%Y%m%d")}.log')
logging.basicConfig(
    level=logging.DEBUG,
    filename=log_file,
    filemode='a',
    format='%(asctime)s %(name) -14s - %(levelname) -8s - %(message)s',
    datefmt='%H:%M:%S %d/%m/%y ',
)
logger = logging.getLogger(__name__)


def find_bold_indices(text):
    bold_indices = []
    start_tags = [match.start() for match in re.finditer(r'<b>', text)]
    end_tags = [match.start() for match in re.finditer(r'</b>', text)]

    for start_index in start_tags:
        for end_index in end_tags:
            if end_index > start_index:
                bold_indices.append((start_index, end_index))
                end_tags.remove(end_index)
                break

    return bold_indices


def generate_rich_string(text, bold_indices, bold):
    parts = []

    # Sort bold indices to start with the smallest index
    bold_indices.sort(key=lambda x: x[0])

    # Split text into segments based on bold indices
    start_index = 0
    for bold_start, bold_end in bold_indices:
        if start_index < bold_start:
            parts.append(text[start_index:bold_start])
        parts.append(bold)
        parts.append(text[bold_start+3:bold_end])
        start_index = bold_end + 4

    if start_index < len(text):
        parts.append(text[start_index:])

    return parts


def main(api_host, client_id, client_secret):
    mi = model_interface.ModelInterface(
        api_host, client_id, client_secret, wait_time=5, max_workers=10
    )

    results = mi.get_studies()
    data = results.json()

    # Extract accession numbers
    accessions = [study["accessionNumber"] for study in data["studies"]]
    accessions = accessions[0:2000]
    accessions_length = len(accessions)
    batch_size = 500
    num_batches = (accessions_length + batch_size - 1) // batch_size
    accession_batches = [accessions[i * batch_size:(i + 1) * batch_size] for i in range(num_batches)]

    failed_dict = {}

    # accessions_list_for_prediction = accessions[0:2000]
    for batch_index, batch_accessions in enumerate(accession_batches):
        logger.info(f'Fetching results for batch : {batch_index}')
        # Get prediction results from BE
        results = mi.bulk_get(batch_accessions)
        for result in results:
            # if result["classification"] == "REQUIRED_CXR_ERROR" or result["classification"] == "REQUIRED_AP_PA_IMAGE_ERROR" or result["classification"] == "ERROR" or result["classification"] == "MODEL_ERROR":
            #     failed_dict[result["accession"]] = result["classification"]
            if result["get_log"] is None:
                json_file = path.join(output_location, f"{result['accession']}.json")
                with open(json_file, 'w') as fp:
                    json.dump(result, fp)
            else:
                failed_dict[result["accession"]] = result["classification"]

    for key in failed_dict:
        accessions.remove(key)

    logger.info(f'Failed dict : {failed_dict}')

    # Generate txt report
    generate_text_report('txt_report_gen/cxrjsons', 'ai_outputs/report_output')

    # Create a new Excel workbook
    workbook = xlsxwriter.Workbook('ai_outputs/accession_data.xlsx')

    # Add a worksheet to the workbook
    worksheet = workbook.add_worksheet()

    bold = workbook.add_format({"bold": True})

    # Define column headings
    column_headings = [
        "Accession Number",
        "List of Findings",
        "Report Embedded",
        "Link to Report Folder",
        "Link to Secondary Capture Folder"
    ]

    # Write column headings to the first row
    for col_num, heading in enumerate(column_headings):
        worksheet.write(0, col_num, heading)

    # Directory path
    pwd = os.getcwd()

    # Sample accessions list
    # accessions = ['0cfa3270d06d3']

    # Iterate over accessions
    for row_num, accession_number in enumerate(accessions, start=1):
        if os.path.exists(f'ai_outputs/report_output/{accession_number}.txt'):
            with open(f'ai_outputs/report_output/{accession_number}.txt', 'r') as file:
                lines = file.readlines()

            with open(f'txt_report_gen/cxrjsons/{accession_number}.json', 'r') as json_file:
                model_output = json.load(json_file)

            finding_labels = []

            relevant_findings = model_output["classification"]["findings"]["vision"]["study"]["classifications"]["relevant"]

            def sorting_key(finding):
                return finding["assignPriorityId"], finding["displayOrder"]

            sorted_findings = []
            for group in relevant_findings:
                findings_list = group["findings"]
                sorted_findings.extend(sorted(findings_list, key=sorting_key))

            finding_labels = [f'{finding["labelName"]}\n' for finding in sorted_findings]
            if len(finding_labels) == 0:
                finding_labels.append('<No findings present>')

            embedded_string = ''.join(lines)
            findings_string = ''.join(finding_labels)

            folder_path = os.path.join(pwd, 'ai_outputs', 'report_output', f'{accession_number}.txt')
            relative_path = os.path.relpath(folder_path, os.path.join(pwd, 'ai_outputs'))
            link_to_folder = f'file://{relative_path}'

            # Write data to the worksheet
            new_row_data = [
                accession_number,  # Accession Number
                findings_string,  # List of Findings
                embedded_string,  # Report Embedded
                link_to_folder,  # Link to Report Folder
                "<PLACEHOLDER>"  # Link to Secondary Capture Folder
            ]

            bold_indices = find_bold_indices(embedded_string)

            for col_num, cell_data in enumerate(new_row_data):
                if col_num == 3:  # Add hyperlink for the "Link to Report Folder" column
                    # hyperlink_formula = f'=HYPERLINK("{relative_path}", "{relative_path}")'
                    # worksheet.write_formula(row_num, col_num, hyperlink_formula)
                    worksheet.write_url(row_num, col_num, relative_path, string=relative_path)
                elif col_num == 2:
                    logger.info(f'accessions: {accession_number}')
                    if len(bold_indices) != 0:
                        rich_text_parts = generate_rich_string(embedded_string, bold_indices, bold)
                        worksheet.write_rich_string(row_num, col_num, *rich_text_parts)
                    else:
                        worksheet.write(row_num, col_num, cell_data)
                else:
                    worksheet.write(row_num, col_num, cell_data)

    # Set column widths and apply text wrap
    for col_num, heading in enumerate(column_headings):
        # max_length = max(len(str(new_row_data[col_num])) for new_row_data in
        #                  [worksheet.row_values(row_num) for row_num in range(worksheet.dim_rowmax + 1)])
        if col_num == 2:
            worksheet.set_column(col_num, col_num, 170)
        elif col_num == 1:
            worksheet.set_column(col_num, col_num, 75)
        else:
            worksheet.set_column(col_num, col_num, 30)

    # Apply text wrap to the entire sheet
    for row_num in range(1, len(accessions) + 1):
        worksheet.set_row(row_num, 280)

    # Close the workbook
    workbook.close()

    print("Excel file created successfully with the specified columns using xlsxwriter.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c",
        "--client_id",
        help="Annalise Backend Client ID",
    )
    parser.add_argument(
        "-s",
        "--client_secret",
        help="Annalise Backend Client Secret",
    )
    parser.add_argument(
        "-a",
        "--api_host",
        help="Annalise Backend Client base url",
    )
    args = parser.parse_args()

    if (
        args.api_host is not None
        and args.client_id is not None
        and args.client_secret is not None
    ):
        main(
            args.api_host,
            args.client_id,
            args.client_secret
        )
    else:
        if args.input_file is not None and args.accession_number is not None:
            logger.error("Only specify an input file OR an accession number")
        else:
            logger.error("Arguments provided not sufficient to run. Use --help for more info.")
