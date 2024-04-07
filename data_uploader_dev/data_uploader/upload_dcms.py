import argparse
from datetime import datetime
import logging
from os import path

from data_uploader import model_interface
from data_uploader.utils.utils import (
    UploadMap,
    get_dses_grouped_by_field,
    get_files_in_directory_with_extension,
)

output_location = "output/"
search_file_extension = "dcm"

now = datetime.now()
log_file = path.join(output_location, f'upload_{now.strftime("%Y%m%d")}.log')
logging.basicConfig(
    level=logging.INFO,
    filename=log_file,
    filemode='a',
    format='%(asctime)s %(name) -14s - %(levelname) -8s - %(message)s',
    datefmt='%H:%M:%S %d/%m/%y ',
)
logger = logging.getLogger(__name__)


def main(regenerate_uids, group_by, api_host, client_id, client_secret, data_folder):
    mi = model_interface.ModelInterface(
        api_host, client_id, client_secret, wait_time=5, max_workers=1
    )

    files_to_open = get_files_in_directory_with_extension(search_file_extension, data_folder)
    logger.info(f"Found {files_to_open} {search_file_extension} files")
    file_details = get_dses_grouped_by_field(files_to_open, group_by)

    output_csv = path.join(output_location, f"upload_map_{now.strftime('%Y%m%d-%H%M%S')}.csv")

    for study in file_details:
        datasets = file_details[study]
        mi.bulk_upload_image(dses=datasets, regenerate_uids=regenerate_uids, upload_by_study=True)
    UploadMap.write_to_file(output_csv, mi.internal_maps)


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
    parser.add_argument(
        "-k",
        "--keep_uids",
        action="store_true",
        dest="keep_uids",
        help="Maintains AccessionNumber, PatientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUId",
    )
    parser.add_argument(
        "-d",
        "--data_folder",
        dest="data_folder",
        help="Folder to search for DICOM files",
    )
    parser.add_argument(
        "-g",
        "--group_by",
        default="StudyInstanceUID",
        help="DICOM field to group studies by per https://github.com/pydicom/pydicom/blob/master/pydicom/_dicom_dict.py. Defaults to StudyInstanceUID",
    )
    parser.set_defaults(keep_uids=False)
    args = parser.parse_args()

    if (
        args.api_host is not None
        and args.client_id is not None
        and args.client_secret is not None
        and args.data_folder is not None
    ):
        main(
            not args.keep_uids,
            args.group_by,
            args.api_host,
            args.client_id,
            args.client_secret,
            args.data_folder,
        )
    else:
        logger.error("Arguments provided not sufficient to run. Use --help for more info.")
