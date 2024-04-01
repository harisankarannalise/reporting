import argparse
from datetime import datetime
import logging
from os import path
import json
import pandas as pd

from data_uploader import model_interface

output_location = "output/"

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


def main(api_host, client_id, client_secret, input_file=None, accession_number=None):
    mi = model_interface.ModelInterface(
        api_host, client_id, client_secret, wait_time=5, max_workers=1
    )
    if input_file != None:
        df = pd.read_csv(input_file, dtype=str)
        accessions = df['New AccessionNumber']
    else:
        accessions = [accession_number]

    results = mi.bulk_get(accessions)
    for result in results:
        json_file = path.join(output_location, f"{result['accession']}.json")
        with open(json_file, 'w') as fp:
            json.dump(result, fp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        "--input_file",
        help="CSV file generated by upload_dcms.py - at a minimum a csv containing a column named 'New AccessionNumber'",
    )
    parser.add_argument(
        "-n",
        "--accession_number",
        help="AccessionNumber to retrieve results for",
    )
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
        and ((args.input_file is not None) != (args.accession_number is not None))
    ):
        main(
            args.api_host,
            args.client_id,
            args.client_secret,
            input_file=args.input_file,
            accession_number=args.accession_number,
        )
    else:
        if args.input_file is not None and args.accession_number is not None:
            logger.error("Only specify an input file OR an accession number")
        else:
            logger.error("Arguments provided not sufficient to run. Use --help for more info.")
