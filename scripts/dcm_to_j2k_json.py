from data_uploader.model_interface import DicomProcessor
import pydicom
import base64
import json
import argparse
from pathlib import Path
from os import walk, path

def main(search_directory):
    files_to_open = []
    for root, _, files in walk(search_directory, topdown=True, onerror=None):
        files_to_open.extend(
            path.join(root, filename)
            for filename in files
            if filename.endswith(".dcm")
        )

    for dcm_file in files_to_open:
        dp = DicomProcessor([pydicom.read_file(dcm_file)])
        payload = dp.vision_request()
        image = payload['images'][0]
        j2c_data = base64.b64decode(image['data'])
        with open(f'{Path(dcm_file).stem}.j2c', 'wb') as f:
            f.write(j2c_data)
        del image['data']
        json.dump(payload, open(f'{Path(dcm_file).stem}.json', 'w'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-d",
        "--directory",
        help="Directory to search for DICOM files",
        required=True
    )

    args = parser.parse_args()

    main(args.directory)
