import glob
import time

import pandas as pd
import json
import base64
from PIL import Image
import io
import numpy as np 
from jinja2 import Environment, FileSystemLoader, select_autoescape
import argparse
import os

def proc(f):
     return dict([(finding['label'], finding['predictionProbability']) for g in f['classification']['findings']['vision']['study']['classifications']['relevant'] for finding in g['findings']] + 
                 [(finding['label'], finding['predictionProbability']) for finding in f['classification']['findings']['vision']['study']['classifications']['irrelevant']])
def get_threshold(f):
     return dict([(finding['label'], finding['predictionThreshold']) for g in f['classification']['findings']['vision']['study']['classifications']['relevant'] for finding in g['findings']] + 
                 [(finding['label'], finding['predictionThreshold']) for finding in f['classification']['findings']['vision']['study']['classifications']['irrelevant']])

def block_reduce(image, block_size, func):
    rows, cols = image.shape
    block_rows, block_cols = block_size
    reduced_rows = rows // block_rows
    reduced_cols = cols // block_cols
    reduced_image = np.empty((reduced_rows, reduced_cols))

    for i in range(reduced_rows):
        for j in range(reduced_cols):
            block = image[i * block_rows: (i + 1) * block_rows, j * block_cols: (j + 1) * block_cols]
            reduced_image[i, j] = func(block)

    return reduced_image

def create_side_zone(arr):
    if arr.shape == (2,):
        if arr[0] == 1 and arr[1] == 0:
            return 'right'
        elif arr[0] == 0 and arr[1] == 1:
            return 'left'
        elif arr[0] == 1 and arr[1] == 1:
            return 'bilateral'
        elif arr[0] == 0 and arr[1] == 0:
            return None
    elif arr.shape == (2,2):
        coords = np.argwhere(arr)
        strings = [f"{'right' if x == 0 else 'left'} {'upper' if y == 0 else 'lower'}" for y,x in coords]
        if 'right upper' in strings and 'right lower' in strings:
            strings.remove('right upper')
            strings.remove('right lower')
            strings.append('right-sided')
        if 'left upper' in strings and 'left lower' in strings:
            strings.remove('left upper')
            strings.remove('left lower')
            strings.append('left-sided')
        if 'left-sided' in strings and 'right-sided' in strings:
            strings.remove('left-sided')
            strings.remove('right-sided')
            strings.append('bilateral')
        if 'left upper' in strings and 'right upper' in strings:
            strings.remove('left upper')
            strings.remove('right upper')
            strings.append('bilateral upper')
        if 'left lower' in strings and 'right lower' in strings:
            strings.remove('left lower')
            strings.remove('right lower')
            strings.append('bilateral lower')    
        if len(strings) > 1:
            return ', '.join(strings[:-1]) + ' and ' + strings[-1]         
        return ', '.join(strings)        
    elif arr.shape == (3,2):
        pass
    else:
        raise NotImplementedError(f'arr shape {arr.shape} not supported')
    
if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", help="Path to the input directory")
    parser.add_argument("output_dir", help="Path to the output directory")
    args = parser.parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir

    paths = glob.glob(f'{input_dir}/*.json')
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    data = []
    ancillary_data = {}
    for path in paths:
        with open(path) as f:
            raw_data = json.load(f)
            d = proc(raw_data)
            filtered_images = []
            for image in raw_data['classification']['findings']['vision']['images']:
                filtered_images.append({k: v for k, v in image.items() if k in ['imageInstanceUid', 'viewPosition']})
            viewpositions = [d['viewPosition'] for d in filtered_images]
            filtered_image_uids = [d['imageInstanceUid'] for d in filtered_images if d['viewPosition'] in {'AP', 'PA'}]

            pooled_segmentations3x2 = {}
            pooled_segmentations2x2 = {}
            pooled_lateralities = {}

            for filtered_image in filtered_image_uids:
                for key, png_bytes in raw_data['segmentation'][filtered_image].items():
                    # Decode the PNG bytes
                    decoded_image = base64.b64decode(png_bytes)

                    # Open the image using PIL
                    image = Image.open(io.BytesIO(decoded_image))
                    image = np.array(image)[:,:,1]

                    # 3 to allow it to be chopped into upper, middle, lower
                    pooled_image = block_reduce(image, (image.shape[0]//3, image.shape[1]//2), np.max)
                    if key not in pooled_segmentations3x2:                
                        pooled_segmentations3x2[key] = pooled_image
                    else:
                        pooled_segmentations3x2[key] += pooled_image
                        
                    # 3 to allow it to be chopped into upper, middle, lower
                    pooled_image = block_reduce(image, (image.shape[0]//2, image.shape[1]//2), np.max)
                    if key not in pooled_segmentations2x2:                
                        pooled_segmentations2x2[key] = pooled_image
                    else:
                        pooled_segmentations2x2[key] += pooled_image

                        
                for key, side in raw_data['laterality'][filtered_image].items():
                    if side == 'RIGHT':
                        pooled_image = np.array([1,0])
                    elif side == 'LEFT':
                        pooled_image = np.array([0,1])
                    elif side == 'BILATERAL':
                        pooled_image = np.array([1,1])
                    elif side == 'NONE':
                        pooled_image = np.array([0,0])
                    else:
                        raise Exception(f'{side} not recognized for {key} for {path}')
                    
                    if key not in pooled_lateralities:                
                        pooled_lateralities[key] = pooled_image
                    else:
                        pooled_lateralities[key] += pooled_image

            for key, value in pooled_segmentations2x2.items():
                pooled_segmentations2x2[key] = (pooled_segmentations2x2[key] > 0)

            for key, value in pooled_segmentations3x2.items():
                pooled_segmentations3x2[key] = (pooled_segmentations3x2[key] > 0)
            
            for key, value in pooled_lateralities.items():
                pooled_lateralities[key] = (pooled_lateralities[key] > 0)

        

            
        d['accession'] = path.split('/')[-1].replace('.json', '')
        ancillary_data[d['accession'] ] = {'pooled_segmentations_3x2': pooled_segmentations3x2,'pooled_segmentations_2x2': pooled_segmentations2x2, 'pooled_lateralities': pooled_lateralities, 'viewpositions':viewpositions}
        data.append(d)
    df = pd.DataFrame(data).set_index('accession')

    # assume thresholds are consistent across all jsons! 
    with open(path) as f:
        thresholds = pd.DataFrame(get_threshold(json.load(f)), index=['threshold']).transpose()

    section_mapping = {}
    for section, _df in pd.read_csv('fortis_spec.csv', index_col=0).groupby('Report Section'):
        slug = section.strip().lower().replace(' ','_')
        if slug not in section_mapping:
            section_mapping[slug] = []
        section_mapping[slug].extend(_df.index.tolist())
    section_mapping['projection'] = ['lat', 'multifrontal']




    env = Environment(
        loader=FileSystemLoader('templates/'),
        autoescape=select_autoescape(['html', 'xml', 'jinja'])
    )

    bin_df = df > thresholds.values.transpose()
    defaults = {'projection':'Frontal chest radiograph.',
                'technical_quality':'Satisfactory image quality.',
                'cardiomediastinal':'Cardiac silhouette within normal limits. Normal mediastinal and hilar contours.',
                'lung_parenchyma':'No significant parenchymal lung abnormality.',
                'pleural_space':'Normal pleural spaces.',
                'lines_and_tubes': "No lines or tubes identified.",
                'bones':'No bony abnormality.',
                'other':'No other findings.'}


    for accession in bin_df.index:
        # get only positive columns
        bin_findings = bin_df.loc[accession]
        bin_findings['lat'] = 'LAT' in ancillary_data[accession]['viewpositions']
        bin_findings['multifrontal'] = ('LAT' not in ancillary_data[accession]['viewpositions']) and len(ancillary_data[accession]['viewpositions']) > 1
        positive_findings = bin_findings[bin_findings]
        
        laterality_texts = {key+'_laterality': create_side_zone(values) for key, values in ancillary_data[accession]['pooled_lateralities'].items()}
        zone2x2_texts = {key+'_zone2x2': create_side_zone(values) for key, values in ancillary_data[accession]['pooled_segmentations_2x2'].items()}
        # 1x2 is map but converted into left/right
        zone_to_laterality_texts = {key+'_laterality': create_side_zone((values.sum(axis=0)>0).astype(int)) for key, values in ancillary_data[accession]['pooled_segmentations_2x2'].items()}
        
        template_components = {}
        for template_name in env.list_templates():
            template_name = template_name.replace('.jinja', '')
            if template_name != 'base_report':
                template = env.get_template(template_name + '.jinja')
                output = template.render(**bin_findings, **laterality_texts, **zone2x2_texts, **zone_to_laterality_texts)
                # remove all empty lines (artifact of how my jinja templates are laid out)
                output = ' '.join([line.capitalize() for line in output.split('\n') if len(line)])
                if template_name != 'projection' and len(output):
                    output = f"<b>{output}</b>"
                # if the section has no relevant findings, use the default
                if template_name in section_mapping:
                    if not any([finding in section_mapping[template_name] for finding in positive_findings.index]):
                        output = defaults[template_name]                    
                template_components[template_name] = output


        template = env.get_template('base_report.jinja')
        output = template.render(**template_components)

        output_path = os.path.join(output_dir, f"{accession}.txt")
        with open(output_path, "w") as f:
            f.write(output)
        print (accession)
        print (output)
        print ('*******************************')