#!/usr/bin/env python
# coding: utf-8

# # First Example based on U2OS data
# 
# Oriented after Cell Painting Gallery folder structure
# 
# https://broadinstitute.github.io/cellpainting-gallery/data_structure.html
# 
# Discussion for implementation here: https://github.com/broadinstitute/cellpainting-gallery/discussions/57
# 
# 
# ## Top level data structure
# 
# ```
# cellpainting-gallery 
# └── cpgXXXX-EU-OS-bioactives 
# │  ├── FMP 
# │  │  ├── images 
# │  │  │  ├── 2023_03_01_Batch1_HepG2
# │  │  │  ├── 2023_03_01_Batch2_U2OS
# │  │  │  ...
# │  │  ├── workspace 
# │  │  └── workspace_dl 
# │  ├── USC
# │  ├── MEDINA 
# │  └── IMTM 
# 
# ```

# ## Images 
# 
# The **images** folder contains the original images. Which are sorted into batches. Each batch is an acquisition from one day. 
# 
# For each batch there is an illumination (illum) folder and an images folder. The **illum** folder contains per **plate-name** the illumination correction files. The images files then the folder **full-plate-name** i.e. acquired images as is. 
# 
# ```
# images
# └── 2023_03_01_Batch2_U2OS
# │  ├── illum
# │  │  ├── <plate-name>
# |  │  │  ├── <plate-name>_Illum<Channel>.npy
# |  │  │  ├── <plate-name>_Illum<Channel>.npy
# |  │  │  ...
# │  │  ├── <plate-name>
# │  │  │  ...
# │  ├── images
# │  │  ├── <full-plate-name>
# │  │  ├── <full-plate-name>
# │  │  │  ...
# │  └── images
# ```

# ## Workspace
# 
# The 'workspace' folder contains everything but images. 
# 
# ```
# cellpainting-gallery/
# └── cpgXXXX-EU-OS-bioactives
#     └── FMP
#         ├── images
#         └── workspace
#             ├── analysis
#             ├── backend
#             ├── load_data_csv
#             ├── metadata
#             └── profiles
# ```
# 
# 
# ### Metadata
# 
# ```
# └── metadata
#      ├─── external_metadata
#      |   └── external_metadata.tsv
#      └── platemaps
#          └── 2023_03_01_Batch2_U2OS
#              ├── barcode_platemap.csv
#              └── platemap
#                  └── <plate-name>.txt
# ```

# # barcode_platemap.csv 
# 
# * Two columns: Assay_Plate_Barcode and Plate_Map_Name. 
# * Assay_Plate_Barcode matches the plate name used for analysis (folder name i.e.  
# * Plate_Map_Name is the name of a platemap in the platemaps/BATCH/platemap folder. 
# * There may be one-to-one or many-to-one correspondence between Assay_Plate_Barcode and Plate_Map_Name. 
# 
# | Assay_Plate_Barcode | Plate_Map_Name | 
# |----------|----------|
# | U2OSB1007R3_2023-03-01T22_29_15-Measurement1 | B1007_R3 | 
# | ...    | ...   | 
# | ...    | ...   | 

# # PLATEMAP.txt  
# 
# * plate_map_name and well_position columns and may be any additional number of metadata columns. 
# * plate_map_name matches the Plate_Map_Name in the barcode_platemap.csv and the PLATEMAP in the file name. 
# * well_position matches the well names in the data output by CellProfiler
# * typically based on raw image file naming as so are generally formatted like A01
# * may be upper or lowercase and may or may not have zero padding (e.g. a1, a01, A1, A01). 
# 
# |plate_map_name | well_position  | Metadata_EOS  | Metadata_Concentration  |
# |----------------|----------------|---------------|-------------------------|
# |B1007_R3| A01            | EOSXXXXXX     | 10                      |
# |B1007_R3| A02            | DMSO          | 0                       |
# |B1007_R3| A03            | Tetrandrine   | 5                       |
# |B1007_R3| A04            | Nocodazole    | 5                       |

# In[11]:


# Load additional data: Annotation file
# Modify Annotation file such that that there is a matching plate_map_name

## From key file then move data into new structure and create metadata files
import os # standard library
import pandas as pd 
import re # standard library
import shutil # standard library
import logging # standard library
import json # standard library


# In[12]:


cwd = os.getcwd()
config_json_path = cwd  + os.path.sep + "config.json"
config_json_path

# load the config.json
with open(config_json_path, "r") as config_file:
    config = json.load(config_file)


# In[13]:


# location where the image data is and keyfile
data_input_path = config['input_directory']
key_file_name = config['filename_keyfile'] # could be automatic
key_file = pd.read_csv(data_input_path + key_file_name) 

# location of annotation
annot_path = config['annotation_directory']
annot_name = config['filename_annotation']
annot_file = pd.read_csv(annot_path + annot_name)

# where the data should be move to
data_output_path = config['output_directory']


# In[14]:


# setup logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# first file logger
log_path = data_input_path + os.path.sep + 'Log_data_structure.log'
log = setup_logger('first_logger', log_path)

os_warning_path = data_input_path + os.path.sep + 'os-error_data_structure.log'
os_warning = setup_logger('second_logger', os_warning_path)

# Log the used paths
log.info("Input path: " + data_input_path)
log.info("Keyfile name: " + key_file_name)
log.info("Annot path: " + annot_path)
log.info("Annot name: " + annot_name)
log.info("Output path: " + data_output_path)

# Further settings
source = None
cpg_name = None


# make sure that there is only one source in the key_file
source_number = len(key_file['source'].unique())

if source_number == 1:
    
    log.info("Only 1 source present in key_file proceed")
    source = key_file['source'][0]
    cpg_name = key_file['cpg_name'][0]
    
else:
    
    log.error("ERROR: Source number incorrect")
    
# Adjust annot_file 
annot_file['plate_map_name'] = annot_file['Metadata_Plate'] + '_' + annot_file['Metadata_Batch']
annot_file = annot_file.rename(columns={"Metadata_Well": "well_position"})
annot_file = annot_file.rename(columns={"Metadata_Batch": "Metadata_Replicate"})
annot_file = annot_file[['plate_map_name', 
                         'well_position', 
                         'Metadata_Partner', 
                         'Metadata_Plate', 
                         'Metadata_Replicate',
                         'Metadata_EOS',
                         'Metadata_Concentration'
                        ]]


# create top folder path
top_folder_path = os.path.join(data_output_path, cpg_name)

try:
    os.mkdir(top_folder_path)
    
except OSError as error:  
    os_warning.error(error)   
    
# source folder path
source_path = os.path.join(top_folder_path, source)

try:
    os.mkdir(source_path)
except OSError as error:  
    os_warning.error(error)  

# create images folder in source path
images_path = os.path.join(source_path, 'images')

try:
    os.mkdir(images_path)
except OSError as error:  
    os_warning.error(error)  
    
# create workspace folder in source path
workspace_path = os.path.join(source_path, 'workspace')

try:
    os.mkdir(workspace_path)
except OSError as error:  
    os_warning.error(error)
    
# create metadata folder in workspace
metadata_path = os.path.join(workspace_path, 'metadata')

try:
    os.mkdir(metadata_path)
except OSError as error:  
    os_warning.error(error)
    
# create external metadata folder in metadata
external_metadata_path = os.path.join(metadata_path, 'external_metadata')

try:
    os.mkdir(external_metadata_path)
except OSError as error:  
    os_warning.error(error)
    
# TODO: Copy external_metaadata.tsv to external_metadata folder
external_metadata_name = annot_path + 'external_metadata.tsv'
shutil.copy(external_metadata_name, external_metadata_path)
    
# create path for platemaps
platemaps_path = os.path.join(metadata_path, 'platemaps')

try:
    os.mkdir(platemaps_path)
except OSError as error:  
    os_warning.error(error)
    

# TODO: change to move after test
def copy_folder(input_path, destination_path):
    # Ensure the source folder exists
    if not os.path.exists(input_path):
        log.error(f"Source folder '{input_path}' does not exist.")
        return

    # put the name of the input path and add it to the destination path
    #input_path_name = os.path.basename(input_path.rstrip(os.sep))
    #destination_with_input_path = os.path.join(destination_path, input_path_name)

    # Create the input folder within the destination path
    # os.makedirs(destination_with_input_path, exist_ok=True)
    
    # Copy the content of the input_path into the destination and input path
    # shutil.copytree(input_path, destination_with_input_path, dirs_exist_ok=True)
    shutil.move(input_path, destination_path)
    
# For batch_name 
batch_name_list = key_file['Batch_Name'].unique()

for batch_name in batch_name_list:
    
    log.info('Batch Name: ' + batch_name)
    
    # Creates Batch folder using <Batch_Name> in <Source>/images/
    batch_path = os.path.join(images_path, batch_name)
    
    try:
        os.mkdir(batch_path)
    except OSError as error:  
        os_warning.error(error)  
    
    # create images folder in Batch folder
    batch_images_path = os.path.join(batch_path, 'images')
    
    try:
        os.mkdir(batch_images_path)
    except OSError as error:  
        os_warning.error(error)
    
    # Creates Batch folder using <Batch_Name> in metadata/platemaps/
    batch_platemaps_path = os.path.join(platemaps_path, batch_name)
    
    try:
        os.mkdir(batch_platemaps_path)
    except OSError as error:  
        os_warning.error(error)
    
    # Creates platemap folder in batch_platemaps_path
    batch_platemaps_platemap_path = os.path.join(batch_platemaps_path, 'platemap')
    
    try:
        os.mkdir(batch_platemaps_platemap_path)
    except OSError as error:  
        os_warning.error(error)
        
    # Create barcode_platemap.csv 
    # containting Assay_Plate_Barcode and Plate_Map_Name for all plates of <Batch_name>
    # Save barcode_platemap.csv in metadata/platemaps/<Batch_Name>/
    filtered_key_file = key_file[key_file['Batch_Name'] == batch_name]
    barcode_platemap = filtered_key_file[['Assay_Plate_Barcode', 'Plate_Map_Name']]
    barcode_platemap.to_csv(batch_platemaps_path + os.path.sep + 'barcode_platemap.csv', index=None)
    
    log.info("Created barcode platemap: " + batch_name)
    
    # For all plates per batch Assay_Plate_Barcode 
    assay_plate_barcode_list = filtered_key_file['Assay_Plate_Barcode'].unique()
    
    for assay_plate_barcode in assay_plate_barcode_list:
        
        # Move folder for original images to batch_images_path
        assay_plate_barcode_input = os.path.join(data_input_path, assay_plate_barcode)
        copy_folder(assay_plate_barcode_input, batch_images_path)
        log.info("Moved: " + assay_plate_barcode)
        
        # Create <Plate_Map_Name>.txt containting: plate_map_name and from Annotation file
        # well_position, Metadata_EOS, Metadata_Concentration
        # Save <Plate_Map_Name>.txt in batch_platemaps_platemap_path
        barcode_filtered_key_file = filtered_key_file[filtered_key_file['Assay_Plate_Barcode'] == assay_plate_barcode]

        if barcode_filtered_key_file.shape[0] == 1:
    
            log.info(f"Plate name {assay_plate_barcode} is unique")
    
            # gets the values to filter the annotation file
            barcode_plate_name = barcode_filtered_key_file['plate_name'].iloc[0]
            barcode_replicate_number = barcode_filtered_key_file['replicate_number'].iloc[0]
            barcode_plate_Map_Name = barcode_filtered_key_file['Plate_Map_Name'].iloc[0]
    
            # filters the annotation file to extract only the necessary values
            filtered_annot_file = annot_file[(annot_file['Metadata_Plate'] == barcode_plate_name) & (annot_file['Metadata_Replicate'] == barcode_replicate_number)]
    
            if filtered_annot_file.shape[0] == 384:
        
                log.info("Plate maps is 384 lines long")
        
                platemape_txt_path = batch_platemaps_platemap_path + os.path.sep + barcode_plate_Map_Name + '.txt'
                filtered_annot_file.to_csv(platemape_txt_path, header=True, index=None, sep=' ', mode='a')
                
                log.info("Created plate map: " + barcode_plate_Map_Name)
            
            else:
        
                log.error("Error: plate map has incorrect length")
    
    
        else:
            
            log.error(f"Error: Plate name {assay_plate_barcode} not unique")


# In[ ]:




