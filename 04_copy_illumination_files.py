#!/usr/bin/env python
# coding: utf-8

# # Copy illumination correction files
# 
# 
# Illumination organisation
# 
# ```
# images
# ├── 2023_03_01_Batch2_U2OS
# │  ├── illum
# │  │  ├── <plate-name>
# |  │  │  ├── <plate-name>_Illum<Channel>.npy
# |  │  │  ├── <plate-name>_Illum<Channel>.npy
# |  │  │  ...
# │  │  ├── <plate-name>
# │  │  │  ...
# │  └── images
# ├── <Batch_name>
# ├── ...
# ```
# 
# Channel names:
# IllumDNA
# IllumER
# IllumAGP
# IllumMito

# In[5]:


import os
import sys
import glob
import shutil
import pandas as pd
import logging
import json # standard library


# In[6]:


cwd = os.getcwd()
config_json_path = cwd  + os.path.sep + "config.json"
config_json_path

# load the config.json
with open(config_json_path, "r") as config_file:
    config = json.load(config_file)


# In[7]:


# location where the image data is and keyfile
data_input_path = config['input_directory']
key_file_name = config['filename_keyfile'] # could be automatic
key_file = pd.read_csv(data_input_path + key_file_name) 

# where the data should be move to
data_output_path = config['output_directory']

# Path to illumination files
# illum_path = "/home/schmiedc/FMP_Docs/Projects/CellPainting/DataUpload/IllumFiles/U2OS_10uM/"
illum_path =  config['illmuniation_files_directory']


# In[8]:


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
log_path = data_input_path + os.path.sep + 'Log_copy_illum.log'
process_logger = setup_logger('first_logger', log_path)

os_warning_path = data_input_path + os.path.sep + 'os-error_copy_illum.log'
os_warning = setup_logger('second_logger', os_warning_path)

# Further settings
cpg_name = None
source = None

# make sure that there is only one source in the key_file
cpg_name_number = len(key_file['cpg_name'].unique())
source_number = len(key_file['source'].unique())

if source_number == 1 & cpg_name_number == 1:
    
    process_logger.info("Only 1 source and 1 cpg name present in key_file: proceed")
    cpg_name = key_file['cpg_name'][0]
    source = key_file['source'][0]
    
else:
    
    process_logger.error("Source number and/or cpg name number incorrect")
    
# Log the used paths
process_logger.info("Input path: " + data_input_path)
process_logger.info("Keyfile name: " + key_file_name)
process_logger.info("Output path: " + data_output_path)
process_logger.info("Illum path: " + illum_path)

# generate paths
cpg_path = os.path.join(data_output_path, cpg_name)
source_path = os.path.join(cpg_path, source)
images_path = os.path.join(source_path, 'images')

def copy_rename(input_path, input_file_name, destination_path, update_base_name, file_ending):
    
    process_logger.info("Copy and rename: " + input_file_name )
    
    original_file_path = input_path + os.path.sep + input_file_name

    # copy from original path
    shutil.copy(original_file_path, destination_path)

    # Create new path
    new_file_path = destination_path + os.path.sep + update_base_name + file_ending

    # rename file with new name
    shutil.move(destination_path + os.path.sep + input_file_name, new_file_path)
    
    process_logger.info("New path: " + new_file_path )


# For batch_name 
batch_name_list = key_file['Batch_Name'].unique()

for batch_name in batch_name_list:

    process_logger.info("Batch Name: " + batch_name)
    
    filtered_key_file = key_file[key_file['Batch_Name'] == batch_name]
    
    # Creates Batch folder using <Batch_Name> in <Source>/images/
    batch_path = os.path.join(images_path, batch_name)

    # create images folder in Batch folder
    batch_illum_path = os.path.join(batch_path, 'illum')
    
    try:
        os.mkdir(batch_illum_path)
    except OSError as error:  
        os_warning.error(error)
    
    process_logger.info("Created illum dir: " + batch_name)
    
    # For all plates per batch Assay_Plate_Barcode 
    assay_plate_barcode_list = filtered_key_file['Assay_Plate_Barcode'].unique()
    
    for assay_plate_barcode in assay_plate_barcode_list:
        
        process_logger.info("Image folder name " + assay_plate_barcode)
        
        # Create plate name folder in illum folder
        barcode_filtered_key_file = filtered_key_file[filtered_key_file['Assay_Plate_Barcode'] == 
                                                      assay_plate_barcode]

        if barcode_filtered_key_file.shape[0] == 1:
    
            process_logger.info("Plate name " + assay_plate_barcode + " is unique")
    
            # gets the values to filter the annotation file
            barcode_plate_name = barcode_filtered_key_file['plate_name'].iloc[0]
            barcode_replicate_number = barcode_filtered_key_file['replicate_number'].iloc[0]
            barcode_plate_Map_Name = barcode_filtered_key_file['Plate_Map_Name'].iloc[0]
            
            # create plate folder in Batch folder
            plate_illum_path = os.path.join(batch_illum_path, barcode_plate_Map_Name)
            
            try:
                os.mkdir(plate_illum_path)
            
            except OSError as error:
                os_warning.error(error)
            
            org_plate_illum_path = os.path.join(illum_path, barcode_plate_Map_Name,"IllumCorr")
            
            # Check if original illum folder exists
            if os.path.exists(org_plate_illum_path):
                
                process_logger.info(f"Orig. illum. folder for " + barcode_plate_Map_Name + " exists")
                illum_corr_file_list = glob.glob1(org_plate_illum_path,"*.npy")
                
                # count number of .npy illum corr files
                illum_corr_file_counter = len(illum_corr_file_list)
                
                # Check if these are 4
                if illum_corr_file_counter == 4:
                    
                    process_logger.info(barcode_plate_name + " has all 4 illum. corr. files")
                
                    # contains all permissable endings
                    illum_corr_endings = ['_IllumMito.npy', '_IllumDNA.npy', '_IllumER.npy', '_IllumAGP.npy']
                    
                    # dict to store the file names
                    illum_corr_file_names = {ending: None for ending in illum_corr_endings}
                    
                    # Iterate over the ill_corr_file_list and check for illum_corr_endings
                    for file in illum_corr_file_list:
                        for ending in illum_corr_endings:
                            if file.endswith(ending):
                                illum_corr_file_names[ending] = file
                                break
                                
                    # Check if all required files are found
                    missing_files = [ending for ending, file in illum_corr_file_names.items() if file is None]
                    missing_files_string = ''.join(missing_files)
                     
                    if missing_files:
                        
                        process_logger.error("Error: The following required files are missing: " +  missing_files_string)
                        
                    else:
                        
                        for ending in illum_corr_endings:
                            
                            filename = illum_corr_file_names[ending]
                            copy_rename(org_plate_illum_path, filename, plate_illum_path, barcode_plate_Map_Name, ending)
                                        
                else:
                    
                    process_logger.error("Error: incorrect number for illum. corr. files of " + barcode_plate_Map_Name)
                    
            else:
                
                process_logger.error("Error: Orig. illum. folder for " + barcode_plate_Map_Name + " does not exists")
                
        else:
            
            process_logger.error(f"Error: Plate name " + assay_plate_barcode + " not unique")


# In[ ]:




