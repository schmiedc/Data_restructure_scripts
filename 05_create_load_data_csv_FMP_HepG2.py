#!/usr/bin/env python
# coding: utf-8

# # Create load_data.csv
# 
# Specify for each dataset the correct channel assignment (After create_image_list.py)
# 
# IMPORTANT: This is dataset dependent. Thus the assignement of channel can change. The below variant is for generating this file list for the FMP U2OS data. 
# 
# Organisation of load_data_csv
# 
# ```
# └── workspace
#     └── load_data_csv
#         ├── 2023_03_01_Batch2_U2OS
#         │   ├── <plate-name>
#         │   │   ├── load_data.csv
#         │   │   └── load_data_with_illum.csv
#         │   ├── <plate-name>
#         │   ├── ...
#         ├── <Batch_name>
#         ├── ...
# ```
# 

# In[1]:


# Create Batch folder using <Batch_Name> in <Source>/load_data_csv/
# Create adjusted Load_Images_for_IllumCorr.csv and Load_Images_for_Analysis.csv

# %%
import os
import pandas as pd
import numpy as np
import sys
import logging
import json # standard library


# In[2]:


cwd = os.getcwd()
config_json_path = cwd  + os.path.sep + "config.json"
config_json_path

# load the config.json
with open(config_json_path, "r") as config_file:
    config = json.load(config_file)


# In[3]:


# location where the image data is and keyfile
data_input_path = config['input_directory']
key_file_name = config['filename_keyfile'] # could be automatic
key_file = pd.read_csv(data_input_path + key_file_name) 

# where the data should be move to
data_output_path = config['output_directory']

# requirements from broad
top_path_cpg = config['top_path_cpg']


# In[4]:


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
log_path = data_input_path + os.path.sep + 'Log_load_data.log'
process_logger = setup_logger('first_logger', log_path)

os_warning_path = data_input_path + os.path.sep + 'os-error_load_data.log'
os_warning = setup_logger('second_logger', os_warning_path)


# In[5]:


# Log the used paths
process_logger.info("Input path: " + data_input_path)
process_logger.info("Keyfile name: " + key_file_name)
process_logger.info("Output path: " + data_output_path)
process_logger.info("Top file setting: " + top_path_cpg)


# In[6]:


### prerequisites for IllumCorr
Rows = np.array(["0", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P"])

### prerequisites for analysis file
Temp_analysis = {"FileName_OrigDNA": [],
        "PathName_OrigDNA": [],
        "FileName_OrigER": [],
        "PathName_OrigER": [],
        "FileName_OrigAGP": [],
        "PathName_OrigAGP": [],
        "FileName_OrigMito": [],
        "PathName_OrigMito": [],
        'Metadata_Batch': [],
        'Metadata_Plate': [],
        'Metadata_Well': [],         
        "Metadata_Site": [],
        "FileName_IllumDNA": [],
        "PathName_IllumDNA": [],
        "FileName_IllumER": [],
        "PathName_IllumER": [],
        "FileName_IllumAGP": [],
        "PathName_IllumAGP": [],
        "FileName_IllumMito": [],
        "PathName_IllumMito": []
        }

Load_Analysis = pd.DataFrame(Temp_analysis)


# In[7]:


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


# In[8]:


# generate paths
cpg_path = os.path.join(data_output_path, cpg_name)
source_path = os.path.join(cpg_path, source)
images_path = os.path.join(source_path, 'images')

worksapce_path = os.path.join(source_path, 'workspace')
load_data_csv_path = os.path.join(worksapce_path, 'load_data_csv')

# create load_data_csv folder
try:
    os.mkdir(load_data_csv_path)
    
except OSError as error:
    
    os_warning.error(error)


# In[9]:


# generate path for load_data that conforms with AWS cpg location
aws_cpg_path = os.path.join(top_path_cpg, cpg_name)
source_aws_cpg_path = os.path.join(aws_cpg_path, source)
images_aws_cpg_path = os.path.join(source_aws_cpg_path, 'images')


# In[10]:


# this is dataset specific
# specific to FMP HepG2
def get_load_data_FMP_HepG2(plate_path, aws_plate_path, aws_illum_corr_path, batch, plate, rows):
    
    ### loop through images in a plate
    images_path = os.path.join(plate_path, "Images")
    aws_images_path = os.path.join(aws_plate_path, "Images")
    
    dataframes = []
    
    if os.path.isdir(images_path):

        for file_name in os.listdir(images_path):
            
            if file_name.endswith(".tiff"):
                
                # determines the postion of the channel number and extracts it
                position = file_name.index('ch')
                channel_number = file_name[position+2:position+3]

                # for image fo the fist channel
                if int(channel_number) == 1:

                    # gets position of the row
                    position= file_name.index('r') 
                    row = file_name[position+1:position+3]
                    # Determines the row letter from the row position
                    Row = rows[int(row)]

                    # gets position of the column in the filename
                    position= file_name.index('c')
                    col = file_name[position+1:position+3]

                    # gets position of the field in the filename
                    position= file_name.index('f') 
                    field = file_name[position+1:position+3]

                    # splits the filename and creates the channel specific name
                    # This naming scheme is specific to FMP HepG2
                    namesplit_file_name = file_name.split("-")
                    file_name_DNA = namesplit_file_name[0] + "-ch3sk1fk1fl1.tiff"
                    file_name_ER = namesplit_file_name[0] + "-ch1sk1fk1fl1.tiff"
                    file_name_AGP = namesplit_file_name[0] + "-ch4sk1fk1fl1.tiff"
                    file_name_Mito = namesplit_file_name[0] + "-ch2sk1fk1fl1.tiff"

                    # process_logger.info("Found image : " + namesplit_file_name[0])
                    
                    ### Analysis
                    temp_analysis = {"FileName_OrigDNA": [file_name_DNA],
                            "PathName_OrigDNA": [aws_images_path], 
                            "FileName_OrigER": [file_name_ER],
                            "PathName_OrigER": [aws_images_path],
                            "FileName_OrigAGP": [file_name_AGP],
                            "PathName_OrigAGP": [aws_images_path],
                            "FileName_OrigMito": [file_name_Mito],
                            "PathName_OrigMito": [aws_images_path],
                            'Metadata_Batch': [batch],
                            'Metadata_Plate': [plate],
                            'Metadata_Well': [Row + col],
                            "Metadata_Site": [field],
                            "FileName_IllumDNA": [plate + "_IllumDNA.npy"],
                            "PathName_IllumDNA": [aws_illum_corr_path],
                            "FileName_IllumER": [plate + "_IllumER.npy"],
                            "PathName_IllumER": [aws_illum_corr_path],
                            "FileName_IllumAGP": [plate + "_IllumAGP.npy"],
                            "PathName_IllumAGP": [aws_illum_corr_path],
                            "FileName_IllumMito": [plate + "_IllumMito.npy"],
                            "PathName_IllumMito": [aws_illum_corr_path]
                            }

                    df_temp_analysis = pd.DataFrame(temp_analysis)

                    dataframes.append(df_temp_analysis)

        load_analysis = pd.concat(dataframes,ignore_index=True)
    
        return load_analysis


# In[11]:


# batch_list = os.listdir(images_path)
# For batch_name 
batch_name_list = key_file['Batch_Name'].unique()

# loop through the batches in the specified images dir
for batch in batch_name_list:
    
    process_logger.info("Create load_data for: " + batch)
    
    # get only the current batch from key file
    filtered_key_file_load = key_file[key_file['Batch_Name'] == batch]
    
    # create batch path for the images
    batch_path = os.path.join(images_path, batch)
    batch_images_path = os.path.join(batch_path, 'images')

    # create batch folder in load_data_csv folder
    batch_load_data_csv_path = os.path.join(load_data_csv_path, batch)
    
    try:
        os.mkdir(batch_load_data_csv_path)
    
    except OSError as error:
    
        os_warning.error(error)
     
    # Path for the images on aws
    batch_aws_cpg_path = os.path.join(images_aws_cpg_path, batch)
    batch_images_aws_cpg_path = os.path.join(batch_aws_cpg_path, 'images')
    batch_illum_corr_aws_cpg_path = os.path.join(batch_aws_cpg_path, 'illum')
    
    # path for the load_csv illum corr files on aws

    # get the folder names for each plate  
    full_plate_name_list = os.listdir(batch_images_path)

    # Walk through the images folder in the batch
    for assay_plate_barcode_load in full_plate_name_list:
        
        process_logger.info("Create load_data for: " + assay_plate_barcode_load)
        
        # Create plate name folder in illum folder
        barcode_filtered_key_file_load = filtered_key_file_load[
            filtered_key_file_load['Assay_Plate_Barcode'] == assay_plate_barcode_load]

        if barcode_filtered_key_file_load.shape[0] == 1:
    
            process_logger.info(f"Plate name {assay_plate_barcode_load} is unique")
    
            # gets the values to filter the annotation file
            barcode_plate_Map_Name_load = barcode_filtered_key_file_load['Plate_Map_Name'].iloc[0]
            
            process_logger.info("Create batch folder: " +  barcode_plate_Map_Name_load)
            
            # create plate folder in Batch folder
            plate_load_data_csv_path = os.path.join(batch_load_data_csv_path, barcode_plate_Map_Name_load)
            
            try:
                os.mkdir(plate_load_data_csv_path)
            
            except OSError as error:
                os_warning.error(error)
        
            # create path for aws
            plate_images_aws_cpg_path = os.path.join(batch_images_aws_cpg_path, assay_plate_barcode_load)
            plate_illum_corr_aws_cpg_path = os.path.join(batch_illum_corr_aws_cpg_path, barcode_plate_Map_Name_load)
            
            process_logger.info("Image Path: " + plate_images_aws_cpg_path )
            process_logger.info("Illum Path: " + plate_illum_corr_aws_cpg_path )
            
            full_plate_name_path = os.path.join(batch_images_path, assay_plate_barcode_load)
            
            # plate_path, aws_plate_path, aws_illum_corr_path, batch, plate, Rows
            load_data_with_illum = get_load_data_FMP_HepG2(full_plate_name_path,
                                                   plate_images_aws_cpg_path,
                                                   plate_illum_corr_aws_cpg_path,
                                                   batch,
                                                   barcode_plate_Map_Name_load,
                                                   Rows)
            
            if load_data_with_illum is not None:
                
                if load_data_with_illum.shape[0] == 3456:
                    
                    process_logger.info('load_data has 3456 rows')
                    
                else: 
                    
                    process_logger.error("load_data has " + str(load_data_with_illum.shape[0]) + " row(s)")

            else: 
                
                process_logger.error('Error: no load_data created')

            
            filename_load_data_with_illum = os.path.join(plate_load_data_csv_path, "load_data_with_illum.csv")
            
            try: 
                load_data_with_illum.to_csv(filename_load_data_with_illum, index = False)
            except AttributeError as error:
                os_warning.error(error)
            
            # reduce to load_data table
            load_data = None
            
            try:
                load_data = load_data_with_illum.iloc[:, 0:12]
            except AttributeError as error:
                os_warning.error(error)

            filename_load_data = os.path.join(plate_load_data_csv_path, "load_data.csv")
            
            try:
                load_data.to_csv(filename_load_data, index = False)
            except AttributeError as error:
                os_warning.error(error)

        else:
            
            process_logger.error(f"Error: Plate name {assay_plate_barcode_load} not unique")


# In[ ]:




