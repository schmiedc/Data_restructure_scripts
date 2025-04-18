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

# In[46]:


# Create Batch folder using <Batch_Name> in <Source>/load_data_csv/
# Create adjusted Load_Images_for_IllumCorr.csv and Load_Images_for_Analysis.csv

# %%
import os
import pandas as pd
import numpy as np
import sys
import logging
import json # standard library


# In[47]:


cwd = os.getcwd()
config_json_path = cwd  + os.path.sep + "config.json"
config_json_path

# load the config.json
with open(config_json_path, "r") as config_file:
    config = json.load(config_file)


# In[48]:


# location where the image data is and keyfile
data_input_path = config['input_directory']
key_file_name = config['filename_keyfile'] # could be automatic
key_file = pd.read_csv(data_input_path + key_file_name) 

# where the data should be move to
data_output_path = config['output_directory']

# requirements from broad
top_path_cpg = config['top_path_cpg']


# In[49]:


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


# In[50]:


# Log the used paths
process_logger.info("Input path: " + data_input_path)
process_logger.info("Keyfile name: " + key_file_name)
process_logger.info("Output path: " + data_output_path)
process_logger.info("Top file setting: " + top_path_cpg)


# In[51]:


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


# In[52]:


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


# In[53]:


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


# In[54]:


# generate path for load_data that conforms with AWS cpg location
aws_cpg_path = os.path.join(top_path_cpg, cpg_name)
source_aws_cpg_path = os.path.join(aws_cpg_path, source)
images_aws_cpg_path = os.path.join(source_aws_cpg_path, 'images')


# In[ ]:


# this is dataset specific
def get_load_data_IMTM_HepG2(plate_path, aws_plate_path, aws_illum_corr_path, batch, plate, name_scheme, rows):

    ### loop through images in a plate
    images_path = plate_path
    aws_images_path = aws_plate_path
    
    dataframes = []

    load_analysis = pd.DataFrame 
    
    if os.path.isdir(images_path):

        for file_name in os.listdir(images_path):

            if file_name.endswith(".tif"):
                
                # if we have a picture of channel 1 (every 4th picture)
                if file_name.endswith("C01.tif"):
                    
                    # split the filename into parts using the _ separator
                    filename_parts = file_name.split("_")
                    
                    # the Well information is in the second last part
                    well = filename_parts[len(filename_parts)-2]
                    
                    # within the last filename part, we look for the Field information
                    last_part = filename_parts[len(filename_parts)-1]
                    last_index = last_part.rfind("F")
                    
                    # the field information is converted to an integer number
                    field = str(int(last_part[last_index+1:last_index+4]))
                    
                    # will be used to reconstruct the filenames of all 4 channels
                    filename_root = file_name[0:file_name.rfind("L")]            

                    # splits the filename and creates the channel specific name
                    # This naming scheme is specific to IMTM HepG2

                    # The illumination paths are plate dependent
                    # The naming pattern does not impact the illumination path
                    
                    # TODO: use correct name_scheme if else
                    #file_name_DNA = filename_root + "L01A01Z01C01.tif"
                    #file_name_ER = filename_root + "L01A02Z01C02.tif"
                    #file_name_AGP = filename_root + "L01A01Z01C03.tif"
                    #file_name_Mito = filename_root + "L01A02Z01C04.tif"

                    if (name_scheme == 'A'):
                        ## Naming scheme A
                        ### Plate B1001 R1 - R4
                        # process_logger.info("Loaded naming scheme: A")
                        file_name_DNA = filename_root + "L01A04Z01C01.tif"
                        file_name_ER = filename_root + "L01A03Z01C02.tif"
                        file_name_AGP = filename_root + "L01A02Z01C03.tif"
                        file_name_Mito = filename_root + "L01A01Z01C04.tif"

                    elif (name_scheme == 'B'):
                        ## Naming scheme B
                        ### Plate B1002 R1 - R4
                        ### Plate B1003 R1 - R4 
                        ### Plate B1004 R1 - R3
                        # process_logger.info("Loaded naming scheme: B")
                        file_name_DNA = filename_root + "L01A01Z01C01.tif"
                        file_name_ER = filename_root + "L01A02Z01C02.tif"
                        file_name_AGP = filename_root + "L01A01Z01C03.tif"
                        file_name_Mito = filename_root + "L01A02Z01C04.tif"

                    elif (name_scheme == 'C'):
                        ## Naming scheme C
                        ### Plate B1004 R4
                        # process_logger.info("Loaded naming scheme: C")
                        file_name_DNA = filename_root + "L01A01Z01C01.tif"
                        file_name_ER = filename_root + "L01A02Z01C02.tif"
                        file_name_AGP = filename_root + "L01A01Z01C03.tif"
                        file_name_Mito = filename_root + "L01A02Z01C04.tif"

                    elif (name_scheme == 'D'):
                        ## Naming scheme D
                        ### Plate B1005 R1 - R4
                        ### Plate B1006 R1 - R4 
                        ### Plate B1007 R1 - R3
                        # process_logger.info("Loaded naming scheme: D")
                        file_name_DNA = filename_root + "L01A04Z01C01.tif"
                        file_name_ER = filename_root + "L01A03Z01C02.tif"
                        file_name_AGP = filename_root + "L01A02Z01C03.tif"
                        file_name_Mito = filename_root + "L01A01Z01C04.tif"

                    else: 
                        process_logger.error("Name scheme not found")

                    
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
                            'Metadata_Well': [well],
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

            else:

                process_logger.error(".tif not found")

        load_analysis = pd.concat(dataframes,ignore_index=True)

    else:

        process_logger.error("Directory not found")
    
    return load_analysis


# In[ ]:


# The scheme for the file name pattern is extracted from the keyfile

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

            # Gets the naming scheme to use for get_load_data_IMTM_HepG2
            name_scheme_load = barcode_filtered_key_file_load['Name_Scheme'].iloc[0]

            process_logger.info("Naming scheme: " + name_scheme_load)
            
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
            load_data_with_illum = get_load_data_IMTM_HepG2(full_plate_name_path,
                                                   plate_images_aws_cpg_path,
                                                   plate_illum_corr_aws_cpg_path,
                                                   batch,
                                                   barcode_plate_Map_Name_load,
                                                   name_scheme_load,
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




