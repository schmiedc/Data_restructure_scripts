#!/usr/bin/env python
# coding: utf-8

# # Data restructering preparations
# 
# 1. Copy original dataset to location on shark: Source_CellLine/ contains all plates
# 2. On copy remove any spaces or special signs with removeSpace.py
# 

# # Create Key File
# 
# 1. For each dataset generate the Keyfile that is used for generating the batch_name and plate_name
# 
# Keyfile will be saved with in the folder Source_CellLine/ folder 

# In[1]:


# Create key_file.csv 
# This creates all necessary information from converting from our data structure to the broad data structure

# Program after copy of original files to transfer directory
# Removing any spaces and special signs from name

# Specify the Source

# load all folder names into file
# Folder names will be Assay_Plate_Barcode
# From name extract: Cell_Type, imaging_date, plate_name
# Alt. supplement information from somewhere else
# Generate and save a "Key file"
# Assay_Plate_Barcode, Source, Plate_Map_Name, Imaging_Date, Cell_Type, Batch_Name

import os
import pandas as pd
import re

# Specify the directory path
directory_path = '/home/schmiedc/FMP_Docs/Projects/CellPainting/DataUpload/TestInput_corr/'

# top folder name - determined by Cell Painting Gallery (CPG) admins
cpg_name = 'cpg0036-EU-OS-bioactives'

# Source of data
source = 'FMP'
cell_type = 'U2OS'

# Get all folder names in the directory
folder_names = [name for name in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, name))]

# Create a DataFrame with the folder names
key_file = pd.DataFrame(folder_names, columns=['Assay_Plate_Barcode'])

# From name extract: imaging_date, plate_name
# Alt. supplement information from somewhere else
# Generate and save a "Key file"
# Assay_Plate_Barcode, Source, Plate_Map_Name, Imaging_Date, Cell_Type, Batch_Name
pattern = r'U2OS(B\d+)(R\d+)__(\d{4}-\d{2}-\d{2})T(\d{2}_\d{2}_\d{2})-'

# Function to extract parts from a string
def extract_parts(s):
    match = re.search(pattern, s)
    if match:
        return match.group(1), match.group(2), match.group(3), match.group(4)
    return None, None, None, None

# Apply the function to the DataFrame
key_file[['plate_name', 'replicate_number', 'imaging_date', 'imaging_time']] = key_file['Assay_Plate_Barcode'].apply(lambda x: pd.Series(extract_parts(x)))

# add top folder to keyfile
key_file['cpg_name'] = cpg_name

# add source and cell type
key_file['source'] = source
key_file['cell_type'] = cell_type

# create the Plate_Map_name value
key_file['Plate_Map_Name'] = key_file['plate_name'] + '_' + key_file['replicate_number']

# Add Batch_Number
key_file['Batch_Number'] = key_file.groupby('imaging_date').ngroup() + 1
key_file['Batch_Number'] = key_file['Batch_Number'].astype(str)

# create the Batch_Name
key_file['Batch_Name'] = key_file['imaging_date'] + '_Batch' + key_file['Batch_Number'] + '_' + key_file['cell_type']

# adjust Batch_Name to use YYYY_MM_DD format
key_file['Batch_Name'] = key_file['Batch_Name'].str.replace('-', '_') 

key_file.to_csv(directory_path + source + '_' + cell_type + '_keyfile.csv')  


# In[ ]:




