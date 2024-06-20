#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
# directory_path = '/euopen/screeningunit/Bioactives/Transfer/Medina_HepG2/'
directory_path = '/home/schmiedc/FMP_Docs/Projects/CellPainting/DataUpload/TestInput_Medina/'


# top folder name - determined by Cell Painting Gallery (CPG) admins
cpg_name = 'cpg0036-EU-OS-bioactives'

# Source of data
source = 'MEDINA'
cell_type = 'HepG2'

# Get all folder names in the directory
folder_names = [name for name in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, name))]

# Create a DataFrame with the folder names
key_file = pd.DataFrame(folder_names, columns=['Assay_Plate_Barcode'])


# In[15]:


# From name extract: imaging_date, plate_name
# pattern = r'210809(R\d+)(B\d+)__(\d{4}-\d{2}-\d{2})T(\d{2}_\d{2}_\d{2})-' 
# The Medina code works as follows: Copy-1 is the Replicate number
# The EU-OS Number corresponds to the EU-OS plate number:
# 33 = B1001 until 39 = B1007 
# Define a function to extract the required parts

def extract_date(s):
    match = re.search(r'__(\d{4}-\d{2}-\d{2})T', s)
    return match.group(1) if match else None

def extract_time(s):
    match = re.search(r'T(\d{2}_\d{2}_\d{2})-', s)
    return match.group(1) if match else None

def extract_replicate_number(s):
    match = re.search(r'_Copy-(\d)__', s)
    return f"R{match.group(1)}" if match else None


# Apply the functions to the DataFrame and create new columns
key_file['imaging_date'] = key_file['Assay_Plate_Barcode'].apply(extract_date)
key_file['imaging_time'] = key_file['Assay_Plate_Barcode'].apply(extract_time)
key_file['replicate_number'] = key_file['Assay_Plate_Barcode'].apply(extract_replicate_number)


def extract_plate_ID(s):
    match = re.search(r'EU-OS-(\d{6})_', s)
    return match.group(1) if match else None

# Apply the function to the DataFrame and create a new column
key_file['Medina_plate_name'] = key_file['Assay_Plate_Barcode'].apply(extract_plate_ID)

# Map from Medina plate ID to EU-OS plate ID
mapping = {
    '000033': 'B1001',
    '000034': 'B1002',
    '000035': 'B1003',
    '000036': 'B1004',
    '000037': 'B1005',
    '000038': 'B1006',
    '000039': 'B1007'
}

# Map the original_id column to new_id using the mapping dictionary
key_file['plate_name'] = key_file['Medina_plate_name'].map(mapping)


# In[16]:


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

