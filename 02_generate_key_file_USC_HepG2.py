#!/usr/bin/env python
# coding: utf-8

# In[34]:


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
import xml.etree.ElementTree as ET

# Specify the directory path
directory_path = '/home/schmiedc/FMP_Docs/Projects/CellPainting/DataUpload/TestInput_USC/'


# top folder name - determined by Cell Painting Gallery (CPG) admins
cpg_name = 'cpg0036-EU-OS-bioactives'

# Source of data
source = 'USC'
cell_type = 'HepG2'

# Get all folder names in the directory
folder_names = [name for name in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, name))]

# Create a DataFrame with the folder names
key_file = pd.DataFrame(folder_names, columns=['Assay_Plate_Barcode'])


# In[35]:


# From name extract: imaging_date, plate_name
# Function to extract B1001
def extract_plate_name(s):
    return s.split('_')[0]

# Function to extract n1 (with n replaced by R)
def extract_replicate_number(s):
    n_value = s.split('_')[1]
    return n_value.replace('n', 'R')

# Apply the functions to the DataFrame and create new columns
key_file['plate_name'] = key_file['Assay_Plate_Barcode'].apply(extract_plate_name)
key_file['replicate_number'] = key_file['Assay_Plate_Barcode'].apply(extract_replicate_number)


# In[36]:


# Lists to store extracted data
assay_plate_barcode_ID = []
dates = []
times = []

# For batch_name 
assay_plate_barcode_list = key_file['Assay_Plate_Barcode'].unique()

for assay_plate_barcode in assay_plate_barcode_list:
    
    xml_path = directory_path + assay_plate_barcode + os.sep + '/Images/Index.idx.xml'

    # Parse the XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Namespace dictionary for XPath queries
    namespace = {'ns': 'http://www.perkinelmer.com/PEHH/HarmonyV5'}

    # Find the MeasurementStartTime element and extract its text
    measurement_start_time = root.find('.//ns:MeasurementStartTime', namespace).text

    # Extract date and time components
    date_part = measurement_start_time.split('T')[0]
    time_part = measurement_start_time.split('T')[1].split('.')[0]
    time_part = time_part.replace(':', '-')
    
    # Append to lists
    assay_plate_barcode_ID.append(assay_plate_barcode)
    dates.append(date_part)
    times.append(time_part)

timedata = {
    'Assay_Plate_Barcode': assay_plate_barcode_ID,
    'imaging_date': dates,
    'imaging_time': times
}

timedata_df = pd.DataFrame(timedata)    
timedata_df.head()

key_file = pd.merge(key_file, timedata_df, on='Assay_Plate_Barcode', how='left')
key_file.head()


# In[37]:


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


# In[38]:


key_file.head()


# In[ ]:




