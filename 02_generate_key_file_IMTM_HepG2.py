#!/usr/bin/env python
# coding: utf-8

# In[11]:


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
directory_path = '/home/schmiedc/FMP_Docs/Projects/CellPainting/DataUpload/TestInput_IMTM/'


# top folder name - determined by Cell Painting Gallery (CPG) admins
cpg_name = 'cpg0036-EU-OS-bioactives'

# Source of data
source = 'IMTM'
cell_type = 'HepG2'

# Get all folder names in the directory
folder_names = [name for name in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, name))]

# Create a DataFrame with the folder names
key_file = pd.DataFrame(folder_names, columns=['Assay_Plate_Barcode'])


# In[12]:


# From name extract: imaging_date, plate_name
# Function to extract the first number with 'B100' added before it
def extract_plate_name(s):
    first_number = s.split('_')[1]
    return 'B100' + first_number

# Function to extract the second number with 'R' added before it
def extract_replicate_number(s):
    second_number = s.split('_')[-1]
    return 'R' + second_number


# Apply the functions to the DataFrame and create new columns
key_file['plate_name'] = key_file['Assay_Plate_Barcode'].apply(extract_plate_name)
key_file['replicate_number'] = key_file['Assay_Plate_Barcode'].apply(extract_replicate_number)


# In[13]:


# Lists to store extracted data
assay_plate_barcode_ID = []
dates = []
times = []

# For batch_name 
assay_plate_barcode_list = key_file['Assay_Plate_Barcode'].unique()

for assay_plate_barcode in assay_plate_barcode_list:
    
    xml_path = directory_path + assay_plate_barcode + os.sep + '/MeasurementData.mlf'

    # Parse the XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Namespace dictionary for XPath queries
    namespace = {'bts': 'http://www.yokogawa.co.jp/BTS/BTSSchema/1.0'}

    # Find the first MeasurementRecord element
    first_measurement_record = root.find('.//bts:MeasurementRecord', namespace)

    # Ensure the element was found
    if first_measurement_record is not None:
        
        # Extract the bts:Time attribute using the fully qualified name with the namespace
        time_attr = first_measurement_record.get('{http://www.yokogawa.co.jp/BTS/BTSSchema/1.0}Time')

        # Ensure the time attribute was found
        if time_attr:
            # Split the time to get date and time separately
            date_part = time_attr.split('T')[0]
            time_part = time_attr.split('T')[1].split('+')[0]
            
            # Remove milliseconds if present
            time_part = time_part.split('.')[0]

            # Convert time_part from "00:00:00.000000" to "00-00-00"
            time_part = time_part.replace(':', '-')
            
            # Append to lists
            assay_plate_barcode_ID.append(assay_plate_barcode)
            dates.append(date_part)
            times.append(time_part)

        else:
            
            print("Time attribute not found in the first MeasurementRecord element.")
            
    else:
        
        print("No MeasurementRecord element found.")

timedata = {
    'Assay_Plate_Barcode': assay_plate_barcode_ID,
    'imaging_date': dates,
    'imaging_time': times
}

timedata_df = pd.DataFrame(timedata)    
timedata_df.head()

key_file = pd.merge(key_file, timedata_df, on='Assay_Plate_Barcode', how='left')
key_file.head()


# In[14]:


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


# In[15]:


key_file.head()


# In[ ]:




