#!/usr/bin/env python
# coding: utf-8

# In[1]:


from io import StringIO
import pandas as pd
from pathlib import Path
import os
import logging


# In[ ]:


# TODO: Create script that checks if files exists based on load_data_csv

load_data_csv_dir = '/euopen/screeningunit/Bioactives/Transfer/ReadyForUpload/cpg0036-EU-OS-bioactives/IMTM/workspace/load_data_csv/'
logger_dir = '/euopen/screeningunit/Bioactives/Transfer/IMTM_HepG2/'

batchlist = ['2022_02_16_Batch1_HepG2',
'2022_03_02_Batch2_HepG2',
'2022_03_04_Batch3_HepG2',
'2022_03_29_Batch4_HepG2',
'2022_03_30_Batch5_HepG2',
'2022_03_31_Batch6_HepG2',
'2022_05_20_Batch7_HepG2']



# In[ ]:


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
log_path = logger_dir + os.path.sep + 'Log_check_missing_files.log'
process_logger = setup_logger('first_logger', log_path)

os_warning_path = logger_dir + os.path.sep + 'os-error_check_missing_files.log'
os_warning = setup_logger('second_logger', os_warning_path)


# In[ ]:


process_logger.info("Input path: " + load_data_csv_dir)


# In[ ]:


old_base = "/home/ubuntu/bucket/"
image_base_dir = '/euopen/screeningunit/Bioactives/Transfer/ReadyForUpload/'

check_results = []

for batch in batchlist:

    process_logger.info("Checking batch: " + batch)

    batch_path = load_data_csv_dir + batch

    for plate in os.listdir(batch_path):

        process_logger.info("Checking plate: " + plate)

    # TODO: walk through the plates to load the load_data.csv file
   
        batch_plates_load_data_path = batch_path + os.sep + plate + os.sep + 'load_data.csv'
        
        df = pd.read_csv(batch_plates_load_data_path)
        
        # List of prefixes you're interested in
        prefixes = ["OrigDNA", "OrigER", "OrigAGP", "OrigMito"]

        # Build a list of file path records
        records = []
        
        for _, row in df.iterrows():

            for prefix in prefixes:

                path_col = f"PathName_{prefix}"
                file_col = f"FileName_{prefix}"
                
            full_path = os.path.join(
                row[path_col].replace(old_base, image_base_dir),
                row[file_col]
            )

            records.append({
                "FullPath": full_path,
                "Channel": prefix
            })

        for record in records:
            
            try:
                file_exists = os.path.exists(record["FullPath"])  # Check if the file exists

                check_results.append({
                    "FullPath": record["FullPath"],
                    "Channel": record["Channel"],
                    "FileExists": file_exists
                    })
                
                if (file_exists == False):

                     process_logger.info("Missing: " + record["Channel"])

            # Handle unexpected errors (e.g., permission issues, invalid paths)
            except Exception as e:
                
                check_results.append({
                    "FullPath": record["FullPath"],
                    "Channel": record["Channel"],
                    "FileExists": False,
                    "Error": str(e)  # Store the error message for debugging
                    })

# Convert the results to a DataFrame
check_results_df = pd.DataFrame(check_results)

# Save to a CSV file
check_results_df.to_csv(logger_dir + os.sep + "missing_files.csv", index=False)


# In[ ]:




