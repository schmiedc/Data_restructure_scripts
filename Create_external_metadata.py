#!/usr/bin/env python
# coding: utf-8

# # external_metadata.tsv 
# 
# * These contain mapping between a perturbation identifier to other metadata using matching column names. 
# 
# | Metadata_EOS  | ...  |
# |---------------|------|
# | EOSXXXXXX     |      |
# | DMSO          |      |
# | Tetrandrine   |      |
# | Nocodazole    |      |
# | ...           |      |
# 
# 
# 
# Metadata_EOS
# EU-OS_Library
# pdid
# smiles
# inchi, 
# inchikey
# 
#     
# TODO: create external metadata
# 
# TODO: load external metdata modify to match 
# 
# TODO: save as external_metadata.tsv
# 

# In[24]:


import pandas as pd

# location of annotation
annot_path = '/home/schmiedc/FMP_Docs/Projects/CellPainting/DataUpload/Annotations/'
annot_name = '2022-05-13_Labels_Bioactives_Combined_with_Broad_MeSH_JUMP-CP.csv'

annot_file = pd.read_csv(annot_path + annot_name)


# In[25]:


annot_file["EU-OS_Library"] = 'ECBL'

annot_file_revise = annot_file[["EOS",
                                "EU-OS_Library",
                                "pdid",
                                "EUopen_smiles",
                                "EUopen_inchi", 
                                "EUopen_inchikey"]]


# In[26]:


annot_file_revise = annot_file_revise.rename(columns={"EOS": "Metadata_EOS",
                                                      "EUopen_smiles": "smiles", 
                                                      "EUopen_inchi" : "inchi",
                                                      "EUopen_inchikey" : "inchikey"
                                                     })


# In[27]:


filename_external_metadata = annot_path + 'external_metadata.tsv'
annot_file_revise .to_csv(filename_external_metadata, sep="\t") 


# In[ ]:




