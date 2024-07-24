#!/bin/bash
#
#SBATCH --job-name=Generate_key_file
#SBATCH --nodes=1

# activate transfer-env
source /euopen/screeningunit/Bioactives/transfer-env/bin/activate
echo "Activated transfer-env"

# get the directory where the script is executed from
base_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo "Script directory: $base_path"

# call python and execute script on base path
/share/apps/python-3.9/bin/python3.9 $base_path/03_create_data_structure.py $base_path

# deactivate transfer-env
deactivate