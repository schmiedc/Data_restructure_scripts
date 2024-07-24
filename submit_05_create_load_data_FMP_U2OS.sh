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
/euopen/screeningunit/Bioactives/transfer-env/bin/python $base_path/05_create_load_data_csv_FMP_U2OS.py $base_path

# deactivate transfer-env
deactivate