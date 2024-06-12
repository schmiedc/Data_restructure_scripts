import os
files = os.listdir(os.getcwd())
[os.replace(file, file.replace(" ", "-")) for file in files]
