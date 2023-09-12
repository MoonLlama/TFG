#!/bin/bash

# Define the folder name
folder="dailyRadData"

# Create the folder if it doesn't exist
if [ ! -d "$folder" ]; then
    mkdir -p "$folder"
fi

# Define the file names
filename="$folder/radSalamanca_$(date +%Y%m%d).png"
ir_filename="$folder/radIrSalamanca_$(date +%Y%m%d).png"

# Download the files
wget -O "$filename" "https://www.aemet.es/imagenes_d/eltiempo/observacion/radiacionuv/radSalamanca.png"
wget -O "$ir_filename" "https://www.aemet.es/imagenes_d/eltiempo/observacion/radiacionuv/irSalamanca.png"