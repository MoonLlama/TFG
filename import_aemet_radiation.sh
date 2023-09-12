#!/bin/bash

# Loop over the files that match the pattern
for file in radiaciondiaria_*.csv; do
    # Build the command string with the current file name
    cmd="python3 aemet_radiacion.py --filename $file"

    # Run the command
    $cmd
done
