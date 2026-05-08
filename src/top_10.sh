#!/bin/bash

PATH_PROGETTO="$HOME/Documenti/secondo_progetto_big_data"
DATA="$PATH_PROGETTO/results"

mkdir -p "$PATH_PROGETTO/ris_top_10"

cd "$DATA"

echo "Inizio generazione prime 10 righe"

for file in *; do

    if echo "$file" | grep -qE '^.*100\.csv$'; then
        head -n 11 "$file" > "$PATH_PROGETTO/ris_top_10/$file"
        echo "File $file salvato in $PATH_PROGETTO/ris_top_10/$file"
    fi

done

echo "Finito!"