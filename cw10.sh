#!/bin/bash

# -------------------------
# Changelog:
# cw10 - 12.01.2025
# Author: Joanna Papros
# Description: Script to unpacking data, load them to database and export to csv
# -------------------------

# Parameters
INDEKS="405293"
TIMESTAMP=$(date +%m%d%Y)
LOG="cw10.log"
URL="http://home.agh.edu.pl/~wsarlej/dyd/bdp2/materialy/cw10/InternetSales_new.zip"
FILE_PASSWORD="bdp2agh"
DB_HOST="localhost"
DB_USER="postgres"
DB_PASSWORD="MTIzNA=="
DB_NAME="cw10"
TABLE="customers_${INDEKS}"
INPUT="InternetSales_new.txt"
OUTPUT="InternetSales_new.csv"
BAD="InternetSales_new.bad_${TIMESTAMP}"
PROCESSED="PROCESSED"

log() {
  echo "$1: $2 - $(date +%Y%m%d%H%M%S)" >> "$LOG"
}
log "Script" "Start"

# Decode
DB_PASSWORD=$(echo $DB_PASSWORD | base64 --decode)

# A - download
log "Download" "Start"
curl -L "$URL" -o InternetSales_new.zip
log "Download" "Finish"

# B - unzip
log "Unzip" "Start"
unzip -o -P "$FILE_PASSWORD" InternetSales_new.zip
log "Unzip" "Finish"

# C - validation
log "Validation" "Start"
 
validation() {

    HEADER=$(head -n 1 "$INPUT")
    COLUMN_COUNT=$(echo "$HEADER" | awk -F'|' '{print NF}')
 
    mapfile -t LINES < <(tail -n +2 "$INPUT")
    for LINE in "${LINES[@]}"; do
        # C.1 - empty lines
        if [[ -z "$LINE" ]]; then
            continue
        fi
 
        # C.3 - column count
        LINE_COLUMN_COUNT=$(echo "$LINE" | awk -F'|' '{print NF}')
        if [[ "$LINE_COLUMN_COUNT" -ne "$COLUMN_COUNT" ]]; then
            echo "$LINE" >> "$BAD"
            continue
        fi
 
		#Get values
        IFS='|' read -r ProductKey CurrencyAlternateKey Customer_Name OrderDateKey OrderQuantity UnitPrice SecretCode <<< "$LINE"
 
        # C.4 - OrderQuantity
        if [[ "$OrderQuantity" -gt 100 ]]; then
            echo "$LINE" >> "$BAD"
            continue
        fi
 
        # C.5 - SecretCode
        SecretCode=""
 
        # C.6/C.7 - Customer_Name -> Last name & First name
        if [[ "$Customer_Name" == *","* ]]; then
            LAST_NAME=${Customer_Name%%,*}  
            FIRST_NAME=${Customer_Name#*,} 
            LAST_NAME=$(echo "$LAST_NAME" | tr -d '"')
            FIRST_NAME=$(echo "$FIRST_NAME" | tr -d '"')
        else
            echo "$LINE" >> "$BAD"
            continue
        fi
        VALID_LINE="$ProductKey|$CurrencyAlternateKey|$LAST_NAME|$FIRST_NAME|$OrderDateKey|$OrderQuantity|$UnitPrice"
        echo "$VALID_LINE" >> "$OUTPUT"
    done
}
 
> "$OUTPUT"
> "$BAD"

HEADER_NEW=$(echo "$HEADER" | sed 's/Customer_Name/LAST_NAME|FIRST_NAME/')
echo "$HEADER_NEW" > "$OUTPUT"

validation
 
# C.2 - Duplicates 
sort "$OUTPUT" | uniq -u > "${OUTPUT}.tmp"
sort "$OUTPUT" | uniq -d > "$OUTPUT"
mv "${OUTPUT}.tmp" "$OUTPUT"


log "Validation" "Finish"


# D - Create table
log "Create Table" "Start"
TABLE_EXISTS=$(PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '$TABLE');" | tr -d '[:space:]')
if [[ "$TABLE_EXISTS" == 't' ]]; then
  PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "TRUNCATE TABLE $TABLE;"
  if [[ $? -eq 0 ]]; then
    log "Create Table" "Finish"
  fi
else
  PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "
  CREATE TABLE $TABLE (
    ProductKey VARCHAR,
    CurrencyAlternateKey VARCHAR,
    LastName VARCHAR,
    FirstName VARCHAR,
    OrderDateKey VARCHAR,
    OrderQuantity VARCHAR,
    UnitPrice VARCHAR,
    SecretCode VARCHAR
  );" 
  if [[ $? -eq 0 ]]; then
    log "Create Table" "Finish"
  fi
fi

# E - Data
log "Data" "Start"
PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "\COPY $TABLE(ProductKey, CurrencyAlternateKey, LastName, FirstName, OrderDateKey, OrderQuantity, UnitPrice) FROM '$OUTPUT' DELIMITER '|' CSV HEADER;" 
if [[ $? -eq 0 ]]; then
  log "Data" "Finish"
fi

# F - Archiving
log "Archive" "Start"
if [ ! -d "$PROCESSED" ]; then
  mkdir -p "$PROCESSED"
fi
mv "$OUTPUT" "$PROCESSED/${TIMESTAMP}_$OUTPUT"
if [ $? -eq 0 ]; then
  log "Archive" "Finish"
fi

# G - SecretCode
log "SecretCode" "Start"
PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "
UPDATE $TABLE SET SecretCode = substring(md5(random()::text), 1, 10);
"
if [[ $? -eq 0 ]]; then
  log "SecretCode" "Finish"
fi

# H - Export
EXPORT_FILE="${TABLE}.csv"
log "Export" "Start"
PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "\COPY (SELECT * FROM $TABLE) TO '$EXPORT_FILE' DELIMITER ';' CSV;"
if [[ -f "$EXPORT_FILE" ]]; then
  mv "$EXPORT_FILE" "$PROCESSED/$EXPORT_FILE"
  log "Export" "Finish"
fi

# I - Zip
log "Zip" "Start"
if tar -czf "$PROCESSED/${EXPORT_FILE}.tar.gz" -C "$PROCESSED" "$EXPORT_FILE"; then
  log "Zip" "Finish"
fi

log "Script" "Finish"
