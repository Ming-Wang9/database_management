#!/bin/bash

echo "Runing data_parse.py"
python3 parse.py ebay_data/items-*.json

echo "sort and removing duplicates.."
sort -u items.dat -o items.dat
sort -u categories.dat -o categories.dat
sort -u bids.dat -o bids.dat
sort -u users.dat -o users.dat

echo "done, you can use it for SQLite import now"
