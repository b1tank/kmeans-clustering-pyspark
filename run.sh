#!/bin/bash
rm -rf output_csv
spark-submit --master local[2] kmeans.py GreatCircle 10 file:/home/training/sharewithHost/fp3/Prj_output/Coordinates/part-*
