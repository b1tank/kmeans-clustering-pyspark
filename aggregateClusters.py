import csv 
import sys
import os
import re

out_dir = "output_csv/"
labels = set()

files = [out_dir + x for x in os.listdir(out_dir) if os.path.getsize(out_dir + x) > 0 and re.match(r'part-\d{5}', x)]
out = open("map-vis.csv", "a")

for inp_name in files:
    with open(inp_name, "rb") as inFile:
        parser = csv.reader(inFile, delimiter=',')
        for row in parser:
            out.write(row[0]+','+row[1].strip('(')+','+row[2].strip(")")+'\n')
    print inp_name + " conversion completed!"
out.close()
