import csv 
import sys
import os
import re

out_dir = "output_csv/"
labels = set()

files = [out_dir + x for x in os.listdir(out_dir) if os.path.getsize(out_dir + x) > 0 and re.match(r'part-\d{5}', x)]

for inp_name in files:
    with open(inp_name, "rb") as inFile:
        parser = csv.reader(inFile, delimiter=',')
        out = None
        
        i = 0
        for row in parser:
            if (i == 0):
                out = open(out_dir + row[0]+"-vis", "w")
                labels.add(row[0])

            if (row[0] not in labels):
                out.close()
                out = open(out_dir + row[0]+"-vis", "w")
                labels.add(row[0])
            out.write(row[1].strip('(')+','+row[2].strip(")")+'\n')
            i += 1
        out.close()
    print inp_name + "conversion completed!"
