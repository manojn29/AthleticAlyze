# To remove duplicates
import pandas as pd
file_name_output = "Dir0/final.csv"
file_name = "Dir0/combined_csv.csv"
df = pd.read_csv(file_name, sep=",")
df.drop_duplicates('id', keep = 'last', inplace=True)
df.to_csv(file_name_output, index=False)

import hashlib
unique_ids = set()
with open("combined_csv_ALL.csv", "r") as input_file, open("final_ALL.csv", "w") as output_file:
    header = input_file.readline()
    output_file.write(header)
    
    for line in input_file:
        fields = line.strip().split(",")
        current_id = fields[0]
        if current_id not in unique_ids:
            unique_ids.add(current_id)
            output_file.write(line)

# To combine csv files
import pandas as pd
extension = 'csv'
import os, glob
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')

# To check duplicate rows in csv
import csv
distinct = set()
fullcount = 0
filename = "Dir0/final.csv"
duplicates = []
with open(filename, 'r', newline='') as file:
    reader = csv.reader(file)
    next(reader, None)
    for row in reader:
        if str(row[0]) in distinct:
            duplicates.append(str(row[0]))
        fullcount += 1
        distinct.add(str(row[0]))

print("Full count:", fullcount)
print("distinct count", len(distinct))

# To remove non-ascii characters and links
import pandas as pd
import re
#add path
data = pd.read_csv("C:/Users/sanja/Desktop/Quarter Material/Winter Quarter/INFO/final_ALL.csv", encoding='utf-8')
data["tweet"] = data["tweet"].str.replace("RT", "", regex=True)
print(data.shape)
data['tweet'] = data['tweet'].str.encode('utf-8').str.decode('unicode_escape')
def remove_non_ascii(text):
    text = re.sub(r'http\S+','', text)
    text = re.sub(r'[^\x00-\x7F]+','', text)
    return text

data['tweet'] = data['tweet'].apply(remove_non_ascii)
data['tweet'] = data['tweet'].str.replace('\n','',regex = True)
data["tweet"] = data["tweet"].str.replace("b'","",regex = True)
data["tweet"] = data["tweet"].str.replace('b"',"",regex = True)
data['tweet'] = data['tweet'].apply(lambda x: x.rstrip('"') if x.endswith('"') else x)
data['tweet'] = data['tweet'].apply(lambda x: x.rstrip("'") if x.endswith("'") else x)

data.to_csv("C:/Users/sanja/Desktop/Quarter Material/Winter Quarter/INFO/datas/final_preprocessed_data.csv",index = False)