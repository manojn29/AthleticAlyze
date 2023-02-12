import pandas as pd

data = pd.read_csv("final_ALL.csv")
data["tweet"] = data["tweet"].str.replace("RT","",regex = True)
data["tweet"] = data["tweet"].str.replace("b'","",regex = True)
data["tweet"] = data["tweet"].str.replace('b"',"",regex = True)

data.to_csv("final.csv")
