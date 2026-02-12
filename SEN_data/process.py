import pandas as pd
from pathlib import Path
from typing import Dict

current_dir = Path(__file__).resolve().parent / "data"

sen_age_sex = pd.read_csv(current_dir / "sen_age_sex_.csv")
sen_secondary_need = pd.read_csv(current_dir / "sen_secondary_need_.csv")

print(len(sen_age_sex))
print(sen_age_sex.columns)
print(sen_age_sex.iloc[1000])
print(sen_age_sex.iloc[100:105])
print(sen_age_sex.sample(5))

"""
Target schema
- Year (2016, 2017, ...)
- Total SEN number
- Total SEN by age



"""


#create map for new LA code -> name

local_authorities : Dict[str, str] = {}

for row in sen_secondary_need:
    local_authorities.setdefault(row["new_la_code"], row["la_name"])
    






