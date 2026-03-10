import pandas as pd
from pathlib import Path
from typing import Dict
import pickle

current_dir = Path(__file__).resolve().parent / "data"

sen_age_sex = pd.read_csv(current_dir / "sen_age_sex_.csv")
sen_secondary_need = pd.read_csv(current_dir / "sen_secondary_need_.csv")

# print(len(sen_age_sex))
# print(sen_age_sex.columns)
# print(sen_age_sex.iloc[1000])
# print(sen_age_sex.iloc[100:105])
# print(sen_age_sex.sample(5))

#create map for new LA code -> name

local_authorities : Dict[str, str] = {}

local_authorities = (
    sen_secondary_need
    .drop_duplicates("new_la_code")
    .set_index("new_la_code")["la_name"]
    .to_dict()
)
    
with (current_dir / "la_map.pkl").open("wb") as f:
    pickle.dump(local_authorities, f)
    






