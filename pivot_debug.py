import pandas as pd
from Ingestion.dataset_builder import slugify
raw = pd.read_csv(r"Data/Quantitative - Schools, pupils and their characteristics/data-small/spc_pupils_fsm.csv")
raw = raw[(raw['geographic_level']=='Local authority') & (raw['phase_type_grouping']=='Total')]
primary_cols=['time_period','new_la_code']
cat_col='fsm'; cnt_col='headcount'
raw_cats = (
    raw[cat_col]
    .dropna()
    .astype(str)
    .map(lambda x: x.strip())
    .unique()
    .tolist()
)
raw_to_key = {rc: slugify(rc) for rc in raw_cats}
seen={}
for rc,k in list(raw_to_key.items()):
    if k not in seen:
        seen[k]=1
    else:
        seen[k]+=1
        raw_to_key[rc]=f"{k}_{seen[k]}"

csv_dict = {raw_to_key[rc]: rc for rc in raw_cats}

tmp = raw[primary_cols + [cat_col, cnt_col]].copy()
for col in primary_cols:
    tmp[col] = tmp[col].astype(str).str.strip()
tmp[cat_col] = tmp[cat_col].astype(str).str.strip().map(raw_to_key)
tmp[cnt_col] = pd.to_numeric(tmp[cnt_col], errors='coerce')
pivot = tmp.pivot_table(index=primary_cols, columns=cat_col, values=cnt_col, aggfunc='sum').reset_index()
print(pivot.head())
print('columns:', pivot.columns[:10])
