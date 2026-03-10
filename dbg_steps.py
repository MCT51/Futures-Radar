import pandas as pd
from Ingestion.dataset_builder import SecondarySpec, build_structured_from_csv, apply_filters, slugify
from Ingestion.primary_variable import PrimaryVariable
from pathlib import Path
import json
raw_path = Path('Data/Quantitative - Schools, pupils and their characteristics/data-small/spc_pupils_fsm.csv')
raw = pd.read_csv(raw_path)
raw.columns = raw.columns.str.strip()
filters=[{"col":"geographic_level","op":"==","value":"Local authority"},{"col":"phase_type_grouping","op":"==","value":"Total"}]
from Ingestion.dataset_builder import apply_filters
raw = apply_filters(raw, filters)
primary_cols=['time_period','new_la_code']
df_work = raw[primary_cols].copy()
for col in primary_cols:
    df_work[col]=df_work[col].astype(str).str.strip()
secondary_specs=[SecondarySpec(name='fsm', display_name='FSM', type='qual_dist', category_col='fsm', count_col='headcount')]
secondary_vars=[]
for spec in secondary_specs:
    cat_col=spec.category_col
    cnt_col=spec.count_col
    raw_cats=(raw[cat_col].dropna().astype(str).map(lambda x:x.strip()).unique().tolist())
    from Ingestion.dataset_builder import slugify
    raw_to_key={rc:slugify(rc) for rc in raw_cats}
    seen={}
    for rc,k in list(raw_to_key.items()):
        if k not in seen:
            seen[k]=1
        else:
            seen[k]+=1
            raw_to_key[rc]=f"{k}_{seen[k]}"
    csv_dict={raw_to_key[rc]:rc for rc in raw_cats}
    tmp=raw[primary_cols+[cat_col,cnt_col]].copy()
    for col in primary_cols:
        tmp[col]=tmp[col].astype(str).str.strip()
    tmp[cat_col]=tmp[cat_col].astype(str).str.strip().map(raw_to_key)
    tmp[cnt_col]=pd.to_numeric(tmp[cnt_col], errors='coerce')
    pivot=(
        tmp.pivot_table(index=primary_cols, columns=cat_col, values=cnt_col, aggfunc='sum')
        .reset_index()
    )
    rename_map={k:f"{k}_count" for k in csv_dict.keys() if k in pivot.columns}
    pivot=pivot.rename(columns=rename_map)
    print(pivot.head())
    df_work=df_work.merge(pivot, on=primary_cols, how='left')
print(df_work.head())
print(df_work[[c for c in df_work.columns if c.endswith('_count')]].sum().head())
