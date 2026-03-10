import pandas as pd
from pathlib import Path
from Ingestion.dataset_builder import SecondarySpec, slugify, apply_filters
from Ingestion.primary_variable import PrimaryVariable
from Ingestion.secondary_variable import QualitativeDistributionVariable
from Ingestion.schema import Schema
raw_path = Path("Data/Quantitative - Schools, pupils and their characteristics/data-small/spc_pupils_fsm.csv")
raw = pd.read_csv(raw_path)
raw.columns = raw.columns.str.strip()
filters=[{"col":"geographic_level","op":"==","value":"Local authority"},{"col":"phase_type_grouping","op":"==","value":"Total"}]
raw = apply_filters(raw, filters)
primary_cols=['time_period','new_la_code']
primary_vars=[]
for col in primary_cols:
    vals=(raw[col].dropna().astype(str).map(lambda x:x.strip()).unique().tolist())
    csv_to_display={v:v for v in sorted(vals)}
    primary_vars.append(PrimaryVariable(title=col.replace('_',' ').title(), column_name=col, csv_to_display=csv_to_display))

spec = SecondarySpec(name='fsm', display_name='FSM', type='qual_dist', category_col='fsm', count_col='headcount')

raw_cats=(raw[spec.category_col].dropna().astype(str).map(lambda x:x.strip()).unique().tolist())
raw_to_key={rc: slugify(rc) for rc in raw_cats}
seen={}
for rc,k in list(raw_to_key.items()):
    if k not in seen:
        seen[k]=1
    else:
        seen[k]+=1
        raw_to_key[rc]=f"{k}_{seen[k]}"

csv_dict={raw_to_key[rc]:rc for rc in raw_cats}
secondary_vars=[QualitativeDistributionVariable(display_name=spec.display_name, csv_dict=csv_dict, variable_name=spec.name)]

df_work = raw[primary_cols].copy()
for col in primary_cols:
    df_work[col]=df_work[col].astype(str).str.strip()

cat_col=spec.category_col
cnt_col=spec.count_col
tmp = raw[primary_cols + [cat_col, cnt_col]].copy()
for col in primary_cols:
    tmp[col]=tmp[col].astype(str).str.strip()
tmp[cat_col]=tmp[cat_col].astype(str).str.strip().map(raw_to_key)
tmp[cnt_col]=pd.to_numeric(tmp[cnt_col], errors='coerce')
pivot=(tmp.pivot_table(index=primary_cols, columns=cat_col, values=cnt_col, aggfunc='sum').reset_index())
rename_map={k:f"{k}_count" for k in csv_dict.keys() if k in pivot.columns}
pivot=pivot.rename(columns=rename_map)
df_work=df_work.merge(pivot, on=primary_cols, how='left')
print('after merge sum:', df_work[[c for c in df_work.columns if c.endswith('_count')]].sum().head())

schema = Schema(primary_variables=primary_vars, secondary_variables=secondary_vars)
df_strict = schema.normalizeToStrictStructure(df_work)
print('after normalize sum:', df_strict[[c for c in df_strict.columns if c.endswith('_count')]].sum().head())
