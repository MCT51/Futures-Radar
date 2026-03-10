from Ingestion.dataset_builder import build_structured_from_csv, SecondarySpec

spec = SecondarySpec(
    name='fsm_eligibility',
    display_name='FSM eligibility',
    type='qual_dist',
    category_col='fsm',
    count_col='headcount'
)
json_out, csv_out = build_structured_from_csv(
    raw_csv_path=r'Data/Quantitative - Schools, pupils and their characteristics/data-small/spc_pupils_fsm.csv',
    dataset_name='test_fsm_builder',
    primary_cols=['time_period','new_la_code','la_name'],
    secondary_specs=[spec],
    filters=[{"col":"geographic_level","op":"==","value":"Local authority"}],
    out_dir=r'Ingestion/test/output'
)
print(json_out)
print(csv_out)
