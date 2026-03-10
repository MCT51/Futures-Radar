from Ingestion.dataset_builder import build_structured_from_csv, SecondarySpec

spec = SecondarySpec(name='fsm', display_name='FSM', type='qual_dist', category_col='fsm', count_col='headcount')
json_out, csv_out = build_structured_from_csv(
    raw_csv_path=r"Data/Quantitative - Schools, pupils and their characteristics/data-small/spc_pupils_fsm.csv",
    dataset_name='debug_finaltestfsm',
    primary_cols=['time_period','new_la_code'],
    secondary_specs=[spec],
    filters=[{"col":"geographic_level","op":"==","value":"Local authority"},{"col":"phase_type_grouping","op":"==","value":"Total"}],
    out_dir=r'Ingestion/test/output',
    display_name_columns={'new_la_code':'la_name'}
)
print(json_out)
print(csv_out)
