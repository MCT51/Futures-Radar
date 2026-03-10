# Ingestion Schema + StructuredData

This folder defines a schema system for converting heterogeneous education datasets into a strict, visualization-ready tabular format.

The core idea is:

- Raw source CSVs can be irregular / incomplete.
- Ingestion normalizes them into a strict grid.
- Missing data is preserved as `NaN` and tracked as "undefined sections".
- Visualizations (`Pie`, `Heatmap`, etc.) consume `StructuredData` instead of raw source files.


## Overview

### Primary Variables
Primary variables define the row identity (primary key / grid axes).

Examples:

- `Year`
- `Location (LA code)`

Each primary variable stores:

- `title`
- `column_name`
- `csv_to_display` mapping (`csv_value -> display name`)

Notes:

- `"Total"` is handled implicitly by the framework and should **not** be included in `csv_to_display`.
- Final processed datasets always include a `"Total"` value for every primary variable.


### Secondary Variables
Secondary variables define what can be visualized.

All secondary variables inherit from `SecondaryVariable`.

Implemented types:

- `QuantitativeScalarSecondaryVariable`
- `QualitativeScalarSecondaryVariable`
- `QualitativeDistributionVariable`
- `QuantitativeDistributionVariable`

#### Scalar Variables
One value per row (per primary-key combination).

- Quantitative scalar example: average sentiment score
- Qualitative scalar example: dominant category / mode label

#### Distribution Variables
Category distributions with one count and one percent column per category.

Examples:

- `Age` (quantitative distribution)
- `Sex` (qualitative distribution)
- `SEN type` (qualitative distribution)
- `Ethnicity` (qualitative distribution)

For distributions:

- counts use `<csv_value>_count`
- percents use `<csv_value>_percent`

For quantitative distributions:

- required summaries: `mean`, `median`, `mode`
- summary columns are auto-named from the variable display name (slugged):
  - `<secondary_name>_mean`
  - `<secondary_name>_median`
  - `<secondary_name>_mode`

Mode storage rule:

- Mode values store the **csv key** (not display label).

Quantitative distribution numeric mapping:

- `QuantitativeDistributionVariable` requires `csv_to_number` (`csv_value -> representative numeric value`)
- For bucketed values (e.g. `<5`, `10+`), the representative value must be decided when creating the schema.


## Validation Model

There are two validation phases:

### `checkRawCSV(...)`
Validates a schema-compatible raw/partially-processed CSV.

Rules:

- Primary columns must exist
- Scalar value columns must exist
- Distribution `*_count` columns must exist
- Percent / totals / averages may be missing at this stage
- No duplicate rows for the primary key
- Raw rows may already include `"Total"` values (allowed)
- Empty entries (`NaN`) are allowed

### `checkCSV(...)`
Validates the final processed CSV / dataframe.

Rules:

- All required final columns must exist
- No duplicate rows for the primary key
- Mode values must be valid `csv_dict` keys
- Qualitative scalar values must be valid `csv_dict` keys
- Strict complete grid enforced by default (`strict_complete_grid=True`)

Strict grid means:

- Every combination of primary values (including `"Total"` for each primary) exists as a row.


## Generation / Processing Functions

Implemented on `Schema`:

- `generateTotals(df)`
- `generatePercentages(df)`
- `generateAverages(df)`
- `generateExampleCSV(df=None)`
- `normalizeToStrictStructure(df)` (grid scaffold helper)

All generation functions are intended to act like **upserts**:

- They work whether generated columns already exist or not.
- They preserve `NaN` when there is not enough source data.

### `generateTotals(df)`
Computes total rows for any primary combination containing `"Total"`.

Current rules:

- Distribution variables: sum count columns
- Quantitative scalar variables: mean over available values
- Qualitative scalar variables: mode over available values
- Partial totals are allowed (uses available data)
- If no usable source data exists, generated values remain `NaN`

### `generatePercentages(df)`
For distribution variables:

- computes `<csv_value>_percent` from `<csv_value>_count`
- preserves `NaN` where counts are missing / unusable

### `generateAverages(df)`
For distributions:

- Qualitative distribution: generates `mode`
- Quantitative distribution: generates `mean`, `median`, `mode`


## Undefined Sections

The final dataset uses strict structure even when data is missing.

Missing sections are represented by:

- `NaN` values in the relevant columns
- a defined/undefined map in `StructuredData`

Policy:

- If not all required data for a secondary variable is present for a given primary-key row, that section is treated as undefined/incomplete.


## StructuredData

`StructuredData` is the post-ingestion object used by visualizations.

It contains:

- `dataframe` (strict, processed table)
- `schema` (`Schema` object)
- `defined_map`: `((primary values tuple), secondary_variable_name) -> bool`

Assumptions:

- Totals / percentages / averages are already generated (or upserted)
- `checkCSV(...)` has been run (unless intentionally skipped)

Key methods:

- `StructuredData.from_dataframe(...)`
- `StructuredData.from_csv(...)`
- `StructuredData.save(json_path=..., csv_path=...)`
- `StructuredData.load(json_path=...)`
- `row_for(...)`
- `is_defined(...)`


## JSON / CSV Persistence

### Schema
`Schema` can be saved/loaded as JSON:

- `schema.save_json(path)`
- `Schema.load_json(path)`

### StructuredData
`StructuredData` is saved as:

- JSON metadata (schema + defined map + CSV path reference)
- CSV data (stored separately)

Methods:

- `structured.save(json_path=..., csv_path=...)`
- `StructuredData.load(json_path)`


## How This Fits Into Visualizations

### Pie

- Uses distribution secondary variables
- Filter by primary variables with dropdowns
- Plots category percentages (or counts)

### Line / Bar

- Uses scalar secondary variables
- Quantitative distributions can be converted to scalars via:
  - `to_scalar_mean()`
  - `to_scalar_median()`
  - `to_scalar_mode()`

### Heatmap

- Uses location primary variable vs scalar secondary variable
- Supports:
  - direct scalar values
  - quantitative distribution summaries (`mean`, `median`, `mode`)
  - distribution percentages (with category selection)


## Current Test Builders (`Ingestion/test`)

Scripts that build and save `StructuredData` examples:

- `make_sen_age_sex_structured.py`
- `make_fsm_ethnicity_structured.py`

Outputs are written to:

- `Ingestion/test/output/*.json`
- `Ingestion/test/output/*.csv`


## Example Workflow

1. Build a dataset-specific `Schema`
2. Transform raw source data into schema-compatible count columns
3. Create strict grid (`generateExampleCSV` / `normalizeToStrictStructure`)
4. Upsert generated fields:
   - `generateTotals`
   - `generatePercentages`
   - `generateAverages`
5. Validate:
   - `checkCSV`
6. Wrap in `StructuredData`
7. Save JSON + CSV
8. Load in visualization (`Pie`, `Heatmap`)
