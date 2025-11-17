# Code Generation Tool - User Guidance

## Overview
This tool automates the generation of SQL DDL (Data Definition Language) code and metadata from configuration files, Excel mappings, and Jinja2 templates. It's designed to process source-to-target column mappings and generate standardized SQL for data warehouse tables.

## Prerequisites

### Files Required
- `src/config.yml` - Configuration file with paths and template definitions
- `table_list.xlsx` - Excel file with table definitions and template flags
- `source_column.xlsx` - Excel file with source column metadata
- Mapping Excel files - One per template (referenced in config.yml)
- Jinja2 template files - SQL templates for code generation

## Configuration Setup

### config.yml Structure
```yaml
OUTPUT_FOLDER:
  code: /output/code/
  metadata: /output/metadata/

TABLE_LIST: /data/table_list.xlsx
SOURCE_COLUMN: /data/source_column.xlsx

TEMPLATE:
  TEMPLATE_NAME_1:
    mapping_path: /data/mapping_1.xlsx
    location: /template/template_1.jinja
  TEMPLATE_NAME_2:
    mapping_path: /data/mapping_2.xlsx
    location: /template/template_2.jinja
```

## How the Tool Works

### Execution Flow

1. **Load Configuration** → Read config.yml and load all reference Excel files
2. **For Each Template** → Iterate through templates defined in config.yml
3. **Generate Metadata** → Merge source columns with table definitions and mappings
4. **Filter Tables** → Identify tables where template flag = 1
5. **For Each Table** → 
   - Generate JSON column mappings
   - Create directory structure
   - Render Jinja2 template with JSON data
   - Output SQL file

### Data Flow Diagram
```
config.yml
    ↓
source_column.xlsx + table_list.xlsx + mapping.xlsx
    ↓
gen_metadata() → merged metadata DataFrame
    ↓
gen_json_mapping_ddl() → JSON mapping file + Excel metadata
    ↓
gen_output() → SQL file (rendered template)
    ↓
OUTPUT_FOLDER/code/{TABLE_NAME}/{TEMPLATE}/{TABLE_NAME}.sql
OUTPUT_FOLDER/metadata/{TABLE_NAME}/{TEMPLATE}.xlsx
```

## Output Structure

```
OUTPUT_FOLDER/
├── code/
│   └── {TABLE_NAME}/
│       └── {TEMPLATE}/
│           ├── {TABLE_NAME}.json    (column mappings)
│           └── {TABLE_NAME}.sql     (generated SQL)
│
└── metadata/
    └── {TABLE_NAME}/
        └── {TEMPLATE}.xlsx          (metadata export)
```

## Troubleshooting

### Common Issues

**Issue:** `FileNotFoundError: config.yml`
- **Solution:** Ensure `src/config.yml` exists in the script directory

**Issue:** `KeyError: TEMPLATE name`
- **Solution:** Verify template name matches config.yml exactly (case-sensitive)

**Issue:** `Column X cannot be used as HASH_KEY and SURROGATE_KEY at the same time`
- **Solution:** Check mapping file - a column cannot have both flags set to 'Y'

**Issue:** No files generated
- **Solution:** Verify template flag (column) in table_list.xlsx is set to 1 for desired tables

## Best Practices

1. **Validate Excel Files** → Ensure no typos in column names
2. **Test One Template First** → Start with a single table/template
3. **Version Control** → Keep templates and config in version control
4. **Template Organization** → Use descriptive template names matching business processes
5. **Metadata Review** → Check generated Excel metadata before running SQL
6. **Backup Output** → Archive generated SQL before regenerating

# ADDING NEW ENTRIES
## New table only:
- Add entry in the table_list.xlsx file as below:
    + DIST_STYLE: Define the distribution type of table in Redshift (EVEN, ALL, KEY)
    + TEMPLATE columns (TEMP_DDL, FINAL_DDL, etc): 1 to generate that template, 0 to ignore
- Add entry in the source_columns.xlsx file as below:
    + SOURCE_SYSTEM, SOURCE_SCHEMA, TABLE_NAME, DATA_SUBJECT must match with the corresponding fields in table_list.xlsx file
- Add entry in the mapping folder (if new mapping fields are required) file as below:
    + TARGET_DATA_LENGTH: -1 if source and target column have the same length, otherwise, specify the correct data length

## New template:
- Add new child attribute for the TEMPLATE attribute in the config.yml like other templates
- Create new jinja template accordingly
- Add new column for that template in the table_list.xlsx and fill in 0/1 accordingly