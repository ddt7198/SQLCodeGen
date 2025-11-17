
import yaml
import pandas as pd
import numpy as np
import jinja2
import os
import json

CURRENT_PATH = os.getcwd()
CONFIG_PATH = os.path.join(CURRENT_PATH, "src/config.yml")
with open(os.path.join(CONFIG_PATH), 'r') as ymlfile:
    cfg = yaml.full_load(ymlfile)

# Define variables
output_folder = cfg['OUTPUT_FOLDER']
table_list_df = pd.read_excel(CURRENT_PATH + cfg['TABLE_LIST'])
source_column = pd.read_excel(CURRENT_PATH + cfg['SOURCE_COLUMN'])
template_list = cfg['TEMPLATE'].keys()
code_path = CURRENT_PATH + output_folder['code']
metadata_path = CURRENT_PATH + output_folder['metadata']

templateLoader = jinja2.FileSystemLoader(searchpath=os.path.join(CURRENT_PATH, 'template'))
env = jinja2.Environment(loader=templateLoader)

class DataFrame():
    pass

def trim_upper_all_columns(df: DataFrame):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    return df.applymap(lambda x: x.strip().upper() if isinstance(x, str) else x)

def hash_generator(col_list: dict, load_type: str):
    """
        Generate hash_key or surrogate_key for required table based on load type
        Arguments: 
            col_list: dictionary of column names and other values related to that column
            load_type: load type of the table
        Returns:
            SQL query to generate hash_key or surrogate_key
    """
    hash_list = []
    sur_list = []
    if load_type != 'SCD2':
        for col in col_list:
            if col_list[col]['HASH_KEY'] == 'Y':
                hash_list.append('UPPER(TRIM(COALESCE([' + col_list[col]['COLUMN_NAME']+ "], '')))")
        col_str = "CAST(sha2(CONCAT(" + "\n\t\t\t,'||',".join(hash_list) + "), 256) as VARBINARY(32)) as hash_key,"
        return col_str
    else:
        for col in col_list:
            if col_list[col]['HASH_KEY'] == 'Y':
                hash_list.append('UPPER(TRIM(COALESCE([' + col_list[col]['COLUMN_NAME']+ "], '')))")
            if col_list[col]['SURROGATE_KEY'] == 'Y' and col_list[col]['COLUMN_NAME'] not in ['T24_LOAD_DATE', 'EFZ_LOAD_DATE']:
                sur_list.append('UPPER(TRIM(COALESCE([' + col_list[col]['COLUMN_NAME']+ "], '')))")
            if col_list[col]['HASH_KEY'] == 'Y' and col_list[col]['SURROGATE_KEY'] == 'Y':
                raise Exception(f"Column {col_list[col]['COLUMN_NAME']} cannot be used as HASH_KEY and SURROGATE_KEY at the same time. Please check")
        
        hash_str = "CAST(sha2(CONCAT(" + "\n\t\t\t,'||',".join(hash_list) + "), 256) as VARBINARY(32)) as hash_key,"
        sur_str = "CAST(sha2(CONCAT(" + "\n\t\t\t,'||',".join(sur_list) + "), 256) as VARBINARY(32)) as surrogate_key,"
        return hash_str + '\n\t\t\t' + sur_str

def data_transformation(column_name: str, data_type: str, data_length: str, nullable: str):
    """
        Generate data transformation queries
        Arguments:
            column_name: name of the column
            data_type: data type of that column
            data_length: data length of that column
            nullable: indicate if that column is null or not
        Returns:
            SQL queries for casting data
    """
    if data_type in ['DATE']:
        if nullable == 'Y' and column_name not in ['T24_LOAD_DATE', 'EFZ_LOAD_DATE']:
            return f"TO_DATE(ISNULL(NULLIF({column_name}, ''), '1900-01-01'), 'YYYYMMDD') AS {column_name}"
        elif nullable == 'N' and column_name not in ['T24_LOAD_DATE', 'EFZ_LOAD_DATE']:
            return f"TO_DATE({column_name}, 'YYYYMMDD') AS {column_name}"
        elif column_name in ['T24_LOAD_DATE', 'EFZ_LOAD_DATE']:
            return f"TO_DATE({column_name}, 'DD-Mon-YYYY') AS {column_name}"
    
    elif data_type in ['VARCHAR']:
        if nullable == 'Y':
            return f"CAST(NULLIF({column_name}, '') AS [{data_type}]({data_length})) AS {column_name}"
        else: 
            return f"CAST({column_name} AS [{data_type}]({data_length})) AS {column_name}"
    
    elif data_type in ['INT', 'BIGINT', 'SMALLINT']:
        if nullable == 'Y':
            return f"CAST(ISNULL(NULLIF({column_name}, ''), 0) AS [{data_type}]) AS {column_name}"
        else: 
            return f"CAST({column_name} AS [{data_type}]) AS {column_name}"

    elif data_type in ['DECIMAL']:
        if nullable == 'Y' and data_length != '<NA>':
            return f"CAST(ISNULL(NULLIF({column_name}, ''), 0.0) AS [{data_type}]({data_length})) AS {column_name}"
        elif nullable == 'Y' and data_length == '<NA>':
            return f"CAST(ISNULL(NULLIF({column_name}, ''), 0.0) AS [{data_type}]) AS {column_name}"
        elif nullable == 'N' and data_length != '<NA>': 
            return f"CAST({column_name} AS [{data_type}]({data_length})) AS {column_name}"
        else:
            return f"CAST({column_name} AS [{data_type}]) AS {column_name}"

def switch_dist_style(dist_style: str):
    """
        Switch distribution style of the table
        Arguments:
            dist_style: distribution type from consolidate file
        Returns:
            SQL queries for choosing distribution style
    """
    if dist_style.upper() == 'KEY':
        return """DISTSTYLE KEY\n\t\tDISTKEY(hash_key)\n\t\tSORTKEY(hash_key)"""
    else:
        return f"""DISTSTYLE {dist_style.upper()}"""


function_mapping = {
    'hash_generator': hash_generator,
    'data_transformation': data_transformation,
    'switch_dist_style': switch_dist_style
}

for function_name, function_object in function_mapping.items():
    env.globals[function_name] = function_object

def gen_json_ddl(field):
    """
        Create JSON from each row of the input DataFrame
        Arguments:
            field: row of the input DataFrame
        Returns:
            new column with each field as a dict object
    """
    return """
    {{"COLUMN_NAME": "{column_name}", "DATA_TYPE": "{column_data_type}", "DATA_LENGTH": "{column_data_length}", "NULLABLE": "{column_nullable}", "HASH_KEY": "{column_hash_key}", "SURROGATE_KEY": "{column_surrogate_key}" }}
    """.format(
        column_name=field['COLUMN_NAME'], 
        column_data_type=field['TARGET_DATA_TYPE'],
        column_data_length=field['TARGET_DATA_LENGTH'], 
        column_nullable=field['NULLABLE'],
        column_hash_key=field['HASH_KEY'],
        column_surrogate_key=field['SURROGATE_KEY']
    )

def adjust_target_length(df: DataFrame):
    """
        Change data length of target columns based on mapping condition
        Arguments:
            df: merged dataframe from all input excel files
        Returns:
            Mapped data length for column TARGET_DATA_LENGTH
    """
    # if df['TARGET_DATA_TYPE'] == 'DECIMAL':
    #     df['TARGET_DATA_LENGTH'] = f"{df['SOURCE_DECIMAL_PRECISION']}, {df['SOURCE_DECIMAL_SCALE']}"
    if int(df['TARGET_DATA_LENGTH']) == -1:
        df['TARGET_DATA_LENGTH'] = df['DATA_LENGTH']

    return df

def gen_metadata(source_column: DataFrame, table_list_df: DataFrame, mapping: DataFrame):
    """
        Generate metadata
        Arguments:
            source_column: list of columns of source table
            table_list_df: list of table names
            mapping: mapping for source2target
        Returns:
            metadata dataframe  
    """
    pre_metadata = pd.merge(source_column, table_list_df, on=['SOURCE_SYSTEM', 'SOURCE_SCHEMA', 'TABLE_NAME'], how='inner')
    metadata = pd.merge(pre_metadata, mapping, left_on=['DATA_TYPE'], right_on=['SOURCE_DATA_TYPE'], how='inner')
    metadata.drop(['No.', 'SOURCE_DATA_TYPE'], axis=1, inplace=True)
    metadata['DATA_LENGTH'] = metadata['DATA_LENGTH'].astype(pd.Int64Dtype())
    metadata['DATA_TYPE'] = metadata['DATA_TYPE'].apply(lambda x: x.strip().upper())
    metadata['TARGET_DATA_TYPE'] = metadata['TARGET_DATA_TYPE'].apply(lambda x: x.strip().upper())
    metadata = metadata.apply(adjust_target_length, axis=1)
    metadata['JSON'] = metadata.apply(lambda x: gen_json_ddl(x), axis=1)
    return metadata

def gen_json_mapping_ddl(input_df: DataFrame, template: str, table_name: str):
    """
        Generate mapped json from mapping dataframe
        Arguments:
            input_df: dataframe of table with at least one template value is 1 from table_list.xlsx
            template: name of required template
            table_name: name of required table
        Returns:
            mapped json file
            mapped excel file
    """
    info = input_df[input_df['TABLE_NAME'] == table_name]

    # Check required path
    table_metadata_path = os.path.join(metadata_path, table_name)
    table_code_path = os.path.join(code_path, table_name)
    table_template_path = os.path.join(code_path, table_name, template)

    check_path = [code_path, table_code_path, metadata_path, table_metadata_path, table_template_path]

    for path in check_path:
        if not os.path.exists(path):
            os.mkdir(path)

    ddl_json = {}
    
    # Generatea column mapping json
    ddl_lst = list(info['JSON'])
    col_dict = {}
    for i in range(len(ddl_lst)):
        col_dict[str(i)] = json.loads(ddl_lst[i])

    # Generate keys for json mapping
    keys_list = ['SOURCE_SCHEMA', 'TABLE_NAME', 'DATA_SUBJECT', 'SOURCE_SYSTEM', 'LOAD_TYPE', 'DIST_STYLE']
    for key in keys_list:
        ddl_json[key] = info[key].unique()[0]

    ddl_json['COLUMNS'] = col_dict

    # Write json data to file
    with open(os.path.join(table_template_path, table_name + '.json'), 'w') as f:
        json.dump(ddl_json, f, indent=3)
    
    # write metadata to excel file
    modified_info = info.drop(['JSON'], axis=1)
    modified_info.to_excel(os.path.join(table_metadata_path, template + '.xlsx'), index=False)

def gen_output(code_path: str, template: str, template_path: str, table_name: str):
    """
        Generate output files for the required table
        Arguments:
            code_path: path of code folder
            template: name of required template
            template_path: path of template file
            table_name: name of required table
    """
    # Get json mapping
    with open(os.path.join(code_path, table_name, template, table_name + '.json'), 'r') as f:
        ddl_json = json.load(f)

    # Get jinja template
    with open(template_path, 'r') as f:
        content_template = f.read()

    # Render jinja template using json mapping
    code_ddl = env.from_string(content_template).render(ddl_json)

    # Write output to file
    with open(os.path.join(code_path, table_name, template, table_name + '.sql'), 'w') as f:
        f.write(code_ddl)

def execute():
    for template in table_list_df.columns[6:]:
        if template in template_list:
            mapping = pd.read_excel(CURRENT_PATH + cfg['TEMPLATE'][template]['mapping_path'])
            metadata = gen_metadata(source_column, table_list_df, mapping)
            template_path = CURRENT_PATH + cfg['TEMPLATE'][template]['location']
            gen_df = metadata[metadata[template] == 1]
            if len(gen_df) > 0:
                table_list = list(gen_df['TABLE_NAME'].unique())
                for table in table_list:
                    print(f"==========START GENERATING TEMPLATE {template} OF TABLE {table}=============")
                    # Generate json for ddl
                    gen_json_mapping_ddl(gen_df, template, table)

                    # Generate code for ddl
                    gen_output(code_path, template, template_path, table)
                    print(f"==========TEMPLATE {template} OF TABLE {table} HAS BEEN CREATED=============")
                    print("*****************************************************************************")

if __name__== "__main__":
    execute()


