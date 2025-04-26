import pandas as pd
import os
import argparse

def infer_postgres_type(dtype):
    """Map pandas dtype to PostgreSQL type."""
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        return 'TEXT'

def generate_create_table_sql(csv_path, table_name, schema='bronze'):
    """Generate a PostgreSQL CREATE TABLE statement from a CSV file."""
    
    df = pd.read_csv(csv_path, nrows=1000)  # Sample first 1000 rows
    columns = []

    for col in df.columns:
        pg_type = infer_postgres_type(df[col].dtype)
        safe_col = col.strip().replace(' ', '_').lower()
        columns.append(f'    {safe_col} {pg_type}')

    columns_sql = ",\n".join(columns)

    create_table_sql = (
f"""
CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
{columns_sql}
);
""")
    return create_table_sql

import os

def main(folder_paths, output_sql_file='create_tables.sql', schema='bronze'):
    """Main function to scan multiple folders and generate DDLs."""
    if isinstance(folder_paths, str):
        folder_paths = [folder_paths]  # Support single folder input too

    sql_statements = []

    for folder_path in folder_paths:
        if not os.path.isdir(folder_path):
            print(f"⚠️ Skipping invalid folder: {folder_path}")
            continue

        print(f"📂 Processing folder: {folder_path}")

        for filename in os.listdir(folder_path):
            file_comment_in_script = f"\n-- {folder_path} > {filename}"
            sql_statements.append(file_comment_in_script)
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                table_name = os.path.splitext(filename)[0].strip().replace(' ', '_').lower()

                ddl = generate_create_table_sql(file_path, table_name, schema=schema)
                sql_statements.append(ddl)

    if not sql_statements:
        print("⚠️ No CSV files found in the provided folders.")
        return

    with open(output_sql_file, 'w') as f:
        header = (
f"""/*
=====================================================
        Create tables in {schema} layer
=====================================================

This script creates tables in the {schema} layer
to load data into.

-----------------------------------------------------

Template generated using: 
utils/generate_ddl_script_template.py

=====================================================
*/\n\n""")
        f.write(header)
        f.writelines(sql_statements)

    print(f"✅ SQL script generated successfully: {output_sql_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate SQL DDL scripts from CSV files."
    )
    parser.add_argument(
        "folders",
        nargs="+",
        help="One or more folder paths containing CSV files."
    )
    parser.add_argument(
        "--output",
        default="ddl.sql",
        help="Output SQL file name. (default: %(default)s)"
    )
    parser.add_argument(
        "--schema",
        default="bronze",
        help="Target schema name for the tables. (default: %(default)s)"
    )

    args = parser.parse_args()

    main(
        folder_paths=args.folders,
        output_sql_file=args.output,
        schema=args.schema
    )