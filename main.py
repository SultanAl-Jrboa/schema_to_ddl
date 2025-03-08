from flask import Flask, request, render_template, send_file
import pandas as pd
import os

def map_data_type(db_type, data_type):
    type_mappings = {
        "SQL_SERVER": {
            "varchar": "VARCHAR(255)", "nvarchar": "NVARCHAR(255)", "char": "CHAR(10)", "text": "TEXT",
            "int": "INT", "bigint": "BIGINT", "smallint": "SMALLINT", "tinyint": "TINYINT", "bit": "BIT",
            "decimal": "DECIMAL(18,2)", "numeric": "NUMERIC(18,2)", "float": "FLOAT", "real": "REAL",
            "datetime": "DATETIME", "date": "DATE", "time": "TIME", "timestamp": "TIMESTAMP",
            "binary": "BINARY", "varbinary": "VARBINARY(MAX)"
        },
        "POSTGRESQL": {
            "BOOLEAN": "BOOLEAN", "TEXT": "TEXT", "FLOAT": "REAL", "INT": "INTEGER", "BIGINT": "BIGINT",
            "VARCHAR(255)": "VARCHAR(255)", "VARCHAR(100)": "VARCHAR(100)", "DATE": "DATE", "DATETIME": "TIMESTAMP",
            "DECIMAL": "NUMERIC", "MONEY": "NUMERIC(19,4)", "CHAR": "CHAR"
        },
        "ORACLE": {
            "BOOLEAN": "NUMBER(1)", "TEXT": "CLOB", "FLOAT": "NUMBER", "INT": "NUMBER(10)", "BIGINT": "NUMBER(19)",
            "VARCHAR(255)": "VARCHAR2(255)", "VARCHAR(100)": "VARCHAR2(100)", "DATE": "DATE", "DATETIME": "TIMESTAMP",
            "DECIMAL": "NUMBER", "MONEY": "NUMBER(19,4)", "CHAR": "CHAR"
        },
        "MYSQL": {
            "BOOLEAN": "TINYINT(1)", "TEXT": "TEXT", "FLOAT": "FLOAT", "INT": "INT", "BIGINT": "BIGINT",
            "VARCHAR(255)": "VARCHAR(255)", "VARCHAR(100)": "VARCHAR(100)", "DATE": "DATE", "DATETIME": "DATETIME",
            "DECIMAL": "DECIMAL", "MONEY": "DECIMAL(19,4)", "CHAR": "CHAR"
        }
    }
    return type_mappings[db_type].get(data_type.upper(), data_type)

def generate_ddl(db_type, excel_file_path):
    if not os.path.exists(excel_file_path):
        raise FileNotFoundError(f"Excel file not found: {excel_file_path}")
    
    try:
        df = pd.read_excel(excel_file_path, sheet_name="Dataset Overview")
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {str(e)}")
    
    db_type = str(df.iloc[12, 1]).strip().upper()
    if db_type not in ['POSTGRESQL', 'MYSQL', 'SQL_SERVER', 'ORACLE']:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    df_metadata = pd.read_excel(excel_file_path, sheet_name="Metadata", skiprows=4)
    df_metadata = df_metadata.dropna(subset=["Table Name", "Attribute Name"])
    
    schema_name = "NIC_DWH_STG"
    
    drop_schema = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;\nCREATE SCHEMA {schema_name};" if db_type == "POSTGRESQL" else \
                  f"DROP DATABASE IF EXISTS {schema_name};\nCREATE DATABASE {schema_name};" if db_type == "MYSQL" else \
                  f"BEGIN EXECUTE IMMEDIATE 'DROP USER \"{schema_name}\" CASCADE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1918 THEN RAISE; END IF; END; /\nCREATE USER \"{schema_name}\" IDENTIFIED BY \"123456\";\nGRANT CONNECT, RESOURCE TO \"{schema_name}\";" if db_type == "ORACLE" else \
                  f"IF EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}') BEGIN EXEC sp_executesql N'DROP SCHEMA {schema_name}'; END GO\nCREATE SCHEMA {schema_name};"
    
    ddl_statements = []
    ddl_statements.append(f"-- DDL for database: {db_type}\n")

    for table in df_metadata["Table Name"].unique():
        table_df = df_metadata[df_metadata["Table Name"] == table]
        columns = []
        primary_keys = []
        foreign_keys = []
        
        for _, row in table_df.iterrows():
            column_name = f'"{row["Attribute Name"].strip()}"' if db_type in ["POSTGRESQL", "ORACLE"] else f'{row["Attribute Name"].strip()}'
            data_type = map_data_type(db_type, row["Data Type and Length"].strip())
            column_def = f"{column_name} {data_type}"
            
            if str(row.get("Is it the Primary Key or part of the Primary Key?")).strip().upper() == "YES":
                primary_keys.append(column_name)
            
            if str(row.get("Is it the LastOperation attribute?")).strip().upper() == "YES":
                column_def += " CHECK (" + column_name + " IN ('INSERT', 'UPDATE', 'DELETE'))"
            
            if str(row.get("Is it the SyncTimestamp attribute?")).strip().upper() == "YES":
                column_def += " DEFAULT CURRENT_TIMESTAMP"
            
            if not pd.isna(row.get("Schema")) and not pd.isna(row.get("Table")) and not pd.isna(row.get("Attribute")):
                foreign_key = f"FOREIGN KEY ({column_name}) REFERENCES {row['Schema']}.{row['Table']}({row['Attribute']})"
                foreign_keys.append(foreign_key)
            
            columns.append(column_def)
        
        table_ddl = f'CREATE TABLE {schema_name}."{table}" (\n    ' + ",\n    ".join(columns)
        
        if primary_keys:
            table_ddl += f',\n    PRIMARY KEY ({", ".join(primary_keys)})'
        
        if foreign_keys:
            table_ddl += f',\n    ' + ",\n    ".join(foreign_keys)
        
        table_ddl += "\n);"
        ddl_statements.append(table_ddl)
    
    return "\n\n".join(ddl_statements), drop_schema

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        file = request.files['file']
        if file:
            file_path = os.path.join("uploads", file.filename)
            file.save(file_path)
            ddl_output, drop_schema_command = generate_ddl("POSTGRESQL", file_path)
            ddl_file_path = os.path.join("outputs", "ddl_output.sql")
            with open(ddl_file_path, "w") as f:
                f.write(ddl_output)
            return send_file(ddl_file_path, as_attachment=True)
    return render_template('index.html')

if __name__ == '__main__':
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)
    app.run(debug=True)
    port = int(os.environ.get("PORT", 4000))  
    app.run(host="0.0.0.0", port=port, debug=True)
