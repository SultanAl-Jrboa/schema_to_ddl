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
        df = pd.read_excel(excel_file_path, sheet_name="Metadata", skiprows=4)
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {str(e)}")
    
    schema_name = "dbo" if db_type == "SQL_SERVER" else "public"
    ddl_statements = []
    alter_statements = []
    
    for table in df["Table Name"].unique():
        table_df = df[df["Table Name"] == table]
        columns = []
        primary_keys = []
        foreign_keys = []
        
        for _, row in table_df.iterrows():
            column_name = f'"{row["Attribute Name"].strip()}"' if db_type != "SQL_SERVER" else f'[{row["Attribute Name"].strip()}]'
            data_type = map_data_type(db_type, row["Data Type and Length"].strip())
            column_def = f"{column_name} {data_type}"
            
            if str(row.get("Is it the Primary Key or part of the Primary Key?")).strip().upper() == "YES":
                primary_keys.append(column_name)
            
            if all(pd.notna(row.get(col)) for col in ["Schema", "Table", "Attribute"]):
                referenced_table = row["Table"]
                referenced_column = row["Attribute"]
                foreign_keys.append((column_name, referenced_table, referenced_column))
            
            columns.append(column_def)
        
        table_ddl = f'CREATE TABLE {schema_name}.[{table}] (\n    ' + ",\n    ".join(columns) + "\n);"
        ddl_statements.append(table_ddl)
        
        if primary_keys:
            alter_statements.append(f'ALTER TABLE {schema_name}.[{table}] ADD CONSTRAINT PK_{table} PRIMARY KEY ({", ".join(primary_keys)});')
        
        for column, ref_table, ref_column in foreign_keys:
            alter_statements.append(
                f'ALTER TABLE {schema_name}.[{table}] ADD CONSTRAINT FK_{table}_{ref_table} FOREIGN KEY ({column}) REFERENCES {schema_name}.[{ref_table}]({ref_column});'
            )
    
    return "\n\n".join(ddl_statements + alter_statements)

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        file = request.files['file']
        if file:
            file_path = os.path.join("uploads", file.filename)
            file.save(file_path)
            ddl_output = generate_ddl("SQL_SERVER", file_path)
            ddl_file_path = os.path.join("outputs", "ddl_output.sql")
            with open(ddl_file_path, "w") as f:
                f.write(ddl_output)
            return send_file(ddl_file_path, as_attachment=True)
    return render_template('index.html')

if __name__ == '__main__':
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)
    app.run(debug=True)
