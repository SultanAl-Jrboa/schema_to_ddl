from flask import Flask, request, render_template, send_file
import pandas as pd
import os

def map_data_type(db_type, data_type):
    type_mappings = {
        "SQL_SERVER": {"varchar": "VARCHAR(255)", "nvarchar": "NVARCHAR(255)", "char": "CHAR(10)", "text": "TEXT",
                        "int": "INT", "bigint": "BIGINT", "smallint": "SMALLINT", "tinyint": "TINYINT", "bit": "BIT",
                        "decimal": "DECIMAL(18,2)", "numeric": "NUMERIC(18,2)", "float": "FLOAT", "real": "REAL",
                        "datetime": "DATETIME", "date": "DATE", "time": "TIME", "timestamp": "TIMESTAMP",
                        "binary": "BINARY", "varbinary": "VARBINARY(MAX)"},
        "POSTGRESQL": {"BOOLEAN": "BOOLEAN", "TEXT": "TEXT", "FLOAT": "REAL", "INT": "INTEGER", "BIGINT": "BIGINT",
                        "VARCHAR(255)": "VARCHAR(255)", "VARCHAR(100)": "VARCHAR(100)", "DATE": "DATE", "DATETIME": "TIMESTAMP",
                        "DECIMAL": "NUMERIC", "MONEY": "NUMERIC(19,4)", "CHAR": "CHAR"},
        "ORACLE": {"BOOLEAN": "NUMBER(1)", "TEXT": "CLOB", "FLOAT": "NUMBER", "INT": "NUMBER(10)", "BIGINT": "NUMBER(19)",
                    "VARCHAR(255)": "VARCHAR2(255)", "VARCHAR(100)": "VARCHAR2(100)", "DATE": "DATE", "DATETIME": "TIMESTAMP",
                    "DECIMAL": "NUMBER", "MONEY": "NUMBER(19,4)", "CHAR": "CHAR"},
        "MYSQL": {"BOOLEAN": "TINYINT(1)", "TEXT": "TEXT", "FLOAT": "FLOAT", "INT": "INT", "BIGINT": "BIGINT",
                   "VARCHAR(255)": "VARCHAR(255)", "VARCHAR(100)": "VARCHAR(100)", "DATE": "DATE", "DATETIME": "DATETIME",
                   "DECIMAL": "DECIMAL", "MONEY": "DECIMAL(19,4)", "CHAR": "CHAR"}
    }
    return type_mappings.get(db_type, {}).get(data_type.upper(), data_type)

def generate_ddl(db_type, excel_file_path):
    df = pd.read_excel(excel_file_path, sheet_name="Dataset Overview")
    db_type = str(df.iloc[12, 1]).strip().upper()
    df_metadata = pd.read_excel(excel_file_path, sheet_name="Metadata", skiprows=4).dropna(subset=["Table Name", "Attribute Name"])
    schema_name = "NIC_DWH_STG"

    ddl_statements = [f"-- DDL for database: {db_type}\n"]
    primary_key_statements = []
    foreign_key_statements = []
    
    for table in df_metadata["Table Name"].unique():
        table_df = df_metadata[df_metadata["Table Name"] == table]
        columns = []
        primary_keys = []
        
        for _, row in table_df.iterrows():
            column_name = row["Attribute Name"].strip()
            data_type = map_data_type(db_type, row["Data Type and Length"].strip())
            
            # Format column name for the database
            quoted_column_name = f'"{column_name}"' if db_type in ["POSTGRESQL", "ORACLE"] else column_name
            
            # Check if this column is a primary key
            is_primary_key = str(row.get("Is it the Primary Key or part of the Primary Key?", "")).strip().upper() == "YES"
            
            # Add NOT NULL constraint to primary key columns
            if is_primary_key:
                column_def = f"{quoted_column_name} {data_type} NOT NULL"
                primary_keys.append(quoted_column_name)
            else:
                column_def = f"{quoted_column_name} {data_type}"
            
            if str(row.get("Is it the LastOperation attribute?", "")).strip().upper() == "YES":
                column_def += f" CHECK ({quoted_column_name} IN ('INSERT', 'UPDATE', 'DELETE'))"
            
            if str(row.get("Is it the SyncTimestamp attribute?", "")).strip().upper() == "YES":
                column_def += " DEFAULT CURRENT_TIMESTAMP"
            
            columns.append(column_def)

            # The foreign key information is at columns 10, 11, 12 (index 10, 11, 12)
            # Check for foreign key relationships in these columns
            ref_schema = row.get(10)  # Index 10 - Schema
            ref_table = row.get(11)   # Index 11 - Table
            ref_attr = row.get(12)    # Index 12 - Attribute
            
            # Handle multiple references (separated by | character)
            if ref_table and ref_attr:
                # Check if there are multiple references (pipe-separated)
                if '|' in str(ref_table) and '|' in str(ref_attr):
                    ref_tables = str(ref_table).split('|')
                    ref_attrs = str(ref_attr).split('|')
                    
                    for i in range(min(len(ref_tables), len(ref_attrs))):
                        rt = ref_tables[i].strip()
                        ra = ref_attrs[i].strip()
                        if rt and ra:
                            fk_name = f"FK_{table}_{column_name}_{i+1}"
                            foreign_key_statements.append(
                                f'ALTER TABLE {schema_name}."{table}" ADD CONSTRAINT {fk_name} ' +
                                f'FOREIGN KEY ({quoted_column_name}) REFERENCES {schema_name}."{rt}"("{ra}");'
                            )
                            print(f"Found foreign key: {table}.{column_name} -> {rt}.{ra}")
                else:
                    # Single reference
                    rt = str(ref_table).strip()
                    ra = str(ref_attr).strip()
                    if rt and ra:
                        fk_name = f"FK_{table}_{column_name}"
                        foreign_key_statements.append(
                            f'ALTER TABLE {schema_name}."{table}" ADD CONSTRAINT {fk_name} ' +
                            f'FOREIGN KEY ({quoted_column_name}) REFERENCES {schema_name}."{rt}"("{ra}");'
                        )
                        print(f"Found foreign key: {table}.{column_name} -> {rt}.{ra}")
        
        table_ddl = f'CREATE TABLE {schema_name}."{table}" (\n    ' + ",\n    ".join(columns) + "\n);"
        ddl_statements.append(table_ddl)
        
        if primary_keys:
            pk_name = f"PK_{table}"
            primary_key_statements.append(f'ALTER TABLE {schema_name}."{table}" ADD CONSTRAINT {pk_name} PRIMARY KEY ({", ".join(primary_keys)});')
    
    # Print for debugging
    print(f"Number of foreign key constraints: {len(foreign_key_statements)}")
    
    return "\n\n".join(ddl_statements + primary_key_statements + foreign_key_statements)


app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)
    
    if request.method == 'POST':
        file = request.files.get('file')
        if file:
            file_path = os.path.join("uploads", file.filename)
            file.save(file_path)
            try:
                ddl_output = generate_ddl("POSTGRESQL", file_path)
                ddl_file_path = os.path.join("outputs", "ddl_output.sql")
                with open(ddl_file_path, "w") as f:
                    f.write(ddl_output)
                return send_file(ddl_file_path, as_attachment=True)
            except Exception as e:
                return f"Error: {str(e)}"
    
    return render_template('index.html')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)