from flask import Flask, request, render_template, send_file
import pandas as pd
import os
import networkx as nx
import re

def map_data_type(db_type, data_type):
    """Maps generic data types to database-specific data types"""
    type_mappings = {
        "SQL_SERVER": {
            "varchar": "VARCHAR(255)", 
            "nvarchar": "NVARCHAR(255)", 
            "char": "CHAR(10)", 
            "text": "TEXT",
            "int": "INT", 
            "bigint": "BIGINT", 
            "smallint": "SMALLINT", 
            "tinyint": "TINYINT", 
            "bit": "BIT",
            "decimal": "DECIMAL(18,2)", 
            "numeric": "NUMERIC(18,2)", 
            "float": "FLOAT", 
            "real": "REAL",
            "datetime": "DATETIME2", 
            "date": "DATE", 
            "time": "TIME", 
            "timestamp": "DATETIME2",
            "binary": "BINARY", 
            "varbinary": "VARBINARY(MAX)",
            "boolean": "BIT",
            "money": "MONEY"
        },
        "POSTGRESQL": {
            "varchar": "VARCHAR(255)", 
            "nvarchar": "VARCHAR(255)", 
            "char": "CHAR(10)", 
            "text": "TEXT",
            "int": "INTEGER", 
            "bigint": "BIGINT", 
            "smallint": "SMALLINT", 
            "tinyint": "SMALLINT", 
            "bit": "BIT(1)",
            "decimal": "NUMERIC(18,2)", 
            "numeric": "NUMERIC(18,2)", 
            "float": "REAL", 
            "real": "REAL",
            "datetime": "TIMESTAMP", 
            "date": "DATE", 
            "time": "TIME", 
            "timestamp": "TIMESTAMP",
            "binary": "BYTEA", 
            "varbinary": "BYTEA",
            "boolean": "BOOLEAN",
            "money": "MONEY"
        },
        "ORACLE": {
            "varchar": "VARCHAR2(255)", 
            "nvarchar": "NVARCHAR2(255)", 
            "char": "CHAR(10)", 
            "text": "CLOB",
            "int": "NUMBER(10)", 
            "bigint": "NUMBER(19)", 
            "smallint": "NUMBER(5)", 
            "tinyint": "NUMBER(3)", 
            "bit": "NUMBER(1)",
            "decimal": "NUMBER(18,2)", 
            "numeric": "NUMBER(18,2)", 
            "float": "FLOAT", 
            "real": "FLOAT",
            "datetime": "TIMESTAMP", 
            "date": "DATE", 
            "time": "DATE", 
            "timestamp": "TIMESTAMP",
            "binary": "BLOB", 
            "varbinary": "BLOB",
            "boolean": "NUMBER(1)",
            "money": "NUMBER(19,4)"
        },
        "MYSQL": {
            "varchar": "VARCHAR(255)", 
            "nvarchar": "VARCHAR(255) CHARACTER SET utf8mb4", 
            "char": "CHAR(10)", 
            "text": "TEXT",
            "int": "INT", 
            "bigint": "BIGINT", 
            "smallint": "SMALLINT", 
            "tinyint": "TINYINT", 
            "bit": "BIT(1)",
            "decimal": "DECIMAL(18,2)", 
            "numeric": "DECIMAL(18,2)", 
            "float": "FLOAT", 
            "real": "FLOAT",
            "datetime": "DATETIME", 
            "date": "DATE", 
            "time": "TIME", 
            "timestamp": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "binary": "BINARY", 
            "varbinary": "VARBINARY(255)",
            "boolean": "TINYINT(1)",
            "money": "DECIMAL(19,4)"
        }
    }
    
    if data_type is None:
        return "VARCHAR(255)"
        
    data_type = str(data_type).strip().upper()
    return type_mappings.get(db_type, {}).get(data_type, data_type)

def get_identifier_quote(db_type):
    """Returns the appropriate identifier quote character(s) for each database type"""
    if db_type == "MYSQL":
        return "`"
    elif db_type == "POSTGRESQL" or db_type == "ORACLE":
        return "\""
    elif db_type == "SQL_SERVER":
        return "["
    else:
        return "\""

def get_identifier_quote_close(db_type):
    """Returns the appropriate closing identifier quote character(s)"""
    if db_type == "SQL_SERVER":
        return "]"
    else:
        return get_identifier_quote(db_type)

def format_identifier(name, db_type):
    """Format identifier with the correct quoting style"""
    open_quote = get_identifier_quote(db_type)
    close_quote = get_identifier_quote_close(db_type)
    return f"{open_quote}{name}{close_quote}"

def normalize_name(name):
    """Normalize table and column names"""
    if name:
        return str(name).strip()
    return name

def generate_ddl(db_type, excel_file_path):
    # Read Excel file
    try:
        df = pd.read_excel(excel_file_path, sheet_name="Dataset Overview")
        if df.shape[0] > 12 and df.shape[1] > 1:
            specified_db_type = str(df.iloc[12, 1]).strip().upper()
            if specified_db_type in ["MYSQL", "ORACLE", "SQL_SERVER", "POSTGRESQL"]:
                db_type = specified_db_type
        
        df_metadata = pd.read_excel(excel_file_path, sheet_name="Metadata", skiprows=4)
        required_columns = ["Table Name", "Attribute Name"]
        for col in required_columns:
            if col not in df_metadata.columns:
                raise Exception(f"Required column '{col}' is missing")
        
        df_metadata = df_metadata.dropna(subset=["Table Name", "Attribute Name"])
        schema_name = "NIC_DWH_STG"
    except Exception as e:
        raise Exception(f"Error reading Excel file: {str(e)}")
    
    # Schema creation statements
    ddl_statements = [f"-- DDL for database: {db_type}\n"]
    
    if db_type == "ORACLE":
        ddl_statements.append(f"-- CREATE USER {schema_name} IDENTIFIED BY password;\n-- GRANT CONNECT, RESOURCE TO {schema_name};\n")
    elif db_type == "POSTGRESQL":
        ddl_statements.append(f"-- CREATE SCHEMA {schema_name};\n")
    elif db_type == "SQL_SERVER":
        ddl_statements.append(f"-- CREATE SCHEMA {schema_name};\n")
    elif db_type == "MYSQL":
        ddl_statements.append(f"-- CREATE DATABASE IF NOT EXISTS {schema_name};\n-- USE {schema_name};\n")
    
    # Track objects
    table_info = {}
    foreign_keys = []
    all_table_names = set()
    schemas_to_create = set()

    # Process metadata
    for index, row in df_metadata.iterrows():
        if pd.isna(row.get("Table Name")) or pd.isna(row.get("Attribute Name")):
            continue
            
        table_name = normalize_name(row["Table Name"])
        column_name = normalize_name(row["Attribute Name"])
        
        # Skip CDC columns for SQL Server
        if db_type == "SQL_SERVER":
            cdc_columns = ["CDC_TS", "CDC_operation", "CDC_start_lsn", "CDC_end_lsn", 
                          "CDC_seqval", "CDC_update_mask", "CDC_command_id"]
            if any(cdc_col.lower() in column_name.lower() for cdc_col in cdc_columns):
                continue
        
        # Handle SQL Server schemas
        if db_type == "SQL_SERVER" and '.' in table_name:
            schema_part = table_name.split('.')[0]
            if schema_part.lower() != "dbo":
                schemas_to_create.add(schema_part)
        
        all_table_names.add(table_name)
        
        # Data type handling
        default_type = "VARCHAR(255)" if db_type != "ORACLE" else "VARCHAR2(255)"
        data_type = default_type  
        if "Data Type and Length" in row and pd.notna(row["Data Type and Length"]):
            data_type = str(row["Data Type and Length"]).strip()
        
        mapped_data_type = map_data_type(db_type, data_type)
        quoted_column_name = format_identifier(column_name, db_type)
        
        # Check constraints
        is_primary_key = False
        if "Is it the Primary Key or part of the Primary Key?" in row and pd.notna(row["Is it the Primary Key or part of the Primary Key?"]):
            is_primary_key = str(row["Is it the Primary Key or part of the Primary Key?"]).strip().upper() == "YES"
        
        is_last_operation = False
        if "Is it the LastOperation attribute?" in row and pd.notna(row["Is it the LastOperation attribute?"]):
            is_last_operation = str(row["Is it the LastOperation attribute?"]).strip().upper() == "YES"
        
        is_timestamp = False
        if "Is it the Timestamp attribute?" in row and pd.notna(row["Is it the Timestamp attribute?"]):
            is_timestamp = str(row["Is it the Timestamp attribute?"]).strip().upper() == "YES"
        
        # Store table info
        if table_name not in table_info:
            table_info[table_name] = {'columns': [], 'primary_keys': []}
        
        column_info = {
            'name': column_name,
            'quoted_name': quoted_column_name,
            'data_type': mapped_data_type,
            'is_primary_key': is_primary_key,
            'is_last_operation': is_last_operation,
            'is_timestamp': is_timestamp,
            'is_foreign_key': False,
            'not_null': is_primary_key
        }
        
        if is_primary_key:
            table_info[table_name]['primary_keys'].append(quoted_column_name)
        
        table_info[table_name]['columns'].append(column_info)
        
        # Process foreign keys
        ref_table_idx = None
        ref_attr_idx = None
        
        for col_idx, col_name in enumerate(df_metadata.columns):
            col_name_str = str(col_name).lower()
            if "reference" in col_name_str and "table" in col_name_str:
                ref_table_idx = col_idx
            if "reference" in col_name_str and "attribute" in col_name_str:
                ref_attr_idx = col_idx
        
        if ref_table_idx is None and len(row) > 11:
            if pd.notna(row.iloc[11]):
                ref_table_idx = 11
        
        if ref_attr_idx is None and len(row) > 12:
            if pd.notna(row.iloc[12]):
                ref_attr_idx = 12
        
        if ref_table_idx is not None and ref_attr_idx is not None:
            if ref_table_idx < len(row) and ref_attr_idx < len(row):
                if pd.notna(row.iloc[ref_table_idx]) and pd.notna(row.iloc[ref_attr_idx]):
                    ref_table = normalize_name(row.iloc[ref_table_idx])
                    ref_column = normalize_name(row.iloc[ref_attr_idx])
                    
                    if ref_table and ref_column:
                        if '|' in ref_table and '|' in ref_column:
                            ref_tables = ref_table.split('|')
                            ref_columns = ref_column.split('|')
                            
                            for i in range(min(len(ref_tables), len(ref_columns))):
                                rt = normalize_name(ref_tables[i])
                                rc = normalize_name(ref_columns[i])
                                
                                if rt and rc and rt.lower() != "nan" and rc.lower() != "nan":
                                    column_info['is_foreign_key'] = True
                                    column_info['not_null'] = True
                                    all_table_names.add(rt)
                                    foreign_keys.append({
                                        'source_table': table_name,
                                        'source_column': column_name,
                                        'target_table': rt,
                                        'target_column': rc
                                    })
                        else:
                            if ref_table.lower() != "nan" and ref_column.lower() != "nan":
                                column_info['is_foreign_key'] = True
                                column_info['not_null'] = True
                                all_table_names.add(ref_table)
                                foreign_keys.append({
                                    'source_table': table_name,
                                    'source_column': column_name,
                                    'target_table': ref_table,
                                    'target_column': ref_column
                                })
    
    # Add schema creation for SQL Server
    if db_type == "SQL_SERVER" and schemas_to_create:
        schema_ddls = []
        for schema in sorted(schemas_to_create):
            if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', schema):
                schema_ddls.append(
                    f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')\n"
                    f"BEGIN\n"
                    f"    EXEC('CREATE SCHEMA {schema}')\n"
                    f"END"
                )
        if schema_ddls:
            ddl_statements.append("\n-- Create non-dbo schemas\n" + "\n\n".join(schema_ddls) + "\n")

    # Generate CREATE TABLE statements
    table_ddl_statements = []
    
    for table_name, info in table_info.items():
        column_defs = []
        
        for col in info['columns']:
            col_def = f"{col['quoted_name']} {col['data_type']}"
            
            if col['not_null']:
                col_def += " NOT NULL"
                
            if col['is_last_operation']:
                if db_type == "MYSQL":
                    col_def = f"{col['quoted_name']} ENUM('INSERT', 'UPDATE', 'DELETE')"
                else:
                    check_column = col['quoted_name']
                    col_def += f" CHECK ({check_column} IN ('INSERT', 'UPDATE', 'DELETE'))"
                
            if col['is_timestamp']:
                if db_type in ["MYSQL", "POSTGRESQL", "ORACLE", "SQL_SERVER"]:
                    col_def += " DEFAULT CURRENT_TIMESTAMP"
                
            column_defs.append(col_def)
        
        # Handle table naming with schemas
        if db_type == "SQL_SERVER":
            if '.' in table_name:
                schema_part, table_part = table_name.split('.', 1)
                full_table_name = f"{schema_part}.{table_part}"
            else:
                full_table_name = f"dbo.{table_name}"
            quoted_table_name = format_identifier(table_part if '.' in table_name else table_name, db_type)
            table_ddl = f'CREATE TABLE {full_table_name} (\n    ' + ",\n    ".join(column_defs) + "\n);"
        else:
            quoted_table_name = format_identifier(table_name, db_type)
            quoted_schema = format_identifier(schema_name, db_type) if db_type == "POSTGRESQL" else schema_name
            table_ddl = f'CREATE TABLE {quoted_schema}.{quoted_table_name} (\n    ' + ",\n    ".join(column_defs) + "\n);"
            
        table_ddl_statements.append(table_ddl)
    
    # Generate PRIMARY KEY constraints
    pk_statements = []
    for table_name, info in table_info.items():
        if info['primary_keys']:
            pk_name = f"PK_{table_name[:20]}"  # Shortened name
            pk_columns = ", ".join(info["primary_keys"])
            
            if db_type == "SQL_SERVER":
                if '.' in table_name:
                    full_table_name = table_name
                else:
                    full_table_name = f"dbo.{table_name}"
                quoted_table_name = format_identifier(table_name.split('.')[-1], db_type)
                pk_stmt = f'ALTER TABLE {full_table_name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_columns});'
            else:
                quoted_table_name = format_identifier(table_name, db_type)
                quoted_schema = format_identifier(schema_name, db_type) if db_type == "POSTGRESQL" else schema_name
                pk_stmt = f'ALTER TABLE {quoted_schema}.{quoted_table_name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_columns});'
            
            pk_statements.append(pk_stmt)
    
    # Generate FOREIGN KEY constraints with short names
    fk_statements = []
    fk_counter = 1
    
    for fk in foreign_keys:
        source_table = fk['source_table']
        source_column = fk['source_column']
        target_table = fk['target_table']
        target_column = fk['target_column']
        
        # Skip if target doesn't exist
        if target_table not in table_info:
            continue
            
        # Skip if target column doesn't exist
        target_columns = [col['name'] for col in table_info[target_table]['columns']]
        if target_column not in target_columns:
            continue
        
        quoted_source_column = format_identifier(source_column, db_type)
        quoted_target_column = format_identifier(target_column, db_type)
        
        # Very short FK names
        fk_name = f"FK_{fk_counter}"
        fk_counter += 1
        
        if db_type == "SQL_SERVER":
            # Format source table
            if '.' in source_table:
                source_table_full = source_table
            else:
                source_table_full = f"dbo.{source_table}"
            
            # Format target table
            if '.' in target_table:
                target_table_full = target_table
            else:
                target_table_full = f"dbo.{target_table}"
            
            quoted_source_table = format_identifier(source_table.split('.')[-1], db_type)
            quoted_target_table = format_identifier(target_table.split('.')[-1], db_type)
            
            fk_stmt = f'ALTER TABLE {source_table_full} ADD CONSTRAINT {fk_name} FOREIGN KEY ({quoted_source_column}) REFERENCES {target_table_full}({quoted_target_column});'
        else:
            quoted_source_table = format_identifier(source_table, db_type)
            quoted_target_table = format_identifier(target_table, db_type)
            quoted_schema = format_identifier(schema_name, db_type) if db_type == "POSTGRESQL" else schema_name
            fk_stmt = f'ALTER TABLE {quoted_schema}.{quoted_source_table} ADD CONSTRAINT {fk_name} FOREIGN KEY ({quoted_source_column}) REFERENCES {quoted_schema}.{quoted_target_table}({quoted_target_column});'
        
        fk_statements.append(fk_stmt)
    
    # Combine all statements
    all_statements = (
        ddl_statements + 
        ["\n-- Create tables"] +
        table_ddl_statements + 
        ["\n-- Add primary key constraints"] +
        pk_statements +
        ["\n-- Add foreign key constraints"] +
        fk_statements
    )
    
    return "\n\n".join(all_statements)


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
                db_type = request.form.get('db_type', 'POSTGRESQL')
                ddl_output = generate_ddl(db_type, file_path)
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