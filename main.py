from flask import Flask, request, render_template, send_file
import pandas as pd
import os
import networkx as nx

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
    return type_mappings.get(db_type, {}).get(data_type.upper() if data_type else "", data_type)

def generate_ddl(db_type, excel_file_path):
    # Read Excel file
    try:
        df = pd.read_excel(excel_file_path, sheet_name="Dataset Overview")
        db_type = str(df.iloc[12, 1]).strip().upper() if df.shape[0] > 12 and df.shape[1] > 1 else db_type
        
        # Print column names to debug
        df_metadata = pd.read_excel(excel_file_path, sheet_name="Metadata", skiprows=4)
        print("Available columns in Metadata sheet:", df_metadata.columns.tolist())
        
        # Make sure required columns exist
        required_columns = ["Table Name", "Attribute Name", "Data Type and Length"]
        for col in required_columns:
            if col not in df_metadata.columns:
                raise Exception(f"Required column '{col}' is missing from Excel file")
        
        df_metadata = df_metadata.dropna(subset=["Table Name", "Attribute Name"])
        schema_name = "NIC_DWH_STG"
    except Exception as e:
        raise Exception(f"Error reading Excel file: {str(e)}")
    
    # Add schema creation statement
    ddl_statements = [f"-- DDL for database: {db_type}\n"]
    
    if db_type == "ORACLE":
        ddl_statements.append(f"-- Create schema if it doesn't exist\n-- Uncomment this if the schema doesn't already exist in your database\n-- CREATE USER {schema_name} IDENTIFIED BY password;\n-- GRANT CONNECT, RESOURCE TO {schema_name};\n")
    elif db_type == "POSTGRESQL":
        ddl_statements.append(f"-- Create schema if it doesn't exist\n-- Uncomment this if the schema doesn't already exist in your database\n-- CREATE SCHEMA {schema_name};\n")
    elif db_type == "SQL_SERVER":
        ddl_statements.append(f"-- Create schema if it doesn't exist\n-- Uncomment this if the schema doesn't already exist in your database\n-- CREATE SCHEMA {schema_name};\n")
    elif db_type == "MYSQL":
        ddl_statements.append(f"-- Create schema if it doesn't exist\n-- Uncomment this if the schema doesn't already exist in your database\n-- CREATE DATABASE IF NOT EXISTS {schema_name};\n-- USE {schema_name};\n")
    
    # Create dictionaries to store table information
    table_columns = {}
    primary_keys = {}
    foreign_keys = []
    
    # First pass: Collect table and column information
    for index, row in df_metadata.iterrows():
        # Skip rows with missing required data
        if pd.isna(row.get("Table Name")) or pd.isna(row.get("Attribute Name")):
            continue
            
        table_name = str(row["Table Name"]).strip()
        column_name = str(row["Attribute Name"]).strip()
        
        # Get data type with safe handling
        data_type = "VARCHAR2(255)"  # Default type
        if "Data Type and Length" in row and pd.notna(row["Data Type and Length"]):
            data_type = str(row["Data Type and Length"]).strip()
        
        # Map data type according to target database
        mapped_data_type = map_data_type(db_type, data_type)
        
        # Format column name for the database
        quoted_column_name = f'"{column_name}"' if db_type in ["POSTGRESQL", "ORACLE"] else column_name
        
        # Check if this column is a primary key
        is_primary_key = False
        if "Is it the Primary Key or part of the Primary Key?" in row and pd.notna(row["Is it the Primary Key or part of the Primary Key?"]):
            is_primary_key = str(row["Is it the Primary Key or part of the Primary Key?"]).strip().upper() == "YES"
        
        # Check if this column is LastOperation
        is_last_operation = False
        if "Is it the LastOperation attribute?" in row and pd.notna(row["Is it the LastOperation attribute?"]):
            is_last_operation = str(row["Is it the LastOperation attribute?"]).strip().upper() == "YES"
        
        # Check if this column is Timestamp
        is_timestamp = False
        if "Is it the SyncTimestamp attribute?" in row and pd.notna(row["Is it the SyncTimestamp attribute?"]):
            is_timestamp = str(row["Is it the SyncTimestamp attribute?"]).strip().upper() == "YES"
        
        # Prepare column definition
        column_def = quoted_column_name + " " + mapped_data_type
        
        # Add constraints
        if is_primary_key:
            column_def += " NOT NULL"
            if table_name not in primary_keys:
                primary_keys[table_name] = []
            primary_keys[table_name].append(quoted_column_name)
        
        if is_last_operation:
            column_def += f" CHECK ({quoted_column_name} IN ('INSERT', 'UPDATE', 'DELETE'))"
        
        if is_timestamp:
            column_def += " DEFAULT CURRENT_TIMESTAMP"
        
        # Add column to table
        if table_name not in table_columns:
            table_columns[table_name] = []
        table_columns[table_name].append(column_def)
        
        # Check for foreign key relationships
        # Look for indices 10, 11, 12 but handle the case where they might not exist
        ref_table = None
        ref_column = None
        
        # Find which columns contain reference information (look for Schema, Table, Attribute headers)
        ref_schema_idx = None
        ref_table_idx = None
        ref_attr_idx = None
        
        # First, try to find by header text
        for col_idx, col_name in enumerate(df_metadata.columns):
            if "Referenced Table" in str(col_name) or "Table" in str(col_name) and "Fill" in str(df_metadata.iloc[0, col_idx]):
                ref_table_idx = col_idx
            if "Referenced Attribute" in str(col_name) or "Attribute" in str(col_name) and "Fill" in str(df_metadata.iloc[0, col_idx]):
                ref_attr_idx = col_idx
                
        # If not found, try fixed positions 10-12
        if ref_table_idx is None and len(row) > 11:
            ref_table_idx = 11
        if ref_attr_idx is None and len(row) > 12:
            ref_attr_idx = 12
        
        # Get reference information
        if ref_table_idx is not None and ref_attr_idx is not None:
            if ref_table_idx < len(row) and ref_attr_idx < len(row):
                if pd.notna(row.iloc[ref_table_idx]) and pd.notna(row.iloc[ref_attr_idx]):
                    ref_table = str(row.iloc[ref_table_idx]).strip()
                    ref_column = str(row.iloc[ref_attr_idx]).strip()
        
        # Handle multiple references (pipe-separated)
        if ref_table and ref_column:
            if '|' in ref_table and '|' in ref_column:
                ref_tables = ref_table.split('|')
                ref_columns = ref_column.split('|')
                
                for i in range(min(len(ref_tables), len(ref_columns))):
                    rt = ref_tables[i].strip()
                    rc = ref_columns[i].strip()
                    
                    if rt and rc and rt.lower() != "nan" and rc.lower() != "nan":
                        # Add NOT NULL constraint if it's a foreign key
                        if " NOT NULL" not in column_def:
                            idx = table_columns[table_name].index(column_def)
                            table_columns[table_name][idx] = column_def + " NOT NULL"
                        
                        foreign_keys.append({
                            'source_table': table_name,
                            'source_column': column_name,
                            'target_table': rt,
                            'target_column': rc
                        })
            else:
                # Single reference
                if ref_table.lower() != "nan" and ref_column.lower() != "nan":
                    # Add NOT NULL constraint if it's a foreign key
                    if " NOT NULL" not in column_def:
                        idx = table_columns[table_name].index(column_def)
                        table_columns[table_name][idx] = column_def + " NOT NULL"
                    
                    foreign_keys.append({
                        'source_table': table_name,
                        'source_column': column_name,
                        'target_table': ref_table,
                        'target_column': ref_column
                    })
    
    # Create a directed graph for cycle detection
    graph = nx.DiGraph()
    
    # Add foreign key relationships to the graph
    for fk in foreign_keys:
        source = fk['source_table']
        target = fk['target_table']
        graph.add_edge(source, target)
    
    # Detect cycles
    try:
        cycles = list(nx.simple_cycles(graph))
    except nx.NetworkXNoCycle:
        cycles = []
    except Exception as e:
        print(f"Warning: Error detecting cycles: {str(e)}")
        cycles = []
    
    # Remove edges to break cycles
    removed_edges = set()
    for cycle in cycles:
        for i in range(len(cycle)):
            source = cycle[i]
            target = cycle[(i+1) % len(cycle)]
            removed_edges.add((source, target))
            graph.remove_edge(source, target)
            print(f"Breaking cycle by removing foreign key from {source} to {target}")
            break  # Just remove one edge per cycle
    
    # Generate CREATE TABLE statements
    table_ddl_statements = []
    for table_name, columns in table_columns.items():
        table_ddl = f'CREATE TABLE {schema_name}."{table_name}" (\n    ' + ",\n    ".join(columns) + "\n);"
        table_ddl_statements.append(table_ddl)
    
    # Generate PRIMARY KEY constraints
    pk_statements = []
    for table_name, columns in primary_keys.items():
        if columns:
            pk_name = f"PK_{table_name}"
            pk_stmt = f'ALTER TABLE {schema_name}."{table_name}" ADD CONSTRAINT {pk_name} PRIMARY KEY ({", ".join(columns)});'
            pk_statements.append(pk_stmt)
    
    # Collect columns referenced by foreign keys that aren't primary keys
    referenced_columns = set()
    for fk in foreign_keys:
        referenced_columns.add((fk['target_table'], fk['target_column']))
    
    # Generate UNIQUE constraints for non-PK referenced columns
    unique_statements = []
    for target_table, target_column in referenced_columns:
        # Check if it's already a primary key
        is_pk = False
        if target_table in primary_keys:
            quoted_column = f'"{target_column}"' if db_type in ["POSTGRESQL", "ORACLE"] else target_column
            if quoted_column in primary_keys[target_table]:
                is_pk = True
        
        if not is_pk:
            quoted_column = f'"{target_column}"' if db_type in ["POSTGRESQL", "ORACLE"] else target_column
            uq_name = f"UQ_{target_table}_{target_column}"
            uq_stmt = f'ALTER TABLE {schema_name}."{target_table}" ADD CONSTRAINT {uq_name} UNIQUE ({quoted_column});'
            unique_statements.append(uq_stmt)
    
    # Generate FOREIGN KEY constraints
    fk_statements = []
    for fk in foreign_keys:
        source_table = fk['source_table']
        source_column = fk['source_column']
        target_table = fk['target_table']
        target_column = fk['target_column']
        
        # Skip foreign keys that would create cycles
        if (source_table, target_table) in removed_edges:
            continue
        
        quoted_source_column = f'"{source_column}"' if db_type in ["POSTGRESQL", "ORACLE"] else source_column
        quoted_target_column = f'"{target_column}"' if db_type in ["POSTGRESQL", "ORACLE"] else target_column
        
        fk_name = f"FK_{source_table}_{source_column}"
        fk_stmt = f'ALTER TABLE {schema_name}."{source_table}" ADD CONSTRAINT {fk_name} FOREIGN KEY ({quoted_source_column}) REFERENCES {schema_name}."{target_table}"({quoted_target_column});'
        fk_statements.append(fk_stmt)
    
    # Combine all statements
    all_statements = (
        ddl_statements + 
        ["\n-- Create tables"] +
        table_ddl_statements + 
        ["\n-- Add primary key constraints"] +
        pk_statements +
        ["\n-- Add unique constraints for columns referenced by foreign keys"] +
        unique_statements +
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