from flask import Flask, request, render_template, send_file
import pandas as pd
import os
import networkx as nx

def map_data_type(db_type, data_type):
    """
    Maps generic data types to database-specific data types
    """
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
    
    # Try to get the mapped type, use original if not found
    if data_type is None:
        return "VARCHAR(255)"
        
    data_type = str(data_type).strip().upper()
    return type_mappings.get(db_type, {}).get(data_type, data_type)

def get_identifier_quote(db_type):
    """
    Returns the appropriate identifier quote character(s) for each database type
    """
    if db_type == "MYSQL":
        return "`"
    elif db_type == "POSTGRESQL" or db_type == "ORACLE":
        return "\""
    elif db_type == "SQL_SERVER":
        return "["  # Opening bracket
    else:
        return "\""

def get_identifier_quote_close(db_type):
    """
    Returns the appropriate closing identifier quote character(s) for each database type
    """
    if db_type == "SQL_SERVER":
        return "]"  # Closing bracket
    else:
        return get_identifier_quote(db_type)  # Same as opening for others

def format_identifier(name, db_type):
    """
    Format identifier with the correct quoting style for the database
    """
    open_quote = get_identifier_quote(db_type)
    close_quote = get_identifier_quote_close(db_type)
    return f"{open_quote}{name}{close_quote}"

def normalize_name(name):
    """
    Normalize table and column names to avoid typos and case sensitivity issues
    """
    if name:
        return str(name).strip()
    return name

def generate_ddl(db_type, excel_file_path):
    # Read Excel file
    try:
        df = pd.read_excel(excel_file_path, sheet_name="Dataset Overview")
        # If database type is specified in the Excel file, use it
        if df.shape[0] > 12 and df.shape[1] > 1:
            specified_db_type = str(df.iloc[12, 1]).strip().upper()
            if specified_db_type in ["MYSQL", "ORACLE", "SQL_SERVER", "POSTGRESQL"]:
                db_type = specified_db_type
        
        # Print column names for debugging
        df_metadata = pd.read_excel(excel_file_path, sheet_name="Metadata", skiprows=4)
        print("Available columns in Metadata sheet:", df_metadata.columns.tolist())
        
        # Make sure required columns exist
        required_columns = ["Table Name", "Attribute Name"]
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
    
    # Track column information separately to avoid string comparison issues
    table_info = {}
    foreign_keys = []
    
    # Keep track of all table names to normalize them
    all_table_names = set()
    
    # First pass: Collect table and column information
    for index, row in df_metadata.iterrows():
        # Skip rows with missing required data
        if pd.isna(row.get("Table Name")) or pd.isna(row.get("Attribute Name")):
            continue
            
        table_name = normalize_name(row["Table Name"])
        column_name = normalize_name(row["Attribute Name"])
        
        # Add to set of all table names
        all_table_names.add(table_name)
        
        # Get data type with safe handling
        default_type = "VARCHAR(255)" if db_type != "ORACLE" else "VARCHAR2(255)"
        data_type = default_type  
        if "Data Type and Length" in row and pd.notna(row["Data Type and Length"]):
            data_type = str(row["Data Type and Length"]).strip()
        
        # Map data type according to target database
        mapped_data_type = map_data_type(db_type, data_type)
        
        # Format column name for the database
        quoted_column_name = format_identifier(column_name, db_type)
        
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
        if "Is it the Timestamp attribute?" in row and pd.notna(row["Is it the Timestamp attribute?"]):
            is_timestamp = str(row["Is it the Timestamp attribute?"]).strip().upper() == "YES"  
        
        # Initialize table info if needed
        if table_name not in table_info:
            table_info[table_name] = {
                'columns': [],
                'primary_keys': []
            }
        
        # Store column information in structured format
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
        
        # Check for foreign key relationships
        # Find which columns contain reference information
        ref_table_idx = None
        ref_attr_idx = None
        
        # First, try to find by header text
        for col_idx, col_name in enumerate(df_metadata.columns):
            col_name_str = str(col_name).lower()
            if "reference" in col_name_str and "table" in col_name_str:
                ref_table_idx = col_idx
            if "reference" in col_name_str and "attribute" in col_name_str:
                ref_attr_idx = col_idx
        
        # If not found by header, try to access by position 10-12
        if ref_table_idx is None and len(row) > 11:
            # Check if the column at index 11 has any value
            if pd.notna(row.iloc[11]):
                ref_table_idx = 11
        
        if ref_attr_idx is None and len(row) > 12:
            # Check if the column at index 12 has any value
            if pd.notna(row.iloc[12]):
                ref_attr_idx = 12
        
        # Get reference information if available
        if ref_table_idx is not None and ref_attr_idx is not None:
            if ref_table_idx < len(row) and ref_attr_idx < len(row):
                if pd.notna(row.iloc[ref_table_idx]) and pd.notna(row.iloc[ref_attr_idx]):
                    ref_table = normalize_name(row.iloc[ref_table_idx])
                    ref_column = normalize_name(row.iloc[ref_attr_idx])
                    
                    # Process references
                    if ref_table and ref_column:
                        # Handle multiple references (pipe-separated)
                        if '|' in ref_table and '|' in ref_column:
                            ref_tables = ref_table.split('|')
                            ref_columns = ref_column.split('|')
                            
                            for i in range(min(len(ref_tables), len(ref_columns))):
                                rt = normalize_name(ref_tables[i])
                                rc = normalize_name(ref_columns[i])
                                
                                if rt and rc and rt.lower() != "nan" and rc.lower() != "nan":
                                    # Mark as foreign key and requires NOT NULL
                                    column_info['is_foreign_key'] = True
                                    column_info['not_null'] = True
                                    
                                    # Add table name to set
                                    all_table_names.add(rt)
                                    
                                    foreign_keys.append({
                                        'source_table': table_name,
                                        'source_column': column_name,
                                        'target_table': rt,
                                        'target_column': rc
                                    })
                        else:
                            # Single reference
                            if ref_table.lower() != "nan" and ref_column.lower() != "nan":
                                # Mark as foreign key and requires NOT NULL
                                column_info['is_foreign_key'] = True
                                column_info['not_null'] = True
                                
                                # Add table name to set
                                all_table_names.add(ref_table)
                                
                                foreign_keys.append({
                                    'source_table': table_name,
                                    'source_column': column_name,
                                    'target_table': ref_table,
                                    'target_column': ref_column
                                })
    
    # Check for table name typos by comparing similar names
    table_name_mapping = {}
    for name in all_table_names:
        # Standardize the name (ignore case, remove spaces)
        standard_name = name.upper().replace(" ", "")
        
        # If standardized name exists, check which is more common
        if standard_name in table_name_mapping:
            # Keep the name that appears more frequently
            existing_name = table_name_mapping[standard_name]
            existing_count = sum(1 for t in table_info if t == existing_name)
            new_count = sum(1 for t in table_info if t == name)
            
            if new_count > existing_count:
                table_name_mapping[standard_name] = name
        else:
            table_name_mapping[standard_name] = name
    
    # Fix table name typos in foreign keys
    for fk in foreign_keys:
        source_std = fk['source_table'].upper().replace(" ", "")
        target_std = fk['target_table'].upper().replace(" ", "")
        
        if source_std in table_name_mapping and fk['source_table'] != table_name_mapping[source_std]:
            print(f"Correcting table name: {fk['source_table']} -> {table_name_mapping[source_std]}")
            fk['source_table'] = table_name_mapping[source_std]
            
        if target_std in table_name_mapping and fk['target_table'] != table_name_mapping[target_std]:
            print(f"Correcting table name: {fk['target_table']} -> {table_name_mapping[target_std]}")
            fk['target_table'] = table_name_mapping[target_std]
    
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
        print(f"Detected cycles: {cycles}")
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
    
    for table_name, info in table_info.items():
        column_defs = []
        
        for col in info['columns']:
            # Build column definition with constraints
            col_def = f"{col['quoted_name']} {col['data_type']}"
            
            if col['not_null']:
                col_def += " NOT NULL"
                
            if col['is_last_operation']:
                if db_type == "MYSQL":
                    # For MySQL, use ENUM type for LastOperation
                    col_def = f"{col['quoted_name']} ENUM('INSERT', 'UPDATE', 'DELETE')"
                else:
                    # For other databases, use CHECK constraints
                    check_column = col['quoted_name']
                    col_def += f" CHECK ({check_column} IN ('INSERT', 'UPDATE', 'DELETE'))"
                
            if col['is_timestamp']:
                if db_type == "MYSQL":
                    col_def += " DEFAULT CURRENT_TIMESTAMP"
                elif db_type == "POSTGRESQL":
                    col_def += " DEFAULT CURRENT_TIMESTAMP"
                elif db_type == "ORACLE":
                    col_def += " DEFAULT CURRENT_TIMESTAMP"
                elif db_type == "SQL_SERVER":
                    col_def += " DEFAULT CURRENT_TIMESTAMP"
                
            column_defs.append(col_def)
        
        # Create table DDL with proper quoting
        quoted_table_name = format_identifier(table_name, db_type)
        
        # In SQL Server, we don't need to prefix schema name with []
        if db_type == "SQL_SERVER":
            table_ddl = f'CREATE TABLE {schema_name}.{quoted_table_name} (\n    ' + ",\n    ".join(column_defs) + "\n);"
        else:
            # For other databases, format schema name consistently with table names
            quoted_schema = format_identifier(schema_name, db_type) if db_type == "POSTGRESQL" else schema_name
            table_ddl = f'CREATE TABLE {quoted_schema}.{quoted_table_name} (\n    ' + ",\n    ".join(column_defs) + "\n);"
            
        table_ddl_statements.append(table_ddl)
    
    # Generate PRIMARY KEY constraints
    pk_statements = []
    for table_name, info in table_info.items():
        if info['primary_keys']:
            pk_name = f"PK_{table_name}"
            pk_columns = ", ".join(info["primary_keys"])
            
            quoted_table_name = format_identifier(table_name, db_type)
            
            # For SQL Server, don't quote the schema name
            if db_type == "SQL_SERVER":
                pk_stmt = f'ALTER TABLE {schema_name}.{quoted_table_name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_columns});'
            else:
                quoted_schema = format_identifier(schema_name, db_type) if db_type == "POSTGRESQL" else schema_name
                pk_stmt = f'ALTER TABLE {quoted_schema}.{quoted_table_name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_columns});'
            
            pk_statements.append(pk_stmt)
    
    # Collect columns referenced by foreign keys that aren't primary keys
    referenced_columns = set()
    for fk in foreign_keys:
        referenced_columns.add((fk['target_table'], fk['target_column']))
    
    # Generate UNIQUE constraints for non-PK referenced columns
    unique_statements = []
    unique_constraints = set()  # Track constraints to avoid duplicates
    
    for target_table, target_column in referenced_columns:
        # Skip if the table isn't in our processed tables
        if target_table not in table_info:
            continue
            
        # Check if it's already a primary key
        is_pk = False
        for col in table_info[target_table]['columns']:
            if col['name'] == target_column and col['is_primary_key']:
                is_pk = True
                break
        
        if not is_pk:
            quoted_column = format_identifier(target_column, db_type)
            quoted_table_name = format_identifier(target_table, db_type)
            
            # Create a unique constraint name and check for duplicates
            uq_name = f"UQ_{target_table}_{target_column}"
            if uq_name in unique_constraints:
                continue
                
            unique_constraints.add(uq_name)
            
            # For SQL Server, don't quote the schema name
            if db_type == "SQL_SERVER":
                uq_stmt = f'ALTER TABLE {schema_name}.{quoted_table_name} ADD CONSTRAINT {uq_name} UNIQUE ({quoted_column});'
            else:
                quoted_schema = format_identifier(schema_name, db_type) if db_type == "POSTGRESQL" else schema_name
                uq_stmt = f'ALTER TABLE {quoted_schema}.{quoted_table_name} ADD CONSTRAINT {uq_name} UNIQUE ({quoted_column});'
            
            unique_statements.append(uq_stmt)
    
    # Generate FOREIGN KEY constraints
    fk_statements = []
    fk_constraints = set()  # Track constraint names to avoid duplicates
    
    for fk in foreign_keys:
        source_table = fk['source_table']
        source_column = fk['source_column']
        target_table = fk['target_table']
        target_column = fk['target_column']
        
        # Skip foreign keys that would create cycles
        if (source_table, target_table) in removed_edges:
            print(f"Skipping foreign key {source_table}.{source_column} -> {target_table}.{target_column} to avoid cycles")
            continue
        
        # Skip if target table doesn't exist in our schema
        if target_table not in table_info:
            print(f"Skipping foreign key to non-existent table: {target_table}")
            continue
        
        quoted_source_column = format_identifier(source_column, db_type)
        quoted_target_column = format_identifier(target_column, db_type)
        quoted_source_table = format_identifier(source_table, db_type)
        quoted_target_table = format_identifier(target_table, db_type)
        
        # Create a unique constraint name
        fk_name = f"FK_{source_table}_{source_column}_TO_{target_table}"
        
        # Make sure we don't have duplicate constraint names
        base_fk_name = fk_name
        counter = 1
        while fk_name in fk_constraints:
            fk_name = f"{base_fk_name}_{counter}"
            counter += 1
            
        fk_constraints.add(fk_name)
        
        # For SQL Server, don't quote the schema name
        if db_type == "SQL_SERVER":
            fk_stmt = f'ALTER TABLE {schema_name}.{quoted_source_table} ADD CONSTRAINT {fk_name} FOREIGN KEY ({quoted_source_column}) REFERENCES {schema_name}.{quoted_target_table}({quoted_target_column});'
        else:
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