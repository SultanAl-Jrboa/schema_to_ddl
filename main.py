from flask import Flask, request, render_template, send_file
import pandas as pd
import os
import networkx as nx
import hashlib

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

def generate_short_name(input_str, max_length=20):
    """
    Generate a short unique identifier based on the input string
    """
    # Create a hash of the input string
    hash_object = hashlib.md5(input_str.encode())
    hex_dig = hash_object.hexdigest()
    
    # Take first 8 characters of hash and combine with truncated input string
    short_hash = hex_dig[:8]
    
    # Take first few characters of the input string
    prefix_length = max_length - len(short_hash) - 1
    if prefix_length <= 0:
        prefix_length = 1
    
    prefix = input_str[:prefix_length]
    
    # Combine them
    return f"{prefix}_{short_hash}"

def should_skip_column(column_name, db_type):
    """
    Check if a column should be skipped (for SQL Server CDC columns)
    """
    if db_type == "SQL_SERVER":
        skip_columns = [
            "CDC_TS", "CDC_operation", "CDC_start_lsn", "CDC_end_lsn", 
            "CDC_seqval", "CDC_update_mask", "CDC_command_id"
        ]
        return column_name in skip_columns
    return False

def get_or_create_schema(table_name, default_schema="dbo"):
    """
    Determine or create schema for a table name
    """
    # If table name contains a dot, split it
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        return schema, table
    
    # For SQL Server, if table is not in dbo, create a schema with the table name
    # For other databases, use the default schema
    return table_name, table_name

def generate_short_constraint_name(prefix, source_table, source_column, target_table, target_column, max_length=30):
    """
    Generate a short, unique constraint name
    """
    # Create a base name from table and column names
    base_name = f"{source_table}_{source_column}_{target_table}_{target_column}"
    
    # Create a hash of the base name
    hash_object = hashlib.md5(base_name.encode())
    short_hash = hash_object.hexdigest()[:8]
    
    # Combine prefix with hash
    full_name = f"{prefix}_{short_hash}"
    
    # Truncate if necessary
    return full_name[:max_length]

def generate_ddl(db_type, excel_file_path):
    # Read Excel file
    try:
        df = pd.read_excel(excel_file_path, sheet_name="Dataset Overview")
        # If database type is specified in the Excel file, use it
        if df.shape[0] > 12 and df.shape[1] > 1:
            specified_db_type = str(df.iloc[12, 1]).strip().upper()
            if specified_db_type in ["MYSQL", "ORACLE", "SQL_SERVER", "POSTGRESQL"]:
                db_type = specified_db_type
        
        # Read metadata
        df_metadata = pd.read_excel(excel_file_path, sheet_name="Metadata", skiprows=4)
        print("Available columns in Metadata sheet:", df_metadata.columns.tolist())
        
        # Validate required columns
        required_columns = ["Table Name", "Attribute Name"]
        for col in required_columns:
            if col not in df_metadata.columns:
                raise Exception(f"Required column '{col}' is missing from Excel file")
        
        df_metadata = df_metadata.dropna(subset=["Table Name", "Attribute Name"])
        default_schema_name = "NIC_DWH_STG"
    except Exception as e:
        raise Exception(f"Error reading Excel file: {str(e)}")
    
    # Initialize DDL statements
    ddl_statements = [f"-- DDL for database: {db_type}\n"]
    
    # Track schemas to create (especially for SQL Server)
    schemas_to_create = set([default_schema_name, "dbo"])
    
    # Collect table and column information
    table_info = {}
    foreign_keys = []
    all_table_names = set()
    
    # Process metadata rows
    for index, row in df_metadata.iterrows():
        # Skip rows with missing required data
        if pd.isna(row.get("Table Name")) or pd.isna(row.get("Attribute Name")):
            continue
        
        # Normalize table and column names
        original_table_name = normalize_name(row["Table Name"])
        column_name = normalize_name(row["Attribute Name"])
        
        # Skip CDC columns for SQL Server
        if should_skip_column(column_name, db_type):
            continue
        
        # Determine schema and table name
        if db_type == "SQL_SERVER":
            schema_name, table_name = get_or_create_schema(original_table_name)
            schemas_to_create.add(schema_name)
        else:
            schema_name = default_schema_name
            table_name = original_table_name
        
        # Add to tracked tables and schemas
        all_table_names.add(table_name)
        
        # Consistent table key across databases
        table_key = table_name if db_type != "SQL_SERVER" else f"{schema_name}.{table_name}"
        
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
        if table_key not in table_info:
            table_info[table_key] = {
                'columns': [],
                'primary_keys': [],
                'schema_name': schema_name
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
            table_info[table_key]['primary_keys'].append(quoted_column_name)
        
        table_info[table_key]['columns'].append(column_info)
        
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
                    ref_table_orig = normalize_name(row.iloc[ref_table_idx])
                    ref_column = normalize_name(row.iloc[ref_attr_idx])
                    
                    # Process references
                    if ref_table_orig and ref_column:
                        # Handle SQL Server schema.table format
                        if db_type == "SQL_SERVER" and "." in ref_table_orig:
                            ref_schema_name, ref_table = get_or_create_schema(ref_table_orig)
                            schemas_to_create.add(ref_schema_name)
                        else:
                            ref_schema_name = schema_name
                            ref_table = ref_table_orig
                            
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
                                    
                                    # For SQL Server, store the reference with schema
                                    if db_type == "SQL_SERVER":
                                        ref_target_key = f"{ref_schema_name}.{rt}"
                                    else:
                                        ref_target_key = rt
                                        
                                    foreign_keys.append({
                                        'source_table': table_key,
                                        'source_column': column_name,
                                        'target_table': ref_target_key,
                                        'target_column': rc,
                                        'source_schema': schema_name,
                                        'target_schema': ref_schema_name
                                    })
                        else:
                            # Single reference
                            if ref_table.lower() != "nan" and ref_column.lower() != "nan":
                                # Mark as foreign key and requires NOT NULL
                                column_info['is_foreign_key'] = True
                                column_info['not_null'] = True
                                
                                # Add table name to set
                                all_table_names.add(ref_table)
                                
                                # For SQL Server, store the reference with schema
                                if db_type == "SQL_SERVER":
                                    ref_target_key = f"{ref_schema_name}.{ref_table}"
                                else:
                                    ref_target_key = ref_table
                                    
                                foreign_keys.append({
                                    'source_table': table_key,
                                    'source_column': column_name,
                                    'target_table': ref_target_key,
                                    'target_column': ref_column,
                                    'source_schema': schema_name,
                                    'target_schema': ref_schema_name
                                })
    
    # For SQL Server, add schema creation statements
    if db_type == "SQL_SERVER":
        schema_statements = []
        for schema in schemas_to_create:
            if schema != "dbo":  # Don't need to create dbo schema
                schema_statements.append(f"-- IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') BEGIN\n--     EXEC('CREATE SCHEMA [{schema}]');\n-- END;")
        
        if schema_statements:
            ddl_statements.append("\n".join(schema_statements) + "\n")
    
    # Check for table name typos by comparing similar names
    table_name_mapping = {}
    for name in all_table_names:
        # Standardize the name (ignore case, remove spaces)
        standard_name = name.upper().replace(" ", "")
        
        # If standardized name exists, check which is more common
        if standard_name in table_name_mapping:
            # Keep the name that appears more frequently
            existing_name = table_name_mapping[standard_name]
            existing_count = sum(1 for t in table_info if t.endswith(existing_name))
            new_count = sum(1 for t in table_info if t.endswith(name))
            
            if new_count > existing_count:
                table_name_mapping[standard_name] = name
        else:
            table_name_mapping[standard_name] = name
    
    # Fix table name typos in foreign keys
    for fk in foreign_keys:
        if db_type == "SQL_SERVER":
            # Extract the table part from schema.table
            source_parts = fk['source_table'].split(".", 1)
            target_parts = fk['target_table'].split(".", 1)
            
            source_schema = source_parts[0]
            source_table = source_parts[1] if len(source_parts) > 1 else source_parts[0]
            
            target_schema = target_parts[0]
            target_table = target_parts[1] if len(target_parts) > 1 else target_parts[0]
            
            source_std = source_table.upper().replace(" ", "")
            target_std = target_table.upper().replace(" ", "")
            
            if source_std in table_name_mapping and source_table != table_name_mapping[source_std]:
                print(f"Correcting table name: {source_table} -> {table_name_mapping[source_std]}")
                fk['source_table'] = f"{source_schema}.{table_name_mapping[source_std]}"
                
            if target_std in table_name_mapping and target_table != table_name_mapping[target_std]:
                print(f"Correcting table name: {target_table} -> {table_name_mapping[target_std]}")
                fk['target_table'] = f"{target_schema}.{table_name_mapping[target_std]}"
        else:
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
    
    for table_key, info in table_info.items():
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
        
        # Extract table name (and schema for SQL Server)
        if db_type == "SQL_SERVER":
            parts = table_key.split(".", 1)
            schema_name = parts[0]
            table_name = parts[1] if len(parts) > 1 else parts[0]
        else:
            schema_name = default_schema_name
            table_name = table_key
            
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
    for table_key, info in table_info.items():
        if info['primary_keys']:
            # Get a nice short name for the constraint
            if db_type == "SQL_SERVER":
                parts = table_key.split(".", 1)
                schema_name = parts[0]
                table_name = parts[1] if len(parts) > 1 else parts[0]
            else:
                schema_name = default_schema_name
                table_name = table_key
                
            # Create a shorter PK name
            pk_name = f"PK_{generate_short_name(table_name)}"
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
    
    for target_table_key, target_column in referenced_columns:
        # Skip if the table isn't in our processed tables
        if target_table_key not in table_info:
            continue
            
        # Check if it's already a primary key
        is_pk = False
        for col in table_info[target_table_key]['columns']:
            if col['name'] == target_column and col['is_primary_key']:
                is_pk = True
                break
        
        if not is_pk:
            # Extract table name (and schema for SQL Server)
            if db_type == "SQL_SERVER":
                parts = target_table_key.split(".", 1)
                schema_name = parts[0]
                table_name = parts[1] if len(parts) > 1 else parts[0]
            else:
                schema_name = default_schema_name
                table_name = target_table_key
                
            quoted_column = format_identifier(target_column, db_type)
            quoted_table_name = format_identifier(table_name, db_type)
            
            # Create a shorter unique constraint name
            uq_name = f"UQ_{generate_short_name(table_name + '_' + target_column)}"
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
        
        # Generate a short, unique foreign key constraint name
        fk_name = generate_short_constraint_name(
            "FK", 
            source_table.split('.')[-1], 
            source_column, 
            target_table.split('.')[-1], 
            target_column
        )
        
        # Extract schema and table names
        if db_type == "SQL_SERVER":
            source_parts = source_table.split(".", 1)
            source_schema = source_parts[0]
            source_table_name = source_parts[1] if len(source_parts) > 1 else source_parts[0]
            
            target_parts = target_table.split(".", 1)
            target_schema = target_parts[0]
            target_table_name = target_parts[1] if len(target_parts) > 1 else target_parts[0]
        else:
            source_schema = default_schema_name
            source_table_name = source_table.split('.')[-1]
            target_schema = default_schema_name
            target_table_name = target_table.split('.')[-1]
        
        # Quote identifiers
        quoted_source_column = format_identifier(source_column, db_type)
        quoted_target_column = format_identifier(target_column, db_type)
        quoted_source_table_name = format_identifier(source_table_name, db_type)
        quoted_target_table_name = format_identifier(target_table_name, db_type)
        
        # Make sure constraint name is unique
        base_fk_name = fk_name
        counter = 1
        while fk_name in fk_constraints:
            fk_name = f"{base_fk_name}_{counter}"
            counter += 1
        
        fk_constraints.add(fk_name)
        
        # Generate foreign key constraint statement
        if db_type == "SQL_SERVER":
            fk_stmt = f'ALTER TABLE {source_schema}.{quoted_source_table_name} ADD CONSTRAINT {fk_name} ' \
                      f'FOREIGN KEY ({quoted_source_column}) ' \
                      f'REFERENCES {target_schema}.{quoted_target_table_name}({quoted_target_column});'
        else:
            quoted_source_schema = format_identifier(source_schema, db_type) if db_type == "POSTGRESQL" else source_schema
            quoted_target_schema = format_identifier(target_schema, db_type) if db_type == "POSTGRESQL" else target_schema
            
            fk_stmt = f'ALTER TABLE {quoted_source_schema}.{quoted_source_table_name} ADD CONSTRAINT {fk_name} ' \
                      f'FOREIGN KEY ({quoted_source_column}) ' \
                      f'REFERENCES {quoted_target_schema}.{quoted_target_table_name}({quoted_target_column});'
        
        fk_statements.append(fk_stmt)
    
    # Combine all statements
    final_statements = (
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
    
    return "\n\n".join(final_statements)

# Flask application
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
                with open(ddl_file_path, "w", encoding='utf-8') as f:
                    f.write(ddl_output)
                return send_file(ddl_file_path, as_attachment=True)
            except Exception as e:
                return f"Error: {str(e)}"
    
    return render_template('index.html')

# Create a simple HTML template for the upload form
def create_upload_template():
    template = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>DDL Generator</title>
        <style>
            body { 
                font-family: Arial, sans-serif; 
                max-width: 500px; 
                margin: 0 auto; 
                padding: 20px; 
            }
            h1 { text-align: center; }
            form { 
                background: #f4f4f4; 
                padding: 20px; 
                border-radius: 5px; 
            }
            input, select { 
                width: 100%; 
                padding: 10px; 
                margin: 10px 0; 
                box-sizing: border-box; 
            }
            input[type="submit"] {
                background: #4CAF50;
                color: white;
                border: none;
                cursor: pointer;
            }
            input[type="submit"]:hover {
                background: #45a049;
            }
        </style>
    </head>
    <body>
        <h1>DDL Generator</h1>
        <form method="POST" enctype="multipart/form-data">
            <label for="file">Select Excel File:</label>
            <input type="file" name="file" accept=".xlsx,.xls" required>
            
            <label for="db_type">Select Database Type:</label>
            <select name="db_type" required>
                <option value="POSTGRESQL">PostgreSQL</option>
                <option value="MYSQL">MySQL</option>
                <option value="ORACLE">Oracle</option>
                <option value="SQL_SERVER">SQL Server</option>
            </select>
            
            <input type="submit" value="Generate DDL">
        </form>
    </body>
    </html>
    '''
    
    # Ensure templates directory exists
    os.makedirs("templates", exist_ok=True)
    
    # Write the template
    with open("templates/index.html", "w", encoding='utf-8') as f:
        f.write(template)

# Create the upload template when the script is run
create_upload_template()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)