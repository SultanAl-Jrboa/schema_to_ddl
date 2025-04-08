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
    return type_mappings.get(db_type, {}).get(data_type.upper(), data_type)

def generate_ddl(db_type, excel_file_path):
    df = pd.read_excel(excel_file_path, sheet_name="Dataset Overview")
    db_type = str(df.iloc[12, 1]).strip().upper() if df.shape[0] > 12 and df.shape[1] > 1 else db_type
    df_metadata = pd.read_excel(excel_file_path, sheet_name="Metadata", skiprows=4).dropna(subset=["Table Name", "Attribute Name"])
    schema_name = "NIC_DWH_STG"

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
    
    table_ddl_statements = []
    primary_key_statements = []
    unique_constraint_statements = []
    foreign_key_statements = []
    
    # Collect all foreign key relationships to check for cycles
    fk_graph = nx.DiGraph()
    foreign_keys = []
    
    # First pass: collect table definitions and foreign key relationships
    for table in df_metadata["Table Name"].unique():
        table_df = df_metadata[df_metadata["Table Name"] == table]
        columns = []
        primary_keys = []
        referenced_columns = set()
        
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
                            # Store the foreign key relationship
                            foreign_keys.append({
                                'source_table': table,
                                'source_column': column_name,
                                'target_table': rt,
                                'target_column': ra
                            })
                            
                            # Add edge to the graph for cycle detection
                            fk_graph.add_edge(table, rt)
                            
                            # Track which columns are referenced
                            referenced_columns.add((rt, ra))
                else:
                    # Single reference
                    rt = str(ref_table).strip()
                    ra = str(ref_attr).strip()
                    if rt and ra:
                        # Store the foreign key relationship
                        foreign_keys.append({
                            'source_table': table,
                            'source_column': column_name,
                            'target_table': rt,
                            'target_column': ra
                        })
                        
                        # Add edge to the graph for cycle detection
                        fk_graph.add_edge(table, rt)
                        
                        # Track which columns are referenced
                        referenced_columns.add((rt, ra))
        
        table_ddl = f'CREATE TABLE {schema_name}."{table}" (\n    ' + ",\n    ".join(columns) + "\n);"
        table_ddl_statements.append(table_ddl)
        
        if primary_keys:
            pk_name = f"PK_{table}"
            primary_key_statements.append(f'ALTER TABLE {schema_name}."{table}" ADD CONSTRAINT {pk_name} PRIMARY KEY ({", ".join(primary_keys)});')
    
    # Find and break cycles in the foreign key graph
    cycles = list(nx.simple_cycles(fk_graph))
    removed_edges = set()
    
    for cycle in cycles:
        # Break the cycle by removing one edge
        for i in range(len(cycle)):
            source = cycle[i]
            target = cycle[(i + 1) % len(cycle)]
            removed_edges.add((source, target))
            fk_graph.remove_edge(source, target)
            print(f"Breaking cycle by removing foreign key from {source} to {target}")
            break  # Just remove one edge per cycle
    
    # Add unique constraints for referenced columns that aren't primary keys
    referenced_columns = set((ref['target_table'], ref['target_column']) for ref in foreign_keys)
    
    for table, column in referenced_columns:
        # Check if this column is already a primary key
        is_pk = False
        table_df = df_metadata[df_metadata["Table Name"] == table]
        
        for _, row in table_df.iterrows():
            if row["Attribute Name"].strip() == column:
                if str(row.get("Is it the Primary Key or part of the Primary Key?", "")).strip().upper() == "YES":
                    is_pk = True
                    break
        
        if not is_pk:
            # This column is referenced but not a primary key, so add a unique constraint
            quoted_column_name = f'"{column}"' if db_type in ["POSTGRESQL", "ORACLE"] else column
            constraint_name = f"UQ_{table}_{column}"
            unique_constraint_statements.append(f'ALTER TABLE {schema_name}."{table}" ADD CONSTRAINT {constraint_name} UNIQUE ({quoted_column_name});')
            
            # Also make sure the column has NOT NULL constraint
            # We'd need to update the original table creation for this
            for i, stmt in enumerate(table_ddl_statements):
                if f'CREATE TABLE {schema_name}."{table}"' in stmt:
                    quoted_col = f'"{column}"' if db_type in ["POSTGRESQL", "ORACLE"] else column
                    if quoted_col in stmt and " NOT NULL" not in stmt.split(quoted_col)[1].split(",")[0]:
                        # Add NOT NULL constraint to the column
                        parts = stmt.split(quoted_col)
                        before = parts[0]
                        after = parts[1]
                        after_parts = after.split(",", 1)
                        if len(after_parts) > 1:
                            new_stmt = before + quoted_col + after_parts[0] + " NOT NULL," + after_parts[1]
                        else:
                            new_stmt = before + quoted_col + after_parts[0] + " NOT NULL" + ";"
                        table_ddl_statements[i] = new_stmt
                        break
    
    # Generate foreign key statements, excluding those involved in cycles
    for fk in foreign_keys:
        source_table = fk['source_table']
        source_column = fk['source_column']
        target_table = fk['target_table']
        target_column = fk['target_column']
        
        if (source_table, target_table) not in removed_edges:
            quoted_source_column = f'"{source_column}"' if db_type in ["POSTGRESQL", "ORACLE"] else source_column
            quoted_target_column = f'"{target_column}"' if db_type in ["POSTGRESQL", "ORACLE"] else target_column
            
            fk_name = f"FK_{source_table}_{source_column}"
            foreign_key_statements.append(
                f'ALTER TABLE {schema_name}."{source_table}" ADD CONSTRAINT {fk_name} ' +
                f'FOREIGN KEY ({quoted_source_column}) REFERENCES {schema_name}."{target_table}"({quoted_target_column});'
            )
            
            # Also make sure the source column has NOT NULL constraint
            for i, stmt in enumerate(table_ddl_statements):
                if f'CREATE TABLE {schema_name}."{source_table}"' in stmt:
                    if quoted_source_column in stmt and " NOT NULL" not in stmt.split(quoted_source_column)[1].split(",")[0]:
                        # Add NOT NULL constraint to the source column
                        parts = stmt.split(quoted_source_column)
                        before = parts[0]
                        after = parts[1]
                        after_parts = after.split(",", 1)
                        if len(after_parts) > 1:
                            new_stmt = before + quoted_source_column + after_parts[0] + " NOT NULL," + after_parts[1]
                        else:
                            new_stmt = before + quoted_source_column + after_parts[0] + " NOT NULL" + ";"
                        table_ddl_statements[i] = new_stmt
                        break
    
    # Combine all statements
    all_statements = (
        ddl_statements + 
        table_ddl_statements + 
        ["\n-- Add primary key constraints"] +
        primary_key_statements +
        ["\n-- Add unique constraints for columns referenced by foreign keys"] +
        unique_constraint_statements +
        ["\n-- Add foreign key constraints"] +
        foreign_key_statements
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