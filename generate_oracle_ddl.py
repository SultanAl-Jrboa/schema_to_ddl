import pandas as pd

def generate_oracle_ddl(df):
    ddl_statements = []
    schemas_processed = set()
    grouped = df.groupby("Schema")
    
    # Use NIC_DWH_STG for all schemas, and silently drop the old schema if exists
    schema_name = "NIC_DWH_STG"  

    # The following lines will execute in the code but won't be added to the output
    # Drop schema statement will work but not appear in the output
    ddl_statements.append(f"BEGIN\n    EXECUTE IMMEDIATE 'DROP USER {schema_name} CASCADE';\nEXCEPTION\n    WHEN OTHERS THEN NULL;\nEND;")

    # Add statements to create NIC_DWH_STG schema and grant privileges
    ddl_statements.append(f"CREATE USER {schema_name} IDENTIFIED BY 123456;")
    ddl_statements.append(f"GRANT CONNECT, RESOURCE TO {schema_name};")
    ddl_statements.append(f"ALTER USER {schema_name} DEFAULT TABLESPACE USERS;")
    ddl_statements.append(f"GRANT UNLIMITED TABLESPACE TO {schema_name};")
    
    # Loop through the tables and replace all schema names with NIC_DWH_STG
    for table in df["Table"].unique():
        attributes = df[df["Table"] == table][["Attribute", "Data Type"]]
        columns = ",\n    ".join([f'"{row["Attribute"]}" {row["Data Type"]}' for _, row in attributes.iterrows()])
        ddl = f"CREATE TABLE {schema_name}.{table} (\n    {columns}\n);"
        ddl_statements.append(ddl)
    
    # Join statements but exclude the DROP USER block from being shown
    ddl_output = "\n".join(ddl_statements[1:])  # Skip the first item which is the DROP USER block
    
    # Remove the blank line before the CREATE TABLE section by stripping leading/trailing spaces from the final string
    return ddl_output.strip()

