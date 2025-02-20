import pandas as pd

def generate_mysql_ddl(df):
    
    ddl_statements = []  # List to store DDL code for each table
    first_table = True  # Flag to add CREATE SCHEMA only for the first table
    
    for table in df["Table"].unique():  # Looping through each unique table
        attributes = df[df["Table"] == table][["Attribute", "Data Type"]]  # Extracting columns and data types for the table
        
        # Creating the column definitions in the format [ ] datatype
        columns = ",\n    ".join([f"[{row['Attribute']}] {row['Data Type']}" for _, row in attributes.iterrows()])
        
        # Add CREATE SCHEMA only for the first table, without extra space
        if first_table:
            ddl_statements.append("CREATE SCHEMA NIC_DWH_STG;")
            first_table = False  # Set the flag to False after the first table
        
        # Constructing the CREATE TABLE SQL statement in the requested format with uppercase NIC_DWH_STG
        ddl = f"CREATE TABLE [NIC_DWH_STG].[{table}] (\n    {columns}\n);"
        ddl_statements.append(ddl)  # Adding the generated DDL statement to the list
    
    return "\n".join(ddl_statements)  # Returning all the DDL statements without extra spaces
