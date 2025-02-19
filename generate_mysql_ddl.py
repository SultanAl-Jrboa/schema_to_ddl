import pandas as pd

def generate_mysql_ddl(df):
    
    ddl_statements = []  # List to store DDL code for each table
    for table in df["Table"].unique():  # Looping through each unique table
        attributes = df[df["Table"] == table][["Attribute", "Data Type"]]  # Extracting columns and data types for the table
        # Creating the column definitions in SQL format
        columns = ",\n    ".join([f"{row['Attribute']} {row['Data Type']}" for _, row in attributes.iterrows()])
        # Constructing the CREATE TABLE SQL statement
        ddl = f"CREATE TABLE {table} (\n    {columns}\n);"
        ddl_statements.append(ddl)  # Adding the generated DDL statement to the list
    
    return "\n\n".join(ddl_statements)  # Returning all the DDL statements as a formatted string
