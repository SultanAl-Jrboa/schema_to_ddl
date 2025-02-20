import pandas as pd

def generate_sqlserver_ddl(df):
    ddl_statements = []  

    ddl_statements.append("CREATE SCHEMA NIC_DWH_STG;")
    
    for table in df["Table"].unique():  
        schema = df[df["Table"] == table]["Schema"].values[0]  

        new_schema = "NIC_DWH_STG"
        
        attributes = df[df["Table"] == table][["Attribute", "Data Type"]]
        
        columns = ",\n    ".join([f"[{row['Attribute']}] {row['Data Type']}" for _, row in attributes.iterrows()])
        
        ddl = f"CREATE TABLE [{new_schema}].[{table}] (\n    {columns}\n);"
        
        ddl_statements.append(ddl)

    return "\n".join(ddl_statements)
