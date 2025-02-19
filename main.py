import pandas as pd
from generate_postgresql_ddl import generate_postgresql_ddl
from generate_mysql_ddl import generate_mysql_ddl
from generate_sqlserver_ddl import generate_sqlserver_ddl

# Load Schema Data from Excel
file_path = "/Users/faisal/Desktop/SCHEMA_DDL/large_schema_data.xlsx"
df = pd.read_excel(file_path)

# User Selection for Database Type
print("\nSelect the database type: ")
print("1. PostgreSQL")
print("2. MySQL")
print("3. SQL Server")
choice = input("Enter your choice (1/2/3): ")

# Generate DDL and define file name based on user selection
ddl_output = ""
output_file_path = ""

if choice == "1":
    ddl_output = generate_postgresql_ddl(df)
    output_file_path = "/Users/faisal/Desktop/SCHEMA_DDL/postgresql_ddl.txt"
    print("\nDDL for PostgreSQL:\n", ddl_output)
elif choice == "2":
    ddl_output = generate_mysql_ddl(df)
    output_file_path = "/Users/faisal/Desktop/SCHEMA_DDL/mysql_ddl.txt"
    print("\nDDL for MySQL:\n", ddl_output)
elif choice == "3":
    ddl_output = generate_sqlserver_ddl(df)
    output_file_path = "/Users/faisal/Desktop/SCHEMA_DDL/sqlserver_ddl.txt"
    print("\nDDL for SQL Server:\n", ddl_output)
else:
    print("Invalid choice. Please select 1, 2, or 3.")
    exit()

# Write DDL to the respective text file
with open(output_file_path, "w") as file:
    file.write(ddl_output)

print(f"\nDDL has been saved to: {output_file_path}")
