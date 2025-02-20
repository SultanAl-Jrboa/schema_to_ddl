import pandas as pd
from generate_postgresql_ddl import generate_postgresql_ddl
from generate_mysql_ddl import generate_mysql_ddl  # Import MySQL function
from generate_sqlserver_ddl import generate_sqlserver_ddl
from generate_oracle_ddl import generate_oracle_ddl  # Import Oracle function

# Load Schema Data from Excel
file_path = "/Users/faisal/Desktop/SCHEMA_DDL/large_schema_data.xlsx"
df = pd.read_excel(file_path, sheet_name="Sheet1")  # Update sheet_name to "Sheet1"

# User Selection for Database Type
print("\nSelect the database type: ")
print("1. PostgreSQL")
print("2. MySQL")
print("3. SQL Server")
print("4. Oracle")  # Added Oracle option
choice = input("Enter your choice (1/2/3/4): ")

# Initialize variables
ddl_output = ""
output_file_path = ""  # Ensure this is defined before the condition block

# Generate DDL and define file name based on user selection
if choice == "1":
    ddl_output = generate_postgresql_ddl(df)
    output_file_path = "/Users/faisal/Desktop/SCHEMA_DDL/postgresql_ddl.txt"
    print("\nDDL for PostgreSQL:\n", ddl_output)
elif choice == "2":    
    ddl_output = generate_mysql_ddl(df)  # Generate MySQL DDL
    output_file_path = "/Users/faisal/Desktop/SCHEMA_DDL/mysql_ddl.txt"
    print("\nDDL for MySQL:\n", ddl_output)
elif choice == "3":
    ddl_output = generate_sqlserver_ddl(df)
    output_file_path = "/Users/faisal/Desktop/SCHEMA_DDL/sqlserver_ddl.txt"
    print("\nDDL for SQL Server:\n", ddl_output)
elif choice == "4":  # Handling Oracle selection
    ddl_output = generate_oracle_ddl(df)
    output_file_path = "/Users/faisal/Desktop/SCHEMA_DDL/oracle_ddl.txt"
    print("\nDDL for Oracle:\n", ddl_output)
else:
    print("Invalid choice. Please select 1, 2, 3, or 4.")
    output_file_path = None  # Set output file path to None to avoid further errors
    exit()

# Only proceed if the output file path is valid
if output_file_path:
    # Write DDL to the respective text file
    with open(output_file_path, "w") as file:
        file.write(ddl_output)

    print(f"\nDDL has been saved to: {output_file_path}")
