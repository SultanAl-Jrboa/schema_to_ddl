# **Schema to DDL Converter**

## **Overview**
The **Schema to DDL Converter** is a Python-based tool that automatically generates **Database Definition Language (DDL)** statements from an Excel schema file. This tool supports multiple databases, including **MySQL, PostgreSQL, and SQL Server**, making it easy to create structured database tables from predefined schema definitions.

## **Features**
✅ **Automatic DDL Generation** – Converts schema data from an Excel file into SQL **CREATE TABLE** statements.  
✅ **Multi-Database Support** – Generates DDL scripts for **MySQL, PostgreSQL, and SQL Server**.  
✅ **Easy-to-Use** – Simply provide an **Excel file** with table structures, and the script will generate the corresponding SQL code.  
✅ **Scalable & Customizable** – Can be extended to support additional database systems.  

## **How It Works**
1 - The tool reads an **Excel file (`large_schema_data.xlsx`)** containing table names, attributes, and data types.  
2 - It processes each table and its columns to construct **DDL (`CREATE TABLE`) statements**.  
3 - The generated **SQL scripts** are saved in a **`.txt` file** for easy access and use.  


# **schema_to_ddl/** 
# Main script to run the tool
│── main.py      

# MySQL DDL generation logic
│── generate_mysql_ddl.py    

# PostgreSQL DDL generation logic
│── generate_postgresql_ddl.py 

# SQL Server DDL generation logic
│── generate_sqlserver_ddl.py   

# Example schema file
│── large_schema_data.xlsx      

# Generated DDL statements
│── output_ddl.txt              

