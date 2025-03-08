# Schema-To-DDL

## Overview
Schema-To-DDL is a web-based tool that generates DDL (Data Definition Language) scripts for creating database schemas from provided metadata files, such as Excel. It supports multiple database systems, including:

- PostgreSQL
- MySQL
- SQL Server
- Oracle

## Features
- **Automated DDL Generation**: Converts schema metadata into SQL scripts.
- **Multi-Database Support**: Generates scripts for PostgreSQL, MySQL, SQL Server, and Oracle.
- **Web-Based Interface**: Upload an Excel file and download the generated DDL script.
- **Schema Validation**: Ensures correct mapping of data types.

## Live Demo
Try the live version of the tool: [Schema-To-DDL](https://schema-to-ddl.onrender.com/)

## Installation
To run the project locally:

1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/schema-to-ddl.git
   cd schema-to-ddl
   ```
2. Create a virtual environment:
   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```
3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
4. Run the application:
   ```sh
   python app.py
   ```

The application will run on `http://localhost:5000/`

## Deployment
The application is deployed on Render. To deploy manually:
1. Push your code to GitHub.
2. Connect Render to your repository.
3. Configure environment variables (if needed).
4. Deploy the service.

## Usage
1. Open the web application.
2. Upload an Excel file with the required schema metadata.
3. Download the generated DDL script.

## Folder Structure
```
/schema-to-ddl
│── app.py               # Main Flask application
│── templates/
│   └── index.html       # Frontend template
│── uploads/             # Directory for uploaded files
│── outputs/             # Directory for generated DDL scripts
│── requirements.txt     # Dependencies
│── README.md            # Project documentation
```

## Contributing
1. Fork the repository.
2. Create a new branch.
3. Make your changes and commit.
4. Push to your fork and submit a pull request.

## License
This project is licensed under the MIT License.

## Author
**Sultan Al-Jarboa**  
[LinkedIn](https://www.linkedin.com/in/sultanal-jrboa/)

