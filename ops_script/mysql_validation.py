import mysql.connector

# Database credentials
MYSQL_HOST = "localhost"
MYSQL_USER = "sales_user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "sales_db"

# Establish a connection to the MySQL database
connection = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    auth_plugin="mysql_native_password"
)

# Create a cursor object to execute SQL queries
cursor = connection.cursor()

# Execute a query to select all records from the sales_data table
cursor.execute("SELECT * FROM sales_data")

# Fetch all the records
records = cursor.fetchall()

# Print the records
for record in records:
    print(record)
