import mysql.connector as mysql
from decouple import config

class DbConnector:
    """
    Connects to the MySQL server on the Ubuntu virtual machine.
    Connector needs HOST, DATABASE, USERNAME and PASSWORD to connect,
    while PORT is optional and should be 3306.

    Example:
    Read values from .env file.

    HOST = ${HOST} // Your server IP address/domain name
    DATABASE = ${DATABASE} // Database name, if you just want to connect to MySQL server, leave it empty
    USER = ${MYSQL_USER} // This is the user you created and added privileges for
    PASSWORD = ${PASSWORD} // The password you set for said user
    """

    def __init__(self,
                 HOST=config('HOST'),
                 DATABASE=config('DATABASE'),
                 USER=config('MYSQL_USER'),
                 PASSWORD=config('PASSWORD')):
        # Connect to the database
        try:
            self.db_connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=3306)
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # Get the db cursor
        self.cursor = self.db_connection.cursor()

        print("Connected to:", self.db_connection.get_server_info())
        # Get database information
        self.cursor.execute("select database();")
        database_name = self.cursor.fetchone()
        print("You are connected to the database:", database_name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # Close the cursor
        self.cursor.close()
        # Close the DB connection
        self.db_connection.close()
        print("\n-----------------------------------------------")
        print("Connection to %s is closed" % self.db_connection.get_server_info())
