from DbConnector import DbConnector
from tabulate import tabulate


class MainProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name, fields):
        query = "CREATE TABLE IF NOT EXISTS %s (%s)"
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % (table_name, fields))
        self.db_connection.commit()

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)
    
    def show_table_details(self, table_name):
        query = "DESCRIBE %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = MainProgram()

        # Create DB tables
        
        program.create_table(
            table_name="User",
            fields="""
            id VARCHAR(255) NOT NULL PRIMARY KEY,
            has_labels BIT NOT NULL
            """
        )
        program.create_table(
            table_name="Activity",
            fields="""
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE,
            transportation_mode VARCHAR(255),
            start_date_time DATETIME,
            end_date_time DATETIME
            """
        )
        program.create_table(
            table_name="TrackPoint",
            fields="""
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            activity_id INT NOT NULL,
            FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE,
            lat DOUBLE,
            lon DOUBLE,
            altitude INT,
            date_days DOUBLE,
            date_time DATETIME
            """
        )

        program.show_table_details(table_name="User")
        program.show_table_details(table_name="Activity")
        program.show_table_details(table_name="TrackPoint")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
