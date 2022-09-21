from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import os


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

    # Code based on https://heremaps.github.io/pptk/tutorials/viewer/geolife.html 
    def read_plt(self):
        dataset_root = "assignment-2/dataset/Data"
        for user in os.listdir(dataset_root):
            if (user=="001"):
                user_dir = f'{dataset_root}/{user}/{"Trajectory"}'

                for activity in os.listdir(user_dir):
                    plt_path = f'{user_dir}/{activity}'
                    file = pd.read_csv(plt_path, skiprows=6, header=None, parse_dates=[[5, 6]], infer_datetime_format=True)

                    if(len(file.index)>100 and len(file.index)<200):
                        print(activity)
                        file.rename(inplace=True, columns={'5_6': 'time', 0: 'lat', 1: 'lon', 3: 'alt'})
                        file.drop(inplace=True, columns=[2,4])
                        print(file)


def main():
    program = None
    try:
        program = MainProgram()
        program.read_plt()
        program.create_table(table_name="Person")
        program.insert_data(table_name="Person")
        _ = program.fetch_data(table_name="Person")
        program.drop_table(table_name="Person")
        # Check that the table is dropped
        program.show_tables()
        

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
