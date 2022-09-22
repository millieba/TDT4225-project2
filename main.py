from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import os
from decouple import config

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

    def insert_user(self, id, has_labels):
        query = "INSERT INTO User (id, has_labels) VALUES ('%s', %s)"
        self.cursor.execute(query % (id, has_labels))
        self.db_connection.commit()
    
    def insert_activity(self, user_id, transportation_mode, start_date_time, end_date_time):
        query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', %s, %s)"
        self.cursor.execute(query % (user_id, transportation_mode, start_date_time, end_date_time))
        self.db_connection.commit()
    
    def insert_track_point(self, activity_id, lat, lon, altitude, date_days, date_time):
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        self.cursor.execute(query % (activity_id, lat, lon, altitude, date_days, date_time))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        # print("Data from table %s, raw format:" % table_name)
        # print(rows)
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
    def insert_dataset(self, dataset_path):

        print(os.path.join(dataset_path, 'labeled_ids.txt'))

        # Read labeled_ids.txt file
        labeled_ids = pd.read_csv(os.path.join(dataset_path, 'labeled_ids.txt'), delim_whitespace=True, header=None, dtype=str)

        subfolders = os.listdir(f'{dataset_path}/Data')
        for i, user in enumerate(subfolders):
            print(f'[{i + 1}/{len(subfolders)}] Inserting User: {user}')

            # Check if user is in labeled_ids.txt
            if user in labeled_ids.values:
                self.insert_user(user, 1)
            else:
                self.insert_user(user, 0)
            
            user_dir = f'{dataset_path}/Data/{user}/{"Trajectory"}'

            # Iterate through all activities for a specific user
            for activity in os.listdir(user_dir):
                plt_path = f'{user_dir}/{activity}'
                file = pd.read_csv(plt_path, skiprows=6, header=None, parse_dates=[[5, 6]], infer_datetime_format=True)

                # Only insert activities with less than or equal 2500 trackpoints
                if(len(file.index) <= 2500):
                    file.rename(inplace=True, columns={'5_6': 'time', 0: 'lat', 1: 'lon', 3: 'alt'})
                    file.drop(inplace=True, columns=[2,4])
                    
                    # Fetch start and end time for the activity
                    start_date_time = file.head(1)['time']
                    end_date_time = file.tail(1)['time']

                    print()

                    if user in labeled_ids.values:
                        # Read labels.txt file
                        labels = pd.read_csv(os.path.join(os.path.basename(os.path.dirname(user_dir)), 'labels.txt'), delim_whitespace=True, header=None, nfer_datetime_format=True)

                        # Check if start_time and end_time matches

                        # Get transportation_mode
                    else:
                        self.insert_activity(user, None, start_date_time, end_date_time)


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

        program.insert_dataset(config('DATASET_PATH'))

        program.fetch_data("User")

        program.drop_table("TrackPoint")
        program.drop_table("Activity")
        program.drop_table("User")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
