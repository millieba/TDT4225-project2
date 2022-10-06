from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import os
from tqdm import tqdm
from decouple import config
from haversine import haversine


class MainProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.db_connection.autocommit = False
        self.cursor = self.connection.cursor

    def create_table(self, table_name, fields):
        query = "CREATE TABLE IF NOT EXISTS %s (%s)"
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % (table_name, fields))
        self.db_connection.commit()

    def insert_user(self, id, has_labels):
        query = """
                    INSERT INTO User (id, has_labels)
                    VALUES ('%s', %s)
                    ON DUPLICATE KEY UPDATE
                    id = VALUES(id),
                    has_labels = VALUES(has_labels)
                """
        self.cursor.execute(query % (id, has_labels))
        self.db_connection.commit()

    def insert_activity(self, user_id, transportation_mode, start_date_time, end_date_time):
        # Insert NULL to the database if no transportation_mode, by not treating it as a string in the statement
        if transportation_mode == 'NULL':
            query = """
                        INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
                        VALUES ('%s', %s, '%s', '%s')
                        ON DUPLICATE KEY UPDATE
                        user_id = VALUES(user_id),
                        transportation_mode = VALUES(transportation_mode),
                        start_date_time = VALUES(start_date_time),
                        end_date_time = VALUES(end_date_time)
                    """
            self.cursor.execute(
                query % (user_id, transportation_mode, start_date_time, end_date_time))
            self.db_connection.commit()
        else:
            query = """
                        INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
                        VALUES ('%s', '%s', '%s', '%s')
                        ON DUPLICATE KEY UPDATE
                        user_id = VALUES(user_id),
                        transportation_mode = VALUES(transportation_mode),
                        start_date_time = VALUES(start_date_time),
                        end_date_time = VALUES(end_date_time)
                    """
            self.cursor.execute(
                query % (user_id, transportation_mode, start_date_time, end_date_time))
            self.db_connection.commit()

    def insert_track_points_batch(self, values):
        query = """
                    INSERT INTO TrackPoint (date_time, lat, lon, altitude, date_days, activity_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    date_time = VALUES(date_time),
                    lat = VALUES(lat), lon = VALUES(lon),
                    altitude = VALUES(altitude),
                    date_days = VALUES(date_days),
                    activity_id = VALUES(activity_id)
                """
        self.cursor.executemany(query, values)
        self.db_connection.commit()

    def fetch_last_insert_id(self):
        query = "SELECT @id:=LAST_INSERT_ID()"
        self.cursor.execute(query)
        id = self.cursor.fetchone()[0]
        return id

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
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

        # Read labeled_ids.txt file
        labeled_ids = pd.read_csv(
            f'{dataset_path}/labeled_ids.txt', delim_whitespace=True, header=None, dtype=str)

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
            for activity in tqdm(os.listdir(user_dir)):
                plt_path = f'{user_dir}/{activity}'
                file = pd.read_csv(plt_path, skiprows=6, header=None, parse_dates=[
                                   [5, 6]], infer_datetime_format=True)

                # Only insert activities with less than or equal 2500 trackpoints
                if (len(file.index) <= 2500):
                    # Rename columns for clarity and remove unused columns
                    file.rename(inplace=True, columns={
                                0: 'lat', 1: 'lon', 3: 'alt', 4: 'date_days', '5_6': 'date_time'})
                    file.drop(inplace=True, columns=[2])

                    # Fetch start and end time for the activity
                    start_date_time = pd.to_datetime(
                        file.head(1)['date_time'].values[0], format="%Y/%m/%d %H:%M:%S")
                    end_date_time = pd.to_datetime(
                        file.tail(1)['date_time'].values[0], format="%Y/%m/%d %H:%M:%S")

                    if user in labeled_ids.values:
                        # Read labels.txt file and rename columns for clarity
                        labels = pd.read_csv(f'{os.path.dirname(user_dir)}/labels.txt', delim_whitespace=True,
                                             skiprows=1, header=None, parse_dates=[[0, 1], [2, 3]], infer_datetime_format=True)
                        labels.rename(inplace=True, columns={
                                      '0_1': 'start_date_time', '2_3': 'end_date_time', 4: 'transportation_mode'})

                        # Match start time and end time in labels
                        matching_row = labels[((labels['start_date_time'] == start_date_time) & (
                            labels['end_date_time'] == end_date_time))]

                        # Check if there is a match
                        if len(matching_row) > 0:
                            transportation_mode = matching_row['transportation_mode'].values[0]
                            self.insert_activity(
                                user, transportation_mode, start_date_time, end_date_time)
                            activity_id = self.fetch_last_insert_id()
                            file['activity_id'] = activity_id
                            self.insert_track_points_batch(
                                list(file.itertuples(index=False, name=None)))
                        else:
                            self.insert_activity(
                                user, 'NULL', start_date_time, end_date_time)
                            activity_id = self.fetch_last_insert_id()
                            file['activity_id'] = activity_id
                            self.insert_track_points_batch(
                                list(file.itertuples(index=False, name=None)))
                    else:
                        self.insert_activity(
                            user, 'NULL', start_date_time, end_date_time)
                        activity_id = self.fetch_last_insert_id()
                        file['activity_id'] = activity_id
                        self.insert_track_points_batch(
                            list(file.itertuples(index=False, name=None)))


    def part2_task1(self):
        query = """ 
                    SELECT
                    (SELECT COUNT(*) AS Users FROM User),
                    (SELECT COUNT(*) AS Activities FROM Activity),
                    (SELECT COUNT(*) AS Trackpoints FROM TrackPoint)
                """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 1: \n")
        print(tabulate(result, headers=["Users", "Activities", "Trackpoints"]))

        
    def part2_task2(self):
        query = "SELECT COUNT(id)/COUNT(DISTINCT user_id) FROM Activity"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 2: \n")
        print(tabulate(result, headers=["Average number of activities per user"]))

    def part2_task3(self):
        query = "SELECT COUNT(id), user_id FROM Activity GROUP BY user_id ORDER BY COUNT(id) DESC LIMIT 20"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 3:\nTop 20 users with the highest number of activities in descending order.")
        print(tabulate(result, headers=["Number of activites", "User id"]))


    def part2_task4(self):
        query = 'SELECT DISTINCT user_id FROM Activity WHERE transportation_mode = "taxi"'
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 4:\n")
        print(tabulate(result, headers=["Users who have taken taxi:"]))


    # Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. 
    # Do not count the rows where the mode is null.
    def part2_task5(self):
        query = """
                    SELECT transportation_mode,COUNT(*) AS countedActivity
                    FROM Activity 
                    WHERE transportation_mode IS NOT NULL
                    GROUP BY transportation_mode
                    ORDER BY countedActivity desc
                 """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 5:\n")
        print(tabulate(result, headers=["Transportation mode", "Number of activities tagged with each transportation mode:"]))
    
    def part2_task6(self):
        # a) Find the year with the most activities. 
        # Note: We chose to place activities into years based on start_date_time. 
        # This is relevant because some activities are on New Years' eve 
        query_a =   """
                        SELECT YEAR(start_date_time) AS year, COUNT(*) as activityCount
                        FROM Activity
                        GROUP BY year
                        ORDER BY activityCount desc
                        LIMIT 1
                    """

        self.cursor.execute(query_a)
        result_a = self.cursor.fetchall()
        print("\n---\nPart 2, task 6a: \n")
        print("The year with most activities is", result_a[0][0], "with", result_a[0][1], "activities")

        # b) Is this also the year with most recorded hours?
        query_b =   """
                        SELECT YEAR(start_date_time) AS year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS hoursCount
                        FROM Activity
                        GROUP BY year
                        ORDER BY hoursCount desc
                        LIMIT 1
                    """
            
        self.cursor.execute(query_b)
        print("\n---\nPart 2, task 6b: \n")
        result_b = self.cursor.fetchall()
        print("The year with the most recorded hours is not 2008, but", result_b[0][0], "with", result_b[0][1], "hours recorded.")

    # Find the total distance (in km) walked in 2008, by user with id=112
    def part2_task7(self):
        query = """
                    SELECT lat, lon 
                    FROM TrackPoint
                    JOIN Activity ON Activity.id = TrackPoint.activity_id 
                    WHERE transportation_mode = "walk" AND YEAR(start_date_time) = "2008" AND user_id = 112
                """

        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 7: \n")
        #print(tabulate(result, headers=["Trackpoint latitude", "Trackpoint longitude"]))

        totalDistance = 0
        for trackpoint in range(0, len(result)-1):
            fromLoc = (result[trackpoint][0], result[trackpoint][1])
            toLoc = (result[trackpoint + 1][0], result[trackpoint + 1][1])
            totalDistance += haversine(fromLoc, toLoc) 
        print("User with id=112 walked", round(totalDistance), 'km in 2008')

    def part2_task8(self):
        """
        Find the top 20 users who have gained the most altitude in meters.
        Ignores invalid altitude values (-777).
        Only sums altitudes if trackpoint_n.altitude > trackpoint_n-1.altitude
        """

        query = """
                SELECT SubTable.user_id, SubTable.gained_altitude
                FROM (
                    SELECT Activity.user_id,
                    SUM(
                        CASE WHEN 
                            TP1.altitude NOT LIKE '%-777' AND
                            TP2.altitude NOT LIKE '%-777'
                        THEN (TP1.altitude - TP2.altitude) * 0.0003048
                        ELSE 0
                        END)
                        AS gained_altitude
                    FROM TrackPoint AS TP1 
                    INNER JOIN TrackPoint AS TP2 ON TP1.activity_id = TP2.activity_id AND TP1.id+1 = TP2.id
                    INNER JOIN Activity ON Activity.id = TP1.activity_id AND TP2.activity_id
                    WHERE TP1.altitude > TP2.altitude
                    GROUP BY Activity.user_id
                ) AS SubTable
                ORDER BY gained_altitude DESC
                LIMIT 20
                """
        
        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("\n---\nPart 2, Task 8:\n")
        print(tabulate(result, headers=["Id", "Total meters gained per user"]))

    # Find all users who have invalid activities, and the number of invalid activities per user.
    def part2_task9(self):
        query = '''
                    SELECT Activity.user_id, COUNT(DISTINCT(Activity.id))
                    FROM Activity
                    INNER JOIN TrackPoint as TP1 ON TP1.activity_id = Activity.id
                    INNER JOIN TrackPoint as TP2 ON TP2.activity_id = Activity.id AND TP1.id+1 = TP2.id
                    WHERE TIMESTAMPDIFF(MINUTE, TP1.date_time, TP2.date_time) > 5 OR TIMESTAMPDIFF(MINUTE, TP2.date_time, TP1.date_time) > 5
                    GROUP BY Activity.user_id
                '''
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 9:\n")
        print(tabulate(result, headers=["User id", "Number of invalid activities"]))

    def part2_task10(self):
        query = "SELECT DISTINCT Activity.user_id FROM TrackPoint INNER JOIN Activity ON TrackPoint.activity_id = Activity.id WHERE TrackPoint.lat BETWEEN 39.915 AND 39.917 AND TrackPoint.lon BETWEEN 116.396 AND 116.398"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 10: \nUsers who have tracked activity in the Forbidden City of Beijing.")
        print(tabulate(result, headers=["User id"]))

    def part2_task11(self):
        print("\n---\nPart 2, task 11: \nAll users with transportation_mode and their most used one")
        query = "WITH cte AS (SELECT user_id, transportation_mode, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY COUNT(*) DESC) AS RowNumber, COUNT(*) AS NumberOfActivities FROM Activity WHERE transportation_mode IS NOT NULL GROUP BY user_id, transportation_mode) SELECT user_id, transportation_mode FROM cte WHERE RowNumber = 1"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=["User id", "Most used transportation"]))



def main():
    program = None
    try:
        # Run queries:
        
        program = MainProgram()
        program.part2_task1()        
        program.part2_task2()
        program.part2_task3()
        program.part2_task4()
        program.part2_task5()
        program.part2_task6()
        program.part2_task7()
        program.part2_task8()
        program.part2_task9()
        program.part2_task10()
        program.part2_task11()

        # Create DB tables:

        # program.create_table(
        #     table_name="User",
        #     fields="""
        #     id VARCHAR(255) NOT NULL PRIMARY KEY,
        #     has_labels BIT NOT NULL
        #     """
        # )
        # program.create_table(
        #     table_name="Activity",
        #     fields="""
        #     id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
        #     user_id VARCHAR(255) NOT NULL,
        #     FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE,
        #     transportation_mode VARCHAR(255),
        #     start_date_time DATETIME,
        #     end_date_time DATETIME
        #     """
        # )
        # program.create_table(
        #     table_name="TrackPoint",
        #     fields="""
        #     id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
        #     activity_id INT NOT NULL,
        #     FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE,
        #     lat DOUBLE,
        #     lon DOUBLE,
        #     altitude INT,
        #     date_days DOUBLE,
        #     date_time DATETIME
        #     """
        # )

        # Insert data to the database
        # program.insert_dataset(config('DATASET_PATH'))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
