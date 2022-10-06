from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine


class Queries:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.db_connection.autocommit = False
        self.cursor = self.connection.cursor

    def task_1(self):
        """
        Find the amount of users, activities and trackpoints in the dataset,
        after insertion to database.
        """

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

        
    def task_2(self):
        """
        Find the average number of activities per user.
        """

        query = "SELECT COUNT(id)/COUNT(DISTINCT user_id) FROM Activity"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 2: \n")
        print(tabulate(result, headers=["Average number of activities per user"]))

    def task_3(self):
        """
        Find the top 20 users with the highest number of activities.
        """

        query = "SELECT COUNT(id), user_id FROM Activity GROUP BY user_id ORDER BY COUNT(id) DESC LIMIT 20"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 3:\nTop 20 users with the highest number of activities in descending order.")
        print(tabulate(result, headers=["Number of activites", "User id"]))


    def task_4(self):
        """
        Find all users who have taken a taxi.
        """

        query = 'SELECT DISTINCT user_id FROM Activity WHERE transportation_mode = "taxi"'
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 4:\n")
        print(tabulate(result, headers=["Users who have taken taxi:"]))


    def task_5(self):
        """
        Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. 
        Do not count the rows where the mode is null.
        """

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
    
    def task_6(self):
        """
        Find the year with the most activities,
        and check if it is also the year with most recorded hours.
        """

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
        print("The year with the most recorded hours is not", result_a[0][0] ,"but", result_b[0][0], "with", result_b[0][1], "hours recorded.")

    def task_7(self):
        """
        Find the total distance (in km) walked in 2008, by user with id=112.
        """

        query = """
                    SELECT lat, lon 
                    FROM TrackPoint
                    JOIN Activity ON Activity.id = TrackPoint.activity_id 
                    WHERE transportation_mode = "walk" AND YEAR(start_date_time) = "2008" AND user_id = 112
                """

        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 7: \n")

        totalDistance = 0
        for trackpoint in range(0, len(result)-1):
            fromLoc = (result[trackpoint][0], result[trackpoint][1])
            toLoc = (result[trackpoint + 1][0], result[trackpoint + 1][1])
            totalDistance += haversine(fromLoc, toLoc) 
        print("User with id=112 walked", round(totalDistance), 'km in 2008')

    def task_8(self):
        """
        Find the top 20 users who have gained the most altitude in meters.
        Ignore invalid altitude values (-777).
        Only sum altitudes if trackpoint_n.altitude > trackpoint_n-1.altitude
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

    def task_9(self):
        """
        Find all users who have invalid activities, and the number of invalid activities per user.
        """
        query = """
                    SELECT Activity.user_id, COUNT(DISTINCT(Activity.id))
                    FROM Activity
                    INNER JOIN TrackPoint as TP1 ON TP1.activity_id = Activity.id
                    INNER JOIN TrackPoint as TP2 ON TP2.activity_id = Activity.id AND TP1.id+1 = TP2.id
                    WHERE TIMESTAMPDIFF(MINUTE, TP1.date_time, TP2.date_time) > 5 OR TIMESTAMPDIFF(MINUTE, TP2.date_time, TP1.date_time) > 5
                    GROUP BY Activity.user_id
                """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 9:\n")
        print(tabulate(result, headers=["User id", "Number of invalid activities"]))

    def task_10(self):
        """
        Find the users who have tracked an activity in the Forbidden City of Beijing.
        Lat: 39.916
        Lon: 116.397
        """

        query = "SELECT DISTINCT Activity.user_id FROM TrackPoint INNER JOIN Activity ON TrackPoint.activity_id = Activity.id WHERE TrackPoint.lat BETWEEN 39.915 AND 39.917 AND TrackPoint.lon BETWEEN 116.396 AND 116.398"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print("\n---\nPart 2, task 10: \nUsers who have tracked activity in the Forbidden City of Beijing.")
        print(tabulate(result, headers=["User id"]))

    def task_11(self):
        """
        Find all users who have registered transportation_mode and their most used transportation mode.
        Do not count rows where mode is NULL.
        """

        print("\n---\nPart 2, task 11: \nAll users with transportation_mode and their most used one")
        query = "WITH cte AS (SELECT user_id, transportation_mode, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY COUNT(*) DESC) AS RowNumber, COUNT(*) AS NumberOfActivities FROM Activity WHERE transportation_mode IS NOT NULL GROUP BY user_id, transportation_mode) SELECT user_id, transportation_mode FROM cte WHERE RowNumber = 1"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=["User id", "Most used transportation"]))



def main():
    queries = None
    try:
        # Run queries:
        
        queries = Queries()
        queries.task_1()        
        queries.task_2()
        queries.task_3()
        queries.task_4()
        queries.task_5()
        queries.task_6()
        queries.task_7()
        queries.task_8()
        queries.task_9()
        queries.task_10()
        queries.task_11()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if queries:
            queries.connection.close_connection()


if __name__ == '__main__':
    main()
