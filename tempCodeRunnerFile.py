

# '''
# SELECT activity.user_id, COUNT(DISTINCT(activity.id)) AS Invalid 
# FROM activity 
# JOIN activity AS a2 ON activity.user_id = a2.user_id 
# JOIN trackpoint AS t1 ON t1.activity_id = activity.id 
# JOIN trackpoint AS t2 ON t1.id = t2.id - 1 
# WHERE t1.activity_id = t2.activity_id AND timestampdiff(SECOND, t1.date_time, t2.date_time) >= 300 
# GROUP BY activity.user_id ORDER BY COUNT(DISTINCT activity.id) DESC
# ''' 