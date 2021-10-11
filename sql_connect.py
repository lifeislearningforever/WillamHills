import mysql.connector as mysql

db = mysql.connect(
    host = "localhost",
    user = "root",
    passwd = "0987!Adhira",
    database = "InterviewQuestions"
)

cursor = db.cursor()


cursor.execute("SHOW TABLES")

tables = cursor.fetchall()

for table in tables:
    print(table)