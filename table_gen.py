import mysql.connector
from faker import Faker
import random

fake = Faker()

# Connect to MySQL server
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="MySQL_Student123"  
)

cursor = conn.cursor()

# Create database
cursor.execute("CREATE DATABASE IF NOT EXISTS university_db")
cursor.execute("USE university_db")


cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

tables = ["enrollments", "course_assignments", "students", "courses", "professors"]
for table in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {table}")

cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

# Create tables
cursor.execute("""
CREATE TABLE IF NOT EXISTS students (
    student_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    major VARCHAR(50),
    year_enrolled INT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS courses (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(100),
    department VARCHAR(50),
    credits INT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS enrollments (
    enrollment_id INT PRIMARY KEY,
    student_id INT,
    course_id INT,
    semester VARCHAR(20),
    grade INT,
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (course_id) REFERENCES courses(course_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS professors (
    professor_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    email VARCHAR(100)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS course_assignments (
    assignment_id INT PRIMARY KEY,
    course_id INT,
    professor_id INT,
    semester VARCHAR(20),
    FOREIGN KEY (course_id) REFERENCES courses(course_id),
    FOREIGN KEY (professor_id) REFERENCES professors(professor_id)
)
""")


conn.commit()

# Random data generators
majors = ["Computer Science", "Economics", "Mathematics", "Physics", "Literature", "Data Science"]
departments = ["Computer Science", "Economics", "Mathematics", "Physics", "Literature"]
grades = [5, "A-", "B+", "B", "B-", "C+", "C"]
season = ["Fall 2023", "Spring 2024", "Fall 2024"]
course_names = ["Intro to Programming", "Data Structures", "Algorithms", "Calculus I", "Calculus II", 
                "Mechanics", "Modern Literature", "Economics 101", "Physics Lab", "AI Basics"]

# Students
students_data = []
for i in range(1, 150):  # 10 students
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{first_name.lower()}.{last_name.lower()}@uni.edu"
    major = random.choice(majors)
    year_enrolled = random.randint(2019, 2023)
    students_data.append((i, first_name, last_name, email, major, year_enrolled))

# Courses
courses_data = []
for i, name in enumerate(course_names, start=101):
    department = random.choice(departments)
    credits = random.randint(3, 5)
    courses_data.append((i, name, department, credits))

# Professors
professors_data = []
for i in range(1, 10):  # 10 professors
    first_name = fake.first_name()
    last_name = fake.last_name()
    department = random.choice(departments)
    email = f"{first_name.lower()}.{last_name.lower()}@uni.edu"
    professors_data.append((i, first_name, last_name, department, email))

# Enrollments
enrollments_data = []
enrollment_id = 1
for student_id in range(1, 50):
    course_ids = random.sample([c[0] for c in courses_data], 3)  # 3 random courses per student
    for cid in course_ids:
        semester = random.choice(season)
        grade = random.randint(40, 100)
        enrollments_data.append((enrollment_id, student_id, cid, semester, grade))
        enrollment_id += 1

# Course assignments
course_assignments_data = []
assignment_id = 1
for course in courses_data:
    professor_id = random.choice([p[0] for p in professors_data])
    semester = random.choice(season)
    course_assignments_data.append((assignment_id, course[0], professor_id, semester))
    assignment_id += 1

# Helper function
def insert_data(table, data):
    placeholders = ', '.join(['%s'] * len(data[0]))
    query = f"INSERT INTO {table} VALUES ({placeholders})"
    cursor.executemany(query, data)
    conn.commit()
    print(f"{cursor.rowcount} rows inserted into {table}")

# Insert data
insert_data("students", students_data)
insert_data("courses", courses_data)
insert_data("professors", professors_data)
insert_data("enrollments", enrollments_data)
insert_data("course_assignments", course_assignments_data)

cursor.close()
conn.close()
print("Random database setup complete!")
