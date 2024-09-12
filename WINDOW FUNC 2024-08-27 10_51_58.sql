-- Databricks notebook source
-- Create the Students table
CREATE TABLE Students (
    StudentID INT,
    StudentName STRING,
    Class CHAR(1),
    ExamDate DATE,
    Score INT
) USING DELTA;

-- COMMAND ----------

CREATE TABLE Subjects (
    SubjectID INT,
    StudentID INT,
    SubjectName STRING,
    TeacherName STRING
) USING DELTA;

-- COMMAND ----------

INSERT INTO Students (StudentID, StudentName, Class, ExamDate, Score) VALUES
(1, 'Alice', 'A', '2024-06-01', 85),
(2, 'Bob', 'A', '2024-06-02', 90),
(3, 'Charlie', 'A', '2024-06-03', 75),
(4, 'David', 'B', '2024-06-01', 88),
(5, 'Eve', 'B', '2024-06-02', 92),
(6, 'Frank', 'B', '2024-06-03', 80),
(7, 'Grace', 'A', '2024-06-04', 95),
(8, 'Hannah', 'B', '2024-06-04', 83),
(9,  'Ivy', 'A', '2024-06-05', 82),
(10, 'Jack', 'B', '2024-06-05', 87),
(11, 'Katherine', 'A', '2024-06-06', 90),
(12, 'Leo', 'B', '2024-06-06', 85);

-- COMMAND ----------

SELECT * FROM Students

-- COMMAND ----------

INSERT INTO Subjects (SubjectID, StudentID, SubjectName, TeacherName) VALUES
(1, 1, 'Mathematics', 'Mr. Smith'),
(2, 2, 'Science', 'Mrs. Johnson'),
(3, 3, 'History', 'Mr. Brown'),
(4, 4, 'Mathematics', 'Mr. Smith'),
(5, 5, 'Science', 'Mrs. Johnson'),
(6, 6, 'History', 'Mr. Brown'),
(7, 7, 'Mathematics', 'Mr. Smith'),
(8,  8, 'Science', 'Mrs. Johnson'),
(9,  9, 'History', 'Mr. Brown'),
(10, 10, 'Mathematics', 'Mr. Smith');

-- COMMAND ----------

SELECT * FROM subjects;

-- COMMAND ----------

SELECT
    StudentID,
    StudentName,
    Class,
    ExamDate,
    Score,

    -- ROW_NUMBER assigns a unique number to each row within the partition of a result set.
    ROW_NUMBER() OVER (PARTITION BY Class ORDER BY ExamDate) AS RowNum
FROM Students;

-- COMMAND ----------

SELECT StudentID, StudentName, Class, ExamDate, Score,
       RANK() OVER (PARTITION BY Class ORDER BY Score DESC) AS Rank
FROM Students;

-- COMMAND ----------

SELECT StudentID, StudentName, Class, ExamDate, Score,
       DENSE_RANK() OVER (PARTITION BY Class ORDER BY Score DESC) AS DenseRank
FROM Students;

-- COMMAND ----------

SELECT StudentID, StudentName, Class, ExamDate, Score,
       PERCENT_RANK() OVER (PARTITION BY Class ORDER BY Score DESC) AS PercentileRank
FROM Students;

-- COMMAND ----------

SELECT StudentID, StudentName, Class, ExamDate, Score,
       NTILE(4) OVER (PARTITION BY Class ORDER BY Score DESC) AS Quartile
FROM Students;

-- COMMAND ----------



SELECT StudentID, StudentName, Class, ExamDate, Score,
       LEAD(Score) OVER (PARTITION BY Class ORDER BY ExamDate) AS NextScore
FROM Students;

-- COMMAND ----------

SELECT StudentID, StudentName, Class, ExamDate, Score,
       LAG(Score) OVER (PARTITION BY Class ORDER BY ExamDate) AS PreviousScore
FROM Students;

-- COMMAND ----------

SELECT StudentID, StudentName, Class, ExamDate, Score,
       MIN(Score) OVER (PARTITION BY Class) AS MinScore,
       MAX(Score) OVER (PARTITION BY Class) AS MaxScore
FROM Students;

-- COMMAND ----------

SELECT StudentID, StudentName, Class, ExamDate, Score,
       AVG(Score) OVER (PARTITION BY Class) AS AvgScore,
       SUM(Score) OVER (PARTITION BY Class) AS TotalScore
FROM Students;

-- COMMAND ----------

SELECT StudentID, StudentName, Class, ExamDate, Score,
       FIRST_VALUE(Score) OVER (PARTITION BY Class ORDER BY ExamDate) AS FirstScore,
       LAST_VALUE(Score) OVER (PARTITION BY Class ORDER BY ExamDate) AS LastScore
FROM Students;

-- COMMAND ----------

SELECT
  s.StudentName,
  sub.SubjectName
FROM Students s
JOIN Subjects sub ON s.StudentID = sub.StudentID;

-- COMMAND ----------

SELECT
  s.StudentName,
  sub.SubjectName
FROM Students s
RIGHT JOIN Subjects sub ON s.StudentID = sub.StudentID;

-- COMMAND ----------

SELECT
  s.StudentName,
  sub.SubjectName
FROM Students s
LEFT JOIN Subjects sub ON s.StudentID = sub.StudentID;

-- COMMAND ----------

SELECT
  s.StudentName,
  sub.SubjectName
FROM Students s
FULL OUTER JOIN Subjects sub ON s.StudentID = sub.StudentID;
