-- Databricks notebook source

-- Create a table with custom date data
CREATE OR REPLACE TEMPORARY VIEW date_input AS
SELECT '2023-09-02' AS input_date UNION ALL
SELECT '2022-06-15' AS input_date UNION ALL
SELECT '2023-01-10' AS input_date UNION ALL
SELECT '2022-12-25' AS input_date;

-- View the data
SELECT * FROM date_input;


-- COMMAND ----------

-- Convert the input dates from string to date type
CREATE OR REPLACE TEMP VIEW date_converted AS
SELECT
    CAST(input_date AS DATE) AS input_date
FROM
    date_input;

-- View the converted data
SELECT * FROM date_converted;


-- COMMAND ----------

-- Step 3: Subtract one year (12 months) from each date
CREATE OR REPLACE TEMP VIEW date_subtracted AS
SELECT
    input_date,
    ADD_MONTHS(input_date, -12) AS one_year_back
FROM
    date_converted;

-- View the data with one year subtracted
SELECT * FROM date_subtracted;


-- COMMAND ----------

-- Extract the year and date components
CREATE OR REPLACE TEMP VIEW date_components AS
SELECT
    input_date,
    one_year_back,
    YEAR(one_year_back) AS year_component,
    DATE_FORMAT(one_year_back, 'MM-dd') AS date_component
FROM
    date_subtracted;

-- View the extracted components
SELECT * FROM date_components;


-- COMMAND ----------

-- Step 1: Create a table with custom date data
CREATE OR REPLACE TEMPORARY VIEW date_input AS
SELECT '2023-09-02' AS input_date UNION ALL
SELECT '2022-06-15' AS input_date UNION ALL
SELECT '2023-01-10' AS input_date UNION ALL
SELECT '2022-12-25' AS input_date;

-- View the data
SELECT * FROM date_input;

-- Step 2: Convert the input dates from string to date type
CREATE OR REPLACE TEMP VIEW date_converted AS
SELECT
    CAST(input_date AS DATE) AS input_date
FROM
    date_input;

-- View the converted data
SELECT * FROM date_converted;

-- Step 3: Subtract one year (12 months) from each date
CREATE OR REPLACE TEMP VIEW date_subtracted AS
SELECT
    input_date,
    ADD_MONTHS(input_date, -12) AS one_year_back
FROM
    date_converted;

-- View the data with one year subtracted
SELECT * FROM date_subtracted;

-- Step 4: Extract the year and date components
CREATE OR REPLACE TEMP VIEW date_components AS
SELECT
    input_date,
    one_year_back,
    YEAR(one_year_back) AS year_component,
    DATE_FORMAT(one_year_back, 'MM-dd') AS date_component
FROM
    date_subtracted;

-- View the extracted components
SELECT * FROM date_components;

