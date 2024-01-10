-- Commands to create the database and Create the Views related to the business
CREATE database company;
USE company;

CREATE TABLE hiredEmployee(
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    datetime VARCHAR(255) NOT NULL,
    department_id INTEGER NOT NULL,
    job_id INTEGER NOT NULL
);

CREATE TABLE job (
    id INTEGER PRIMARY KEY,
    job VARCHAR(255)
);

CREATE TABLE department (
    id INTEGER PRIMARY KEY,
    department VARCHAR(255)
);
-- QUARTER VIEWS
CREATE VIEW employees_department AS
	WITH RECURSIVE department_job AS (

	SELECT d.department, j.job,QUARTER(STR_TO_DATE(h.datetime,'%Y-%m-%dT%H:%i:%s')) AS quarter 
	FROM department d 
	INNER JOIN hiredEmployee h ON d.id = h.department_id
	INNER JOIN job j ON j.id = h.job_id 
	where h.datetime LIKE '2021-%'
	)

	SELECT department, job,
       	SUM(CASE WHEN quarter = 1 THEN 1 ELSE 0 END) AS Q1,
       	SUM(CASE WHEN quarter = 2 THEN 1 ELSE 0 END) AS Q2,
       	SUM(CASE WHEN quarter = 3 THEN 1 ELSE 0 END) AS Q3,
       	SUM(CASE WHEN quarter = 4 THEN 1 ELSE 0 END) AS Q4
	FROM department_job
	GROUP BY department,job
	ORDER BY department, job ASC;
-- DEPARTMENT HIRE
CREATE VIEW department_hire AS 
    SELECT  d.id,d.department,COUNT(*) AS hired 
    FROM hiredEmployee h
    INNER JOIN department d
    ON h.department_id = d.id  
    GROUP BY d.id 
    HAVING COUNT(*) > (     
    SELECT AVG(employees)
    FROM ( 
    SELECT COUNT(*) AS employees         
    FROM hiredEmployee         
    WHERE YEAR(datetime) = 2021         
    GROUP BY department_id ) AS avg_counts ) 
    ORDER BY hired DESC;