'''
Spark SQL Analysis
'''

USE f1_processed;

CREATE TABLE f1_presentation.calculates_race_results
USING parquet
AS
SELECT races.race_year,
       constructors.name AS team_name
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
FROM results
JOIN drivers ON (results.driver_id = drivers.driver_id)
JOIN constructors ON (results.constructor_id = constructors.constructor_id)
JOIN races ON (results.race_id = races.race_id)
WHERE results.position <= 10


#Dominant Drivers Analysis
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY average_points DESC

#Dominabt Teams Analysis
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2010
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY average_points DESC

#Dominant Drivers - Visualization
CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS average_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY race_year, driver_name
HAVING COUNT(1) >= 50
ORDER BY average_points DESC



SELECT race_year,
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY average_points DESC