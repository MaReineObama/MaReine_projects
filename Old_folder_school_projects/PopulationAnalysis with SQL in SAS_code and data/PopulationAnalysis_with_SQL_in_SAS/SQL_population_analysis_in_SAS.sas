libname barc "C:\Users\mobama\OneDrive - IESEG\Business reporting tools\BRT_retake_data";

/**** Number of accidents per month****/
PROC SQL;
CREATE TABLE barc.Accidents_per_month AS
SELECT Month, count(Id) AS Nbr_of_accidents
FROM accidents_2017
GROUP BY 1
ORDER BY 2 DESC;
QUIT;

/*****Weekday with the highest number of accident*****/

PROC SQL;
CREATE TABLE barc.Weekday_Max_accident AS
SELECT Weekday, count(Id) AS Nbr_of_accidents
FROM accidents_2017
GROUP BY 1
ORDER BY 2 DESC;
QUIT;

/*****Moment of the days, number of accidents and number of victims
where serious injuries were less than any accidents 
with mild injuries****/

PROC SQL;
CREATE TABLE barc.less_serious_injuries_death AS
SELECT DISTINCT Part_of_the_day, COUNT(Id) as Accidents, SUM(Victims) as Victims
FROM accidents_2017
WHERE Serious_Injuries > 0
GROUP BY 1
HAVING COUNT(Id) < ANY (SELECT COUNT(Id)
						FROM accidents_2017
						WHERE Mild_Injuries > 0);
RUN;

/****Average number of deaths per age group due to an accident 
during which there were more than 2 vehicles involved***/

PROC SQL;
CREATE TABLE barc.Avg_accident_death_with2vehicles AS
SELECT DISTINCT d.Age, AVG(d.Number) as Avg_Deaths
FROM deaths AS d
INNER JOIN accidents_2017 as a
ON a.District_Name = d.District_Name
WHERE a.Vehicles_involved > 2
GROUP BY 1
ORDER BY 2 DESC;
RUN;


/*******number of distinct gender born in a station place***/
PROC SQL;
CREATE TABLE barc.Gender_station AS
SELECT distinct b.gender, sum(b.Number) as number
FROM births as b, air_stations_Nov2017 as a
WHERE b.district_name = a.district_name
AND b.neighborhood_name = a.neighborhood_name
GROUP BY 1
ORDER BY 2 DESC;
RUN;


/****... Districts with big number of immigrants that 
belong to districts with big number of deaths***/

PROC SQL;
CREATE TABLE barc.District_imm_death AS
SELECT a.District_Name, b.Immigrants, b.Deaths /*Then I inner join to get all 3 columns*/
FROM 
	(SELECT District_Name, /* I could not select District_Name from table b cause there was an issue with the outputted columns named apparently not easy to rename, then appeared with a space. Therefore I has to inner join again to get the column name properly wirtted*/
		SUM(Number) as Immigrants /* I select only District_Name to be able to select it on the top*/
		FROM immigrants_by_nationality
		WHERE Number > 1000
		GROUP BY 1) as a
INNER JOIN 
	(SELECT Immigrants, Deaths /* Here I only select Number of Immigrants  and deaths created from table i and d */
		FROM 
		(SELECT District_Name AS District2, /* Number of immigrants greate than 1000 as table i*/
		SUM(Number) as Immigrants
		FROM immigrants_by_nationality
		WHERE Number > 1000
		GROUP BY 1) as i,
		(SELECT District_Name AS District1, /* Joined with number with Number of deaths greater than 100 as table d*/
		SUM(Number) as Deaths
		FROM deaths 
		WHERE Number > 100
		GROUP BY 1) as d
		WHERE d.District1 = i.District2) as b /* table i and d joined to create table b in the final inner join*/
ON a.Immigrants = b.Immigrants ;
RUN;

/****What gender is more unemployed****/
PROC SQL;
CREATE TABLE barc.unemployed_gender AS
SELECT DISTINCT Gender, SUM(Number) as Count
FROM unemployment
GROUP BY 1;
RUN;

/****Sum of baby's names frequency popular before 1930****/
PROC SQL;
CREATE TABLE barc.baby_names_in1930 AS
SELECT DISTINCT Name, Gender, SUM(Frequency) AS Frequency_sum
FROM most_frequent_baby_names
WHERE Name IN
			(SELECT Name
			FROM most_frequent_names
			WHERE Decade CONTAINS 'Before')
GROUP BY 1
ORDER BY 2 DESC;
RUN;

/*** Number of Neighborhood name per District with underground transport type***/
PROC SQL;
CREATE TABLE barc.Neighb_per_dist_undergroud AS
SELECT DISTINCT District_Name, COUNT(Neighborhood_Name) AS Nbr_of_Neighborhood
FROM population
WHERE EXISTS
		(SELECT *
		FROM transports
		WHERE transports.District_Name = population.District_Name
		AND Transport LIKE '%Underground%')
GROUP BY 1;
RUN;

/***List of distinct transport type and their number*/
PROC SQL;
CREATE TABLE barc.Tranport_count AS
SELECT Transport, COUNT(Transport) AS Count
FROM transports
GROUP BY 1
ORDER BY 2 ASC;
RUN;

/*List of air stations that are also present in the list of air quality***/
PROC SQL;
CREATE TABLE barc.air_station_quality AS
SELECT *
FROM ((SELECT Station, Longitude, Latitude FROM air_quality_Nov2017)
INTERSECT
(SELECT Station, Longitude, Latitude FROM air_stations_Nov2017));
RUN;

