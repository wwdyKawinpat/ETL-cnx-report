create table  IF NOT EXISTS cnx_report(
	name VARCHAR (50) NOT NULL,
    weather_main VARCHAR (50) NOT NULL,
    sunrise_date DATE,
    sunrise_time TIMESTAMP ,
    sunset_date DATE,
    sunset_time TIMESTAMP,
    temp numeric NOT NULL,
    temp_feellike numeric NOT NULL,
    temp_max numeric NOT NULL,
    temp_min numeric NOT NULL,
    aqi numeric NOT NULL,
    population numeric NOT null);