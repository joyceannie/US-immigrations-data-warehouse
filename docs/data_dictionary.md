# Data Dictionary

## dim_ports

|Field|Type|Description|
|-----|----|-----------|
|port_id|int8|Primary Key|
|port_code|varchar(3) not null|3 character code used for I94 ports|
|port_state|varchar(50)|U.S. state of port|
|average_temperature|numeric(16,3)|Average temperature of port city|

## dim_countries

|Field|Type|Description|
|-----|----|-----------|
|country_id|int8|Primary Key|
|country_code|varchar(3) not null|3 character code used for country|
|avg_temperature|numeric(16, 3)|average temperature of the country|

## dim_time

|Field|Type|Description|
|-----|----|-----------|
|timestamp|int4|SAS timestamp (days since 1/1/1960)|
|year|int4|year in 4 digits|
|month|int4|month(1 to 12)|
|day|int4|day of month(1 to 31)|
|week|int4|week of the year(1 to 52)|
|day_of_week|int4|day of the week (1 to 7 starting from Sunday)|
|quarter|int4|quarter of the year (1 to 4)|

## dim_demographics
|Field|Type|Description|
|-----|----|-----------|
|demographics_id|int8|Primary Key|
|port_id|int8|Foreign key to dim_ports|
|median_age|numeric(18,2)|The median age for the demographic|
|male_population|int4|Count of male population in the city|
|female_population|int4|Count of female population in the city|
|total_population|int8|Population of the city|
|num_of_veterans|int4|Count of veterans in the city|
|foreign_born|int4|Count of foreign born persons|
|avg_household_size|numeric(18,2)|Avg household size of the city|
|race|varchar(100)|Race of the demographic|
|demo_count|int4|Count of the demographic


## dim_airports
|Field|Type|Description|
|-----|----|-----------|
|airport_id|int8|Primary Key|
|port_id|int4|Foreign key to dim_ports|
|airport_type|varchar(256)|Short description of airport type|
|airport_name|Name of the airport||
|elevation_ft|int4|Airport elevation|
|municipality|varchar(256)|Airport municipality|
|gps_code|varchar(256)|Airport GPS code|
|iata_code|varchar(256)|Airport International Air Transport Association code|
|local_code|varchar(256)|Airport local code|
|coordinates|varchar(256)|Airport Coordinates|

## fact_immigration
|Field|Type|Description|
|-----|----|-----------|
|immigration_id|int8|Primary Key|
|country_id|int8|Foreign key to dim_countries|
|port_id|int8|Foreign key to dim_ports|
|age|int4|Age of the immigrant|
|travel_mode|varchar(50)|Mode of travel(air, land etc)|
|visa_category|varchar(50)|Category of visa|
|visa_type|varchar(50)|Type of visa|
|gender|varchar(15)|Gender of the immigrant|
|birth_year|int4|Birth year of the immigrant|
|arrival_date|int4|SAS timestamp of arrival date, Foreign key to dim_time|
|departure_date|int4|SAS timestamp of departure date, Foreign key to dim_time|










