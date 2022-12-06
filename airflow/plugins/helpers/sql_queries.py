class SqlQueries:
    """SQL queries used to create schema and extract data. 
    The data model diagram used for cresting the schema is available at docs/images/data_model.png"""
    
    # Create city temperatures staging table
    create_staging_city_temps_table = """
    DROP TABLE IF EXISTS public.staging_city_temperatures;
    CREATE TABLE public.staging_city_temperatures
    (
    dt date,
    "AverageTemperature" double precision,
    "AverageTemperatureUncertainty" double precision,
    "City" text,
    "Country" text,
    "Latitude" double precision,
    "Longitude" double precision
    );
    """
    
    # Create country temperatures staging table
    create_staging_country_temps_table = """
    DROP TABLE IF EXISTS public.staging_country_temperatures;
    CREATE TABLE public.staging_country_temperatures
    (
    dt date,
    "AverageTemperature" double precision,
    "AverageTemperatureUncertainty" double precision,
    "Country" text
    );
    """       

    # Create staging immigration table
    create_staging_immigration_table = """    
    DROP TABLE IF EXISTS public.staging_immigration;
    CREATE TABLE public.staging_immigration
    (    
    country_code double precision,
    port_code text,
    age double precision, 
    travel_mode    text,
    visa_category text, 
    visa_type text,
    gender text,
    birth_year double precision,
    arrival_date date,
    departure_date date       
    )
    """

    # Create Immigration table on Redhsift
    create_fact_immigration_table = """
    DROP TABLE IF EXISTS public.fact_immigration CASCADE;
    CREATE TABLE public.fact_immigration
    (
    immigration_id BIGINT GENERATED ALWAYS AS IDENTITY,
    country_code INT,
    port_code VARCHAR(3),
    state_code VARCHAR(50),
    city VARCHAR(256),
    age INT,
    gender VARCHAR(10),
    travel_mode VARCHAR(100),
    visa_category VARCHAR(100),
    visa_type VARCHAR(100),
    birth_year INT,
    arrival_date DATE,
    departure_date DATE,
    PRIMARY KEY(immigration_id),
    CONSTRAINT fk_port FOREIGN KEY(port_code) REFERENCES dim_ports(port_code),
    CONSTRAINT fk_country FOREIGN KEY(country_code) REFERENCES dim_countries(country_code),
    CONSTRAINT fk_arrdate FOREIGN KEY(arrival_date) REFERENCES dim_time(sas_timestamp),
    CONSTRAINT fk_depdate FOREIGN KEY(departure_date) REFERENCES dim_time(sas_timestamp),
    CONSTRAINT fk_demographics FOREIGN KEY(city, state_code) REFERENCES dim_demographics(city, state)
    
    );
    """

    immigration_table_insert = ("""
    INSERT INTO public.fact_immigration(country_code, port_code, state_code, city, age, gender, travel_mode, visa_category, visa_type, birth_year, arrival_date, departure_date) 
    SELECT i.country_code, i.port_code, p.state , p.city, i.age, i.gender, i.travel_mode, i.visa_category, i.visa_type, i.birth_year, i.arrival_date, i.departure_date    
    FROM public.staging_immigration i
    LEFT JOIN public.staging_ports p
    ON i.port_code = p.port_code;
    """)
    
    # Create Countries staging table
    create_staging_countries_table = """
    DROP TABLE IF EXISTS public.staging_countries;
    CREATE TABLE public.staging_countries (
    country_code int NOT NULL,
    country varchar(256) NOT NULL
    );
    """
    # Create Countries table on Redshift
    create_countries_table = """
    DROP TABLE IF EXISTS public.dim_countries CASCADE;
    CREATE TABLE public.dim_countries
    (
    country_code INT NOT NULL UNIQUE,
    country varchar(256) NOT NULL UNIQUE,
    average_temperature NUMERIC(16,3) NULL,
    PRIMARY KEY(country_code)
    );
    """

    countries_table_insert = ("""
    INSERT INTO public.dim_countries(country_code, country, average_temperature) 
    SELECT  c.country_code, c.country, MAX(t.AverageTemperature)
    FROM public.staging_immigration i
    JOIN public.staging_countries c
    ON i.country_code = c.country_code
    LEFT JOIN public.staging_country_temperatures t
    ON UPPER(c.country) = UPPER(t.Country)
    GROUP BY  c.country_code, c.country;
    """)
    
#     countries_table_insert = ("""
#     INSERT INTO public.dim_countries(country_code, country) 
#     SELECT  DISTINCT c.country_code, c.country
#     FROM public.staging_immigration i
#     JOIN public.staging_countries c
#     ON i.country_code = c.country_code;
#     """)

    # Create staging ports table
    create_staging_ports_table = """
    DROP TABLE IF EXISTS public.staging_ports;
    CREATE TABLE public.staging_ports (
    port_code varchar(3),
    city varchar(256),
    state varchar(50)
    );
    """

    # Create Ports table on Redshift
    create_ports_table = """
    DROP TABLE IF EXISTS public.dim_ports CASCADE;
    CREATE TABLE public.dim_ports
    (
    port_code VARCHAR(3) UNIQUE,
    port_city VARCHAR(256),
    port_state VARCHAR(50),
    average_temperature NUMERIC(16,3) NULL,
    PRIMARY KEY(port_code)
    );
    """
    
    ports_table_insert = ("""
    INSERT INTO public.dim_ports(port_code, port_city, port_state, average_temperature) 
    SELECT p.port_code, p.city, p.state, MAX(t.AverageTemperature)
    FROM public.staging_immigration i
    JOIN public.staging_ports p
    ON i.port_code = p.port_code
    LEFT JOIN public.staging_city_temperatures t
    ON UPPER(t.City) = UPPER(p.city)
    GROUP BY p.port_code, p.city, p.state;
    """)
    
#     ports_table_insert = ("""
#     INSERT INTO public.dim_ports(port_code, port_city, port_state) 
#     SELECT DISTINCT p.port_code, p.city, p.state
#     FROM public.staging_immigration i
#     JOIN public.staging_ports p
#     ON i.port_code = p.port_code;
#     """)
  
    
    # Create demographics staging table
    create_staging_demographics_table = """
    DROP TABLE IF EXISTS public.staging_demographics;
    CREATE TABLE public.staging_demographics (
    City varchar(256),
    State varchar(100),
    "Median Age" numeric(18,2),
    "Male Population" numeric(18,2),
    "Female Population" numeric(18,2),
    "Total Population" int8,
    "Number of Veterans" numeric(18,2),
    "Foreign-born" numeric(18,2),
    "Average Household Size" numeric(18,2),
    "State Code" varchar(50),
    "American Indian and Alaska Native" int, 
    "Asian" int,
    "Black or African-American" int,
    "Hispanic or Latino" int,
    "White" int   
    );
    """

    # Create demographics dimension table on Redshift
    create_demographics_table = """
    DROP TABLE IF EXISTS public.dim_demographics CASCADE;
    CREATE TABLE public.dim_demographics
    (
    city varchar(256),
    state varchar(100),
    median_age numeric(18,2),
    male_population numeric(18,2),
    female_population numeric(18,2),
    total_population bigint,
    number_of_veterans numeric(18,2),
    foreign_born numeric(18,2),
    avg_household_size numeric(18,2),
    state_code varchar(50),
    "American Indian and Alaska Native Population" int4, 
    "Asian Population" int4,
    "Black or African-American Population" int4,
    "Hispanic or Latino Population" int4,
    "White Population" int4,
    PRIMARY KEY(city, state)
    );
    """

    demographics_table_insert = ("""
    INSERT INTO public.dim_demographics(city, state, median_age, male_population, female_population, total_population, number_of_veterans, foreign_born, avg_household_size, state_code, "American Indian and Alaska Native Population", "Asian Population", "Black or African-American Population", "Hispanic or Latino Population", "White Population") SELECT p.city,
    p.state,
    sd."Median Age", 
    sd."Male Population", 
    sd."Female Population", 
    sd."Total Population",
    sd."Number of Veterans",
    sd."Foreign-born",
    sd."Average Household Size",
    sd.state,
    sd."American Indian and Alaska Native",
    sd."Asian",
    sd."Black or African-American",
    sd."Hispanic or Latino",
    sd.White
    FROM public.staging_ports p
    JOIN public.staging_immigration i
    ON p.port_code = i.port_code
    JOIN staging_demographics sd
    ON UPPER(p.city) = UPPER(sd.City) AND UPPER(p.state) = UPPER(sd."State Code");
    """)
    
   
    # Create Time table on Redshift
    create_time_table = """
    DROP TABLE IF EXISTS public.dim_time CASCADE;
    CREATE TABLE public.dim_time
    (
    sas_timestamp DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL,
    quarter INT NOT NULL,
    PRIMARY KEY (sas_timestamp)
    );
    """

    time_table_insert = ("""  
    INSERT INTO public.dim_time(sas_timestamp, year, month, day, week, day_of_week, quarter) SELECT t1.sas_date, 
    date_part('year', t1.sas_date) as year,
    date_part('month', t1.sas_date) as month,
    date_part('day', t1.sas_date) as day, 
    date_part('quarter', t1.sas_date) as quarter,
    date_part('week', t1.sas_date) as week,
    date_part('dow', t1.sas_date) as day_of_week
    FROM
    (SELECT DISTINCT arrival_date as sas_date
    FROM staging_immigration
    ) t1;
    """)
    

    

    

    

    


    
