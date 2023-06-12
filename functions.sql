--Creacion de tablas necesarias
----------------TABLAS-------------------
DROP TABLE IF EXISTS us_births_2016_2021;
DROP TABLE IF EXISTS definitiva;
DROP TABLE IF EXISTS ESTADO;
DROP TABLE IF EXISTS ANIO;
DROP TABLE IF EXISTS nivel_educacion;


CREATE TABLE us_births_2016_2021
(
    State                           text not null,
    State_Abbreviation              text not null,
    Year                            integer,
    Gender                          text not null,
    Mother_Education_Level          text not null,
    Education_Level_Code            integer,
    Births                          integer,
    Mother_Average_Age              float,
    Average_Birth_Weight            float,
    PRIMARY KEY (State_Abbreviation, Year,Gender,Education_Level_Code)
);

CREATE TABLE ESTADO
(
    State                           text not null,
    State_Abbreviation              text not null,
    PRIMARY KEY (State_Abbreviation)
);

CREATE TABLE ANIO
(
    Year                            integer,
    Is_Leap_Year                    bool,
    PRIMARY KEY (Year)
);

CREATE TABLE NIVEL_EDUCACION
(
    Education_Level_Code            integer,
    Mother_Education_Level          text not null,
    PRIMARY KEY (Education_Level_Code)
);

CREATE TABLE definitiva
(
    State_Abbreviation              text not null,
    Year                            integer,
    Gender                          text not null,
    Education_Level_Code            integer,
    Births                          integer,
    Mother_Average_Age              float,
    Average_Birth_Weight            float,
    PRIMARY KEY (State_Abbreviation, Year,Gender,Education_Level_Code),
    FOREIGN KEY (State_Abbreviation) REFERENCES ESTADO(State_Abbreviation),
    FOREIGN KEY (Year) REFERENCES ANIO(Year),
    FOREIGN KEY (Education_Level_Code) REFERENCES NIVEL_EDUCACION(EDUCATION_LEVEL_CODE)
);

----------------FUNCIONES-------------------

CREATE OR REPLACE FUNCTION check_leap_year()
  RETURNS TRIGGER AS $$
BEGIN
  IF NEW.year % 4 = 0
    AND (NEW.year % 100 != 0
    OR NEW.year % 400 = 0) THEN
    NEW.Is_Leap_Year := TRUE;
  ELSE
    NEW.Is_Leap_Year := FALSE;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fill_tables()
  RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO ESTADO (State, State_Abbreviation) VALUES (new.State,new.State_Abbreviation) ON CONFLICT DO NOTHING;
    INSERT INTO ANIO (Year) VALUES (new.Year) ON CONFLICT DO NOTHING;
    INSERT INTO NIVEL_EDUCACION (Education_Level_Code, Mother_Education_Level) VALUES (new.Education_Level_Code, new.Mother_Education_Level) ON CONFLICT DO NOTHING;
    INSERT INTO definitiva (State_Abbreviation, Year, Gender, Education_Level_Code, Births, Mother_Average_Age, Average_Birth_Weight) VALUES (new.State_Abbreviation, new.Year, new.Gender, new.Education_Level_Code, new.Births, new.Mother_Average_Age, new.Average_Birth_Weight) ON CONFLICT DO NOTHING;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

----------------TRIGGERS----------------

CREATE TRIGGER leap_year_trigger
  BEFORE INSERT OR UPDATE ON ANIO
  FOR EACH ROW
  EXECUTE FUNCTION check_leap_year();

CREATE TRIGGER fill_tables
  BEFORE INSERT OR UPDATE ON us_births_2016_2021
  FOR EACH ROW
  EXECUTE FUNCTION fill_tables();

----------------FUNCION REPORTE Y FUNCIONES DERIVADAS-------------------

CREATE OR REPLACE FUNCTION ReporteConsolidado(in years_qty integer)
    RETURNS VOID AS $$
    DECLARE
        currentYear                 INTEGER;
        firstPrint                  BOOLEAN;
        totals                      RECORD;
        stateCursor                 REFCURSOR;
        genderCursor                REFCURSOR;
        educationLevelCursor        REFCURSOR;
BEGIN
    IF years_qty<=0 THEN
        RAISE WARNING 'La cantidad de anios a mostrar debe ser mayor a 0.';
        RETURN;
    END IF;
    RAISE NOTICE '-------------------------------------------------------------------------------------------------------------------------------------------------------------';
    RAISE NOTICE '------------------------------------------------------------------CONSOLIDATED BIRTH REPORT------------------------------------------------------------------';
    RAISE NOTICE '-------------------------------------------------------------------------------------------------------------------------------------------------------------';
    RAISE NOTICE 'Year--Category---------------------------------------------------------------------------Total---AvgAge---MinAge---MaxAge---AvgWeight---MinWeight---MaxWeight';
    RAISE NOTICE '-------------------------------------------------------------------------------------------------------------------------------------------------------------';

    SELECT min(year) INTO currentYear FROM definitiva;

    WHILE (years_qty > 0)
        LOOP
            firstPrint := TRUE;
            OPEN stateCursor FOR SELECT E.State AS Category,
                                        sum(definitiva.births) AS Total,
                                        AVG(definitiva.Mother_Average_Age) AS AvgAge,
                                        min(definitiva.Mother_Average_Age) AS MinAge,
                                        max(definitiva.Mother_Average_Age) AS MaxAge,
                                        AVG(definitiva.Average_Birth_Weight) AS AvgWeight,
                                        min(definitiva.Average_Birth_Weight) AS MinWeight,
                                        max(definitiva.Average_Birth_Weight) AS MaxWeight FROM definitiva JOIN ESTADO E on E.State_Abbreviation = definitiva.State_Abbreviation
                                        WHERE definitiva.Year = currentYear
                                        GROUP BY E.State
                                        HAVING SUM(definitiva.births) > 200000
                                        ORDER BY E.State DESC;
            firstPrint := executeCursor(stateCursor, currentYear, 'State',firstPrint);
            CLOSE stateCursor;

            OPEN genderCursor FOR SELECT (CASE WHEN definitiva.Gender = 'M' THEN 'Male' WHEN definitiva.Gender = 'F' THEN 'Female' END) AS Category,
                                        sum(definitiva.births) AS Total,
                                        avg(definitiva.Mother_Average_Age) AS AvgAge,
                                        min(definitiva.Mother_Average_Age) AS MinAge,
                                        max(definitiva.Mother_Average_Age) AS MaxAge,
                                        avg(definitiva.Average_Birth_Weight) AS AvgWeight,
                                        min(definitiva.Average_Birth_Weight) AS MinWeight,
                                        max(definitiva.Average_Birth_Weight) AS MaxWeight FROM definitiva
                                        WHERE definitiva.Year = currentYear
                                        GROUP BY definitiva.Gender
                                        ORDER BY definitiva.Gender DESC;
            firstPrint := executeCursor(genderCursor, currentYear, 'Gender',firstPrint);
            CLOSE genderCursor;

            OPEN educationLevelCursor FOR SELECT NE.Mother_Education_Level AS Category,
                                        sum(definitiva.births) AS Total,
                                        avg(definitiva.Mother_Average_Age) AS AvgAge,
                                        min(definitiva.Mother_Average_Age) AS MinAge,
                                        max(definitiva.Mother_Average_Age) AS MaxAge,
                                        avg(definitiva.Average_Birth_Weight) AS AvgWeight,
                                        min(definitiva.Average_Birth_Weight) AS MinWeight,
                                        max(definitiva.Average_Birth_Weight) AS MaxWeight FROM definitiva JOIN NIVEL_EDUCACION NE on NE.Education_Level_Code = definitiva.Education_Level_Code
                                        WHERE definitiva.Year = currentYear AND NE.Mother_Education_Level IS NOT NULL
                                        GROUP BY NE.Mother_Education_Level
                                        ORDER BY NE.Mother_Education_Level DESC;
            firstPrint := executeCursor(educationLevelCursor, currentYear, 'Education',firstPrint);
            CLOSE educationLevelCursor;

            SELECT  sum(definitiva.Births) AS Total,
                    avg(definitiva.Mother_Average_Age) AS AvgAge,
                    min(definitiva.Mother_Average_Age) AS MinAge,
                    max(definitiva.Mother_Average_Age) AS MaxAge,
                    avg(definitiva.Average_Birth_Weight) AS AvgWeight,
                    min(definitiva.Average_Birth_Weight) AS MinWeight,
                    max(definitiva.Average_Birth_Weight) AS MaxWeight INTO totals FROM definitiva
                    WHERE definitiva.Year = currentYear;

            IF (totals.Total != 0) THEN
                RAISE NOTICE '---------------------------------------------------------------------------------------- % % % % % % %',
                    rpad(CAST(totals.Total AS integer)::text, 7), rpad(CAST(totals.AvgAge AS integer)::text, 8), rpad(CAST(totals.MinAge AS integer)::text, 8),
                    rpad(CAST(totals.MaxAge AS integer)::text, 8), rpad(CAST(ROUND(CAST(totals.AvgWeight/1000 as numeric),3) AS float)::text, 11), rpad(CAST(ROUND(CAST(totals.MinWeight/1000 as numeric),3) AS float)::text, 11), CAST(ROUND(CAST(totals.MaxWeight/1000 as numeric),3) AS float);
                RAISE NOTICE '-------------------------------------------------------------------------------------------------------------------------------------------------------------';
            END IF;

            years_qty := years_qty - 1;
            currentYear := currentYear + 1;

        END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION executeCursor(IN cursor REFCURSOR, IN year INTEGER, IN category TEXT,
                                         IN firstPrint BOOLEAN) RETURNS BOOLEAN AS
$$
DECLARE
    i RECORD;

BEGIN
    LOOP
        FETCH cursor INTO i;
        EXIT WHEN NOT FOUND;
        IF (firstPrint) THEN
            PERFORM printInfo(year, category, i.Category, CAST(i.Total AS integer), CAST(i.AvgAge AS integer), CAST(i.MinAge AS integer),
                    CAST(i.MaxAge AS integer), CAST(i.AvgWeight AS float), CAST(i.MinWeight AS float),CAST(i.MaxWeight AS float));
            firstPrint := FALSE;
        ELSE
            PERFORM printInfo(-1, category, i.Category, CAST(i.Total AS integer), CAST(i.AvgAge AS integer), CAST(i.MinAge AS integer),
                    CAST(i.MaxAge AS integer), CAST(i.AvgWeight AS float), CAST(i.MinWeight AS float),CAST(i.MaxWeight AS float));
        END IF;
    END LOOP;
    RETURN firstPrint;
END;
$$ LANGUAGE plpgsql
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION printInfo(IN year INTEGER, IN category TEXT, IN categoryType TEXT, IN total_births INTEGER,
                                     IN avg_age INTEGER, IN min_age INTEGER,IN max_age INTEGER,IN avg_weight FLOAT,
                                     IN min_weight FLOAT, IN max_weight FLOAT) RETURNS VOID AS
$$
BEGIN
    IF (year <> -1) THEN
        RAISE NOTICE '%  %: % % % % % % % %', year, rpad(category, 10), rpad(categoryType, 70), rpad(total_births::text, 7), rpad(avg_age::text, 8), rpad(min_age::text, 8), rpad(max_age::text, 8), rpad(ROUND(CAST(avg_weight/1000 as numeric),3)::text, 11), rpad(ROUND(CAST(min_weight/1000 as numeric),3)::text, 11), ROUND(CAST(max_weight/1000 as numeric),3);
    ELSE
        RAISE NOTICE '----  %: % % % % % % % %', rpad(category, 10), rpad(categoryType, 70), rpad(total_births::text, 7), rpad(avg_age::text, 8), rpad(min_age::text, 8), rpad(max_age::text, 8), rpad(ROUND(CAST(avg_weight/1000 as numeric),3)::text, 11), rpad(ROUND(CAST(min_weight/1000 as numeric),3)::text, 11),ROUND(CAST(max_weight/1000 as numeric),3);
    END IF;
    RETURN;
END;
$$ LANGUAGE plpgsql
    RETURNS NULL ON NULL INPUT;
