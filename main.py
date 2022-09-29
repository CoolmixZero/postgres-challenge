import requests
import json
import psycopg2
import pandas as pd
import datetime


class Database:

    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="admin")
        self.cur = self.conn.cursor()

    def close_connection(self) -> None:
        self.cur.close()
        self.conn.close()

    @staticmethod
    def sort_fetch(fetch) -> dict:
        d = dict(fetch)
        return {k: v for k, v in sorted(d.items(), key=lambda item: item[1], reverse=True)}

    @staticmethod
    def _parse_current_date() -> tuple[str, ...]:
        my_date = datetime.date.today()
        year, week_num, day_of_week = my_date.isocalendar()
        dates = [f"{year}-{week_num - i}" for i in range(1, 3)]
        return tuple(dates)

    @staticmethod
    def _parse_date(date: str) -> str:
        """format DD/MM/YYYY"""
        date = date.split("/")
        year, month = date[2], date[1]
        return f"{year}-{month}"


class CovidData(Database):

    def __init__(self):
        super().__init__()

    def create_covid_table(self) -> None:
        query_create_table = """CREATE TABLE IF NOT EXISTS covid_data(
                    continent TEXT, country TEXT, country_code TEXT, cumulative_count NUMERIC, \
                    indicator TEXT, population INTEGER, source TEXT, weekly_count TEXT, \
                    year_week TEXT)"""
        self.cur.execute(query_create_table)
        self._delete_duplicates()
        self.conn.commit()

    """Exercise 1: Loading Data (Data source 1) + Exercise 2: Create a Pipeline"""

    def load_json_pipeline(self, data: list) -> None:
        query_compare_columns = """
                    INSERT INTO covid_data \
                    SELECT * FROM json_populate_recordset(NULL::covid_data, %s)
                    WHERE(year_week, weekly_count) \
                    NOT IN (SELECT year_week, weekly_count FROM covid_data)"""
        self.cur.execute(query_compare_columns, (json.dumps(data),))
        self.conn.commit()

    """Exercise 3: Create a View"""

    def create_view(self) -> None:
        week1, week2 = self._parse_current_date()
        query_create_view = """CREATE OR REPLACE VIEW latest_cases AS \
                                SELECT continent, country, 
                                    round(CAST((100 / NULLIF(cumulative_count, 0) * 100000) AS NUMERIC), 2) \
                                    "cumulative_count_per_100000",  
                                    indicator, year_week \
                                FROM covid_data \
                                WHERE indicator = 'cases' AND cumulative_count IS NOT NULL \
                                    AND (year_week = %s OR year_week = %s)"""
        self.cur.execute(query_create_view, (week1, week2))
        self.conn.commit()

    """Exercise 4 (1): Queries"""

    def get_country_with_the_highest_covid_cases(self, date: str) -> str:
        date = self._parse_date(date)
        query = """SELECT country, (MAX(100 / NULLIF(cumulative_count, 0) * 100000))::float \
                    FROM covid_data \
                    WHERE year_week = %s AND cumulative_count > 0 \
                    GROUP BY cumulative_count, country \
                    ORDER BY cumulative_count DESC"""
        self.cur.execute(query, (date,))
        countries = self.cur.fetchall()
        d = {value: country for country, value in countries if value}
        country = d[max(d.keys())]
        return country

    """Exercise 4 (2): Queries"""

    def get_top_10_countries_with_the_lowest_covid_cases(self, date: str) -> list:
        date = self._parse_date(date)
        query = """SELECT country, (MIN(100 / NULLIF(cumulative_count, 0) * 100000))::float \
                            FROM covid_data \
                            WHERE cumulative_count > 0 AND year_week = %s \
                            GROUP BY cumulative_count, country"""
        self.cur.execute(query, (date,))
        countries = self.cur.fetchall()
        top_10 = [country[0] for i, country in enumerate(countries) if i <= 9]
        return top_10

    """Exercise 4 (3): Queries"""

    def get_top_10_countries_with_highest_num_of_cases_among_richest(self, top_20_richest: tuple[str, ...]) -> list:
        query_top_10_highest_cases = """SELECT DISTINCT country, 
                                        (MAX(100 / NULLIF(cumulative_count, 0) * 1000000))::float \
                                        FROM covid_data \
                                        WHERE cumulative_count > 0 \
                                        GROUP BY country"""
        self.cur.execute(query_top_10_highest_cases)
        d = self.sort_fetch(self.cur.fetchall())
        top_10 = [country for country in d if country in top_20_richest]
        return top_10

    """Exercise 4 (4): Queries"""

    def get_regions_with_the_number_of_cases_per_million(self, date: str) -> dict:
        date = self._parse_date(date)
        query = """SELECT DISTINCT country, (MAX(100 / NULLIF(cumulative_count, 0) * 1000000))::float \
                        FROM covid_data
                        WHERE year_week = %s AND cumulative_count > 0 \
                        GROUP BY country;"""
        self.cur.execute(query, (date,))
        regions = self.sort_fetch(self.cur.fetchall())
        return regions

    """Exercise 4 (5): Queries"""

    def find_duplicates(self) -> list:
        query = """SELECT (covid_data.*)::text, count(*)
                    FROM covid_data
                    GROUP BY covid_data.*
                    HAVING count(*) > 1"""
        self.cur.execute(query)
        duplicates = self.cur.fetchall()
        return duplicates

    def _delete_duplicates(self) -> None:
        query = """DELETE FROM "covid_data" a
                    USING "covid_data" b
                    WHERE a.ctid < b.ctid AND 
                    a."country" = b."country" AND
                    a."cumulative_count" = b."cumulative_count" AND
                    a."indicator" = b."indicator" AND
                    a."population" = b."population" AND
                    a."weekly_count"= b."weekly_count" AND
                    a."year_week" = b."year_week";"""
        self.cur.execute(query)
        self.conn.commit()

    @staticmethod
    def get_json(url: str) -> list:
        response = requests.get(url)
        data = response.json()
        return data


class CountriesData(Database):

    def __init__(self):
        super().__init__()

    def create_countries_table(self) -> None:
        query_create_table = """CREATE TABLE IF NOT EXISTS countries_data(
                            "Country" TEXT, "Region" TEXT, "Population" INTEGER, "Area (sq. mi.)" TEXT,
                            "Pop. Density (per sq. mi.)" TEXT, "Coastline (coast/area ratio)" TEXT,
                            "Net migration" TEXT, "Infant mortality (per 1000 births)" TEXT,
                            "GDP ($ per capita)" INTEGER, "Literacy (%)" TEXT, "Phones (per 1000)" TEXT,
                            "Arable (%)" TEXT, "Crops (%)" TEXT, "Other (%)" TEXT, "Climate" VARCHAR,
                            "Birthrate" TEXT, "Deathrate" TEXT, "Agriculture" TEXT, "Industry" TEXT,
                            "Service" TEXT)"""
        self.cur.execute(query_create_table)
        self._delete_duplicates()
        self.conn.commit()

    """Exercise 4 (3): Queries"""

    def get_top_20_richest_countries(self) -> tuple[str, ...]:
        query_top_20_richest = """SELECT countries_data."Country", "GDP ($ per capita)"
                                    FROM countries_data \
                                    WHERE "GDP ($ per capita)" > 0 
                                    ORDER BY "GDP ($ per capita)" DESC"""
        self.cur.execute(query_top_20_richest)
        d = {}
        for row in set(self.cur.fetchall()):
            country, gdp = row
            d[gdp] = country
        sorted_gdp = sorted(d, reverse=True)
        top_20 = [d[sorted_gdp[i]].strip() for i in range(20)]
        return tuple(top_20)

    """Exercise 1: Loading Data (Data source 2)"""

    def load_csv(self) -> None:
        query_load_csv = """COPY countries_data
                            FROM 'D:\PyCharm Projects\Big Data Engineer Challenge\countries_of_the_world.csv'
                            CSV HEADER DELIMITER ',';"""
        self.cur.execute(query_load_csv)
        self.conn.commit()

    """Exercise 4 (4): Queries"""

    def display_information_on_population_density(self) -> dict:
        query = """SELECT DISTINCT ON ("Pop. Density (per sq. mi.)") "Country", "Pop. Density (per sq. mi.)" \
                        FROM countries_data
                        GROUP BY "Country", "Pop. Density (per sq. mi.)";"""
        self.cur.execute(query)
        data = self.sort_fetch(self.cur.fetchall())
        return data

    """Exercise 4 (5): Queries"""

    def find_duplicates(self) -> list:
        query = """SELECT (countries_data.*)::text, count(*)
                    FROM countries_data
                    GROUP BY countries_data.*
                    HAVING count(*) > 1"""
        self.cur.execute(query)
        duplicates = self.cur.fetchall()
        return duplicates

    def _delete_duplicates(self) -> None:
        query = """DELETE FROM "countries_data" a
                    USING "countries_data" b
                    WHERE a.ctid < b.ctid AND 
                    a."Country" = b."Country" AND
                    a."Region" = b."Region" AND
                    a."Population" = b."Population" AND
                    a."Area (sq. mi.)" = b."Area (sq. mi.)" AND
                    a."Pop. Density (per sq. mi.)" = b."Pop. Density (per sq. mi.)" AND
                    a."Coastline (coast/area ratio)" = b."Coastline (coast/area ratio)" AND
                    a."Net migration" = b."Net migration" AND
                    a."Infant mortality (per 1000 births)" = b."Infant mortality (per 1000 births)" AND
                    a."GDP ($ per capita)" = b."GDP ($ per capita)" AND
                    a."Literacy (%)" = b."Literacy (%)" AND
                    a."Phones (per 1000)" = b."Phones (per 1000)" AND
                    a."Arable (%)" = b."Arable (%)" AND
                    a."Crops (%)" = b."Crops (%)" AND
                    a."Other (%)" = b."Other (%)" AND
                    a."Climate" = b."Climate" AND
                    a."Birthrate" = b."Birthrate" AND
                    a."Deathrate" = b."Deathrate" AND
                    a."Agriculture" = b."Agriculture" AND
                    a."Industry" = b."Industry" AND
                    a."Service" = b."Service";"""
        self.cur.execute(query)
        self.conn.commit()

    @staticmethod
    def read_csv(path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        return df


def main():
    covid_db = None
    countries_db = None

    try:
        covid_db = CovidData()
        countries_db = CountriesData()

        # Ex. 1 and 2
        # Create tables
        covid_db.create_covid_table()
        countries_db.create_countries_table()

        # Load covid data (json file) to database
        covid_url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/"
        covid_data = covid_db.get_json(covid_url)
        covid_db.load_json_pipeline(covid_data)

        # Load countries data (CSV file that already downloaded) to database")
        countries_db.load_csv()

        # Ex. 3
        # Create view
        covid_db.create_view()

        # Ex. 4
        date = "31/07/2020"

        # Get country with the highest number of Covid-19 cases per 100 000 Habitats
        covid_db.get_country_with_the_highest_covid_cases(date)  # output -> Sweden

        # Get top 10 countries with the lowest number of Covid-19 cases per 100 000 Habitats
        covid_db.get_top_10_countries_with_the_lowest_covid_cases(date)
        # output -> ['Finland', 'Germany', 'Iceland', 'Ireland', 'Luxembourg',
        # 'Austria', 'Belgium', 'Denmark', 'Netherlands']

        # Get top 10 countries with the highest number of cases among the top 20 richest countries (by GDP per capita)?
        top_20_richest = countries_db.get_top_20_richest_countries()
        # output -> ('Luxembourg', 'United States', 'Bermuda',
        # 'Cayman Islands', 'San Marino', 'Switzerland', 'Denmark',
        # 'Iceland', 'Austria', 'Canada', 'Ireland', 'Belgium',
        # 'Australia', 'Hong Kong', 'Netherlands', 'Japan',
        # 'Aruba', 'United Kingdom', 'France', 'Finland')

        covid_db.get_top_10_countries_with_highest_num_of_cases_among_richest(top_20_richest)
        # output -> ['Luxembourg', 'Norway', 'Finland', 'Netherlands', 'Denmark',
        # 'Iceland', 'Austria', 'France', 'Ireland', 'Belgium']

        # List all the regions with the number of cases per million of inhabitants
        # and display information on population density
        regions = covid_db.get_regions_with_the_number_of_cases_per_million(date)
        # output -> {'Finland': 100000000.0, 'France': 100000000.0, 'Spain': 100000000.0,
        # 'Sweden': 100000000.0, 'Belgium': 33333333.333333332, 'Italy': 33333333.333333332,
        # 'Iceland': 6666666.666666667, 'Germany': 4000000.0}

        countries_db.display_information_on_population_density()
        # output -> {'Armenia ': '99,9', 'Slovenia ': '99,2', 'Togo ': '97,7',
        # 'Romania ': '93,9', 'Ghana ': '93,6', 'Montserrat ': '92,5',
        # 'Azerbaijan ': '91,9', 'Dominica ': '91,4', 'Turkey ': '90,2',
        # 'Niger ': '9,9', 'Angola ': '9,7', 'Mali ': '9,5', 'Cook Islands ': '89,1', ...}

        # Query the data to find duplicated records
        covid_db.find_duplicates()
        countries_db.find_duplicates()

    except (Exception, psycopg2.DatabaseError) as Error:
        print(Error)

    finally:
        # Close connections
        covid_db.close_connection()
        countries_db.close_connection()


if __name__ == "__main__":
    main()
