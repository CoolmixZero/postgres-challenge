# Data Engineering Challenge
Desired Output: A Document, with all the steps necessary to execute the proposed solution and conclusions derived from data analysis.
-	Load Script(s)
-	SQL Script(s)
-	Text file(s) with the conclusions.
## Exercise 1: Loading Data
Programmatically (Using Python or other programming language or DB tools) retrieve JSON data from europe.eu (link below) and load the data to a PostgreSQL Database (install it locally).

Data source 1: Covid  Data: https://www.ecdc.europa.eu/en/publications-data/data-national-14-day-notification-rate-covid-19 (use the Json file)

Load the CSV table Countries of the word.

Data source 2: Countries data: https://www.kaggle.com/fernandol/countries-of-the-world/data?select=countries+of+the+world.csv

Alternative: Load the data manually (using some importing tool).
## Exercise 2: Create a Pipeline

Create a Data Pipeline that extracts the last version of the data (Covid-19 data, Data Source 1) and adds to the PostgreSQL database only the new records. (Note the Data source 1, Covid-19 dataset, changes one time per day)
## Exercise 3: Create a View

Create a view with the data of the table “Countries of the word” with the latest number of cases, “Cumulative_number_for_14_days_of_COVID-19_cases_per_100000” and date when the Information was extracted.
## Exercise 4: Queries
1.	What is the country with the highest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?
2.	What is the top 10 countries with the lowest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?
3.	What is the top 10 countries with the highest number of cases among the top 20 richest countries (by GDP per capita)?
4.	List all the regions with the number of cases per million of inhabitants and display information on population density, for 31/07/2020. 
5.	Query the data to find duplicated records.
