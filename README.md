## National Weather Service Web Scrape and ETL
#### By [Drew White](https://www.linkedin.com/in/drew-riley-white/)
## Contents
[Summary](#summary) |
[Technologies Used](#technologies-used) |
[Sources](#sources) |
[Description](#description) |
[dw_weather_scrape.py](#dw_weather_scrapepy) |
[dw_weekly_avg.py](#dw_weekly_avgpy) |
[Visualizations](#visualizations) |
[Known Bugs](#known-bugs)
### Links:
[Looker Dashboard](https://lookerstudio.google.com/reporting/7fda1afa-8d15-402d-8773-a37f5a61bed1)
## Summary:
This is a data engineering project that utilizes various technologies to scrape weather data, transform it, and store it in Google BigQuery. The project uses Python as its primary language and Apache Airflow as its workflow management system. BeautifulSoup is used to scrape the data from the National Weather Service and Pandas is used to manipulate the data. Google BigQuery is used as the primary data store.

The `dw_weather_scrape.py` script contains three functions that work together to scrape weather data from the National Weather Service, transform the data, and write it to Google BigQuery on a daily basis.

The `dw_weekly_avg.py` script pulls data from the daily table in Google BigQuery and calculates weekly averages for select columns. The script then writes the averages to the weekly_avg table on a weekly basis.

_Note: For demonstration purposes in this project, the shcedule intervals are not daily and weekly but instead hourly and daily. This was to gather more data for the presentation of this project. In a full production environment, the Airflow DAGs will trigger at the daily and and weekly intervals._

## Technologies Used


* Python
* Apache Airflow
* Pandas
* BeautifulSoup
* Google BigQuery

</br>

## Sources:
_A dictionary of the sources of the city weather data:_

<img src="./../images/dw_cities.png" height=50% width=50%>

## Description:
## dw_weather_scrape.py
<img src="../images/dw_webscrape_daily_gif.gif">

* `scrape_weather_data`
    - Uses BeautifulSoup to scrape National Weather Service and put into Pandas data frame.
* `transform_weather_data`
    - Takes Pandas data frame and makes transformations on data to create more usable values.
* `write_weather_data_to_bq`
    - Writes the scraped/transformed data to Google BigQuery daily appending on to existing `daily` table.

<img src="../images/dw_weather_scrape.png" height=60% width=60%>

### Daily Schema:
<img src="../images/dw_daily_schema.png">

## dw_weekly_avg.py
<img src="../images/dw_weekly_avg_gif.gif" height=60% width=60%>

* `calculate_weekly_averages`
    - Pulls `daily` data from BigQuery and gets averages of select columns.
* `write_weekly_avg_to_bq`
    - Writes averages to BigQuery on weekly schedule to `weekly_avg` table.

<img src="../images/dw_weekly_avg.png" height=60% width=60%>

### Weekly Avg Schema:
<img src="../images/dw_weekly_avg_schema.png">
<br>

## Visualizations
<img src="../images/dw_dashboard.png" height=80% width=60%>

## Known Bugs

* No known bugs

<br>

_If you find any issues, please reach out at: **d.white0002@gmail.com**._

Copyright (c) _2023_ _Drew White_

</br>
