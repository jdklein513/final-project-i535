# final-project-i535

Final project for I535 - Access, Use, and Management of Big Data

## Synopsis

This project builds an analytics dashboard tracking travel levels before, during, and after the COVID-19 pandemic while gaining insight on the driving factors halting travel recovery in specific areas of the country. 
Although the Bureau of Transportation reports travel statistics directly on their website, there is no readily available dashboard which compares current travel rates directly to pre-COVID rates. In addition, there is also no single source which highlights the relationship between metro status, political voting, demographics, and weather - all effects shown to be associated with higher vaccination rates, case counts, and general apprehension of COVID-19 - on travel recovery. The resulting dashboard demonstrates the recovery rates over time by vaccination rate, average age, and weather patterns, as well as compares travel recovery of metro counties vs. non-metro counties, and democratic vs. republican counties respectively. The project repository contains the supporting code to implement the pipeline within Google Cloud Platform.


## Repo Structure


+ **code**: stores Python code used in Google Cloud Functions for extracting data from public APIs and ingesting into Google BigQuery and SQL code for cleaning and integrating the data sources for the Tableau dashboard.

    + pt01_data_load: Python code used in Google Cloud Functions for extracting trip, vaccination rate, and weather data from public APIs and ingesting into Google BigQuery
      + requirements.txt: requirements needed for the python runtime in Cloud Functions

    + pt02_data_prep: SQL code for cleaning and integrating the data sources into a clean table within Google BigQuery for the Tableau dashboard

+ **static_data**: stores the static public data set which are used to generate metropolitan status, election results, and demographics features for comparing to travel recovery rates.

    + 2000 2020 Presidential Election Results: 2000-2020 Presidential Election Results data extract from (Harvard Dataverse)[https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VOQCHQ]

    + 2013 NCHS Urban Rural Classification Scheme for Counties: 2013 NCHS Urban Rural Classification data extract from (National Center for Health Statistics - NCHS)[https://www.cdc.gov/nchs/data_access/urban_rural.htm#Data_Files_and_Documentation]

    + 2020 US Census Demographic Data Age Sex: 2020 U.S. Census Demographics data extract from (U.S. Census Bureau)[https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/asrh/]


+ **dashboard**: sore the reports, presentations, models, and plot output from the code section of the repo.

    + covid_recovery_dashboard.twb: the final version of the U.S. Travel Recovery Tableau dashboards published to Tableau Public


## Contributors

* Joel Klein (joeklein@iu.edu)
