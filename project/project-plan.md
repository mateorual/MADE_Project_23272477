# Project Plan

## Title
<!-- Give your project a short title. -->
### The Cost of Popularity: Analyzing Tourism's Effect on Medellín's Housing Market

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
1. What is the impact of tourism (domestic and inbound) on housing sales and rental prices in Medellin, Colombia

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
As the city of Medellín grows in popularity as a destination, understanding its economic impact on local housing markets becomes critical, especially regarding housing affordability for residents.The aim of this project is to address the central question by analyzing historical data on monthly arrivals of both international and domestic passengers at Medellín's airport, alongside datasets on real estate sales and rental listings across various years. One of the main objectives is to investigate correlations and potential causal relationships between tourism patterns and housing price fluctuations.
The analysis will involve statistical methods to detect trends, seasonal effects, and changes over time, offering a comprehensive view of tourism's influence on the housing market.The findings aim to provide insights that could inform local policy decisions, guide real estate investors, and  and offer residents a clearer understanding of housing dynamics. This project also seeks to contribute to broader discussions on sustainable urban development in popular tourist destinations, balancing Medellín's appeal with the needs of its community.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Monthly entry of foreigners and Colombians at José María Córdova airport (Medellín).

* Metadata URL: <https://medata.gov.co/dataset/1-010-04-000188>

* Data URL: <https://medata.gov.co/sites/default/files/distribution/1-010-04-000188/ingreso_mensual_de_extranjeros_y_colombianos_por_punto_migratorio_jose_maria_cordova.csv>

* Data Type: CSV

* License: Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)

This dataset provides monthly data on the number of passengers arriving at the José María Córdova International Airport in Medellín from 2008 to 2022. It captures arrival trends of domestic and international passengers over various months, useful for analyzing tourism influxes over time.

### Datasource2: Property offers for sale in Medellín for the year 2021.

* Metadata URL: <https://medata.gov.co/dataset/1-014-26-000430>

* Data URL: <https://www.google.com/maps/d/kml?mid=1dvXgm6Xb_hHjsVqhh6FWcZuq1g1pjTI&resourcekey&forcekml=1>

* Data Type: KML -> CSV

* License: Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)

This dataset enables analysis of property values, price per square meter, property types, and distribution across neighborhoods, allowing for insights into the housing market in Medellín.
There are datasets of sales offers for the period 2008-2022, in this case the referenced data set corresponds to the year 2021. In turn, there are data sets available for the same period with information on rental offers.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Datasets selection. [#1][i1]
2. Building an automated data pipeline. [#2][i2]
3. Data Profiling / Exploratory Data Analysis (EDA) and Feature Engineering
4. Statistical Modeling or implementation of a suitable technique/method to derive conclusions.
5. Model Evaluation: interpretation and insights.
6. Reporting on challenges-findings / Presentation

[i1]: https://github.com/mateorual/MADE_Project_23272477/issues/1 
[i2]: https://github.com/mateorual/MADE_Project_23272477/issues/2