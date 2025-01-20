# The Cost of Popularity: Analyzing Tourism’s Effect on Medellín’s Housing Market (2011–2021)  
[![License: CC BY-SA 4.0](https://img.shields.io/badge/License-CC%20BY--SA%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-sa/4.0/)

> *This repository contains data engineering and data science projects and exercises using open data sources as part of the Methods of Advanced Data Engineering (MADE) course, taught by the FAU Chair for [Open-Source Software (OSS)](https://oss.cs.fau.de/) in the Winter 24/25 semester. This repo has been forked from the [made-template repository](https://github.com/jvalue/made-template) repository.*

<figure>
    <img src="[https://upload.wikimedia.org/wikipedia/commons/e/e3/Medellin_skyline.jpg](https://www.medellinadvisors.com/wp-content/uploads/2021/04/urban-transformation-in-medellin-768x400.jpg)" alt="Medellín Skyline" style="width:100%">
    <figcaption>Image credit: www.medellinadvisors.com </figcaption>
</figure>

---

## Overview  

Tourism is a vital driver of economic activity, with significant implications for local housing markets. Medellín, Colombia, has seen a remarkable rise in tourism over the past decade, understanding the relationship between tourism influx and housing market dynamics is essential.This study investigates how tourism, characterized by both domestic and inbound travel, impacts housing sales and rental prices. By analyzing data from 2011 to 2021, this research aims to uncover trends and correlations that provide insight into the economic and social effects of tourism on Medellín’s housing sector.

---

## Key Questions  

1. To what extent do domestic and inbound tourism trends correlate with fluctuations in housing marketprices in Medellín?
2. What is the impact of tourism (domestic and inbound) on housing sales and rental prices?

---

## Data Sources  

| Dataset | Description | License |  
|---------|-------------|---------|  
| Tourism Data | Monthly passenger arrivals in Medellín, segmented by nationality and origin. | [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) |  
| Housing Market Data | Monthly sales and rental offers, including property type, condition, price, and geospatial details. | [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) |  

For a more detailed explanation of the sources used, including metadata and corresponding links, please refer to the **Data Sources** section in the [project plan](https://github.com/mateorual/MADE_Project_23272477/blob/main/project/project-plan.md). This document provides additional insights into the datasets, their scope, and how they were utilized for the analysis.  

### Acknowledgment  

This project uses data sourced from the following organizations:  

1. [MEData](https://www.medata.gov.co/)  
   Data used under the [Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)](https://creativecommons.org/licenses/by-sa/4.0/) license.  

2. [Medellín Real Estate Observatory (OIME)](https://www.medellin.gov.co/es/secretaria-gestion-y-control-territorial/oime/)  
   Data used under the [Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)](https://creativecommons.org/licenses/by-sa/4.0/) license.  

Changes and adaptations have been made to the original data for use in this project. The derived works are also licensed under the [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) license.  

### Modifications to Original Data  

The following changes were made to the original datasets:  

- Merged datasets from multiple years (2007–2022).  
- Truncated data for the period 2011–2021.  
- Renamed columns for consistency.  
- Performed data cleaning, including standardizing formats, filling missing values where possible, and filtering data by property type (Apartment and House).  

### Note: 
	This work is purely non-commercial and is used for only a semester project at FAU to implement the ETL pipeline and provide valuable insights.

---

## Highlights  

- **Comprehensive ETL (Extract, Transform, Load) Data Pipeline**:  
  - Compiled and cleaned data from various sources into a unified SQLite database (`Housing_Tourism_Data.sqlite`), containing detailed information on tourism and housing metrics.  
  - [Pipeline Code](https://github.com/mateorual/MADE_Project_23272477/blob/main/project/pipeline.py) - Code for data extraction, transformation, and loading.  
  - [Data Report](https://github.com/mateorual/MADE_Project_23272477/blob/main/project/data-report.pdf) - Detailed description of the data cleaning and preparation process.  

- **In-Depth Analysis**:  
  - Conducted exploratory time series, correlation analysis, and Granger causality tests.  
  - Visualized trends and relationships through charts, scatter plots, and lagged correlations.  
  - [Analysis Code](https://github.com/mateorual/MADE_Project_23272477/blob/main/project/analysis-code.ipynb) - Jupyter Notebook containing the analysis process and code.  

- **Key Findings**:  
  - Moderate correlations between foreign tourist arrivals and sales prices, with lagged effects suggesting delayed impacts.  
  - Limited evidence of causality between tourism and housing prices.  
  - [Analysis Report](https://github.com/mateorual/MADE_Project_23272477/blob/main/project/analysis-report.pdf) - Summary of analysis, results, limitations, and conclusions of the study.  

---

## File Structure  

```plaintext
MADE_Project_23272477/
├── /data                                  # Processed data directory
│   └── Housing_Tourism_Data.sqlite        # Unified SQLite database
├── /project                              
│   ├── pipeline.py                        # Data pipeline script (ETL)
│   └── data-report.pdf                    # Data sources, ETL process, pipeline.py output
│   └── analysis-code.py                   # Analysis and visualization code
│   └── analysis-report.pdf                # Analysis, visualizations and insights
│   └── tests.py                           # Executes data pipeline and validates output
├── LICENSE.md                             # Project license details
└── README.md                              # Project documentation
```

---

## Run Pipeline Locally
Before you begin, make sure you have [Python3](https://www.python.org/) installed and set up [VSCode](https://code.visualstudio.com/) with the [Jupyter extension](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter).

1. Clone the project

```
  git clone https://github.com/mateorual/MADE_Project_23272477.git
```

2. Go to the project directory

```
  cd MADE_Project_23272477
```

3. Installing Dependencies

```
  pip install -r requirements.txt
```

4. Run the bash script `project/pipeline.sh`

```
  bash project/pipeline.sh
```

This will start a virtual environment and finally create a SQL database out of data sources named `Housing_Tourism_Data.sqlite` in the `\data` directory.

## Running Tests

To run tests, run the following command

```
  bash project/tests.sh
```

---
## Author  
:computer: **Mateo Ruiz Alvarez**  :star: contact me: mateo.a.ruiz@fau.de

---

## Exercises
During the semester you will need to complete exercises using [Jayvee](https://github.com/jvalue/jayvee). You **must** place your submission in the `exercises` folder in your repository and name them according to their number from one to five: `exercise<number from 1-5>.jv`.

In regular intervals, exercises will be given as homework to complete during the semester. Details and deadlines will be discussed in the lecture, also see the [course schedule](https://made.uni1.de/).

### Exercise Feedback
We provide automated exercise feedback using a GitHub action (that is defined in `.github/workflows/exercise-feedback.yml`). 

To view your exercise feedback, navigate to Actions → Exercise Feedback in your repository.

The exercise feedback is executed whenever you make a change in files in the `exercise` folder and push your local changes to the repository on GitHub. To see the feedback, open the latest GitHub Action run, open the `exercise-feedback` job and `Exercise Feedback` step. You should see command line output that contains output like this:

```sh
Found exercises/exercise1.jv, executing model...
Found output file airports.sqlite, grading...
Grading Exercise 1
	Overall points 17 of 17
	---
	By category:
		Shape: 4 of 4
		Types: 13 of 13
```


