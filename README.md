# epl_data_pipeline
End-to-end EPL Data Engineering Pipeline built using Databricks, PySpark, Delta Lake, and Auto Loader with a Bronze-Silver-Gold architecture

PROJECT OVERVIEW

This project implements an end-to-end English Premier League (EPL) data engineering pipeline using Databricks, PySpark, Delta Lake, and Auto Loader.

The project uses synthetic football datasets designed to simulate a realistic data engineering environment. The datasets follow different data arrival patterns. Teams and players data are treated as periodically updated datasets, while match data is treated as a daily arriving dataset.

The pipeline follows a Bronze-Silver-Gold architecture. The Bronze layer handles data ingestion, the Silver layer performs data cleaning and transformation, and the Gold layer provides analytics-ready data for reporting and visualization.

The complete pipeline is automated using a Databricks Job that is scheduled to run daily.

PROJECT ARCHITECTURE

Source Files

↓

Auto Loader

↓

Bronze Layer

Raw and Ingested Data

↓

Silver Layer

Cleaned and Transformed Data

↓

Gold Layer

Analytics-Ready Data

↓

Power BI / Analytics

DATA ARRIVAL PATTERN

The project simulates a realistic football data environment in which different datasets arrive at different frequencies.

Teams data is treated as a periodically updated dataset.

Players data is treated as a periodically updated dataset.

Matches data is treated as a daily arriving dataset.

Dataset	Data Arrival Frequency
Teams	Twice a year
Players	Twice a year
Matches	Daily

The Databricks Job is scheduled to run daily so that newly arriving match data can be processed automatically.

TECHNOLOGY STACK

Databricks

PySpark

Python

Apache Spark

Delta Lake

Auto Loader

Databricks Jobs

SQL

Power BI

GitHub

PROJECT STRUCTURE

The project contains three main Databricks processing notebooks.

teams_processing

players_processing

matches_processing

Each notebook is responsible for processing a specific dataset and performing the required data engineering operations.

BRONZE LAYER

The Bronze layer is responsible for ingesting the source datasets into Databricks.

Auto Loader is used for incremental file ingestion. It allows newly arriving files to be detected and processed without repeatedly processing previously handled files.

The Bronze layer maintains the incoming data with minimal transformation and provides the foundation for downstream processing.

The main datasets processed in the Bronze layer are Teams, Players, and Matches.

Metadata such as source file information and ingestion timestamps can also be maintained to support data tracking and monitoring.

SILVER LAYER

The Silver layer is responsible for cleaning, validating, and transforming the data received from the Bronze layer.

The main operations performed in the Silver layer include data type conversion, null handling, duplicate removal, data standardization, column transformations, and data validation.

The Silver layer converts raw source data into structured and reliable datasets that can be used for further analytical processing.

Data quality checks are applied during the transformation process to improve the reliability of downstream data.

GOLD LAYER

The Gold layer contains analytics-ready tables designed for reporting and business analysis.

The project follows a dimensional modeling approach using fact and dimension tables.

The main Gold tables include:

dim_teams

dim_players

dim_date

fact_matches

The dimension tables contain descriptive information, while the fact table contains match-related information and measures.

The Gold layer provides a structured data model that can be directly consumed by reporting and visualization tools.

DIMENSION TABLES

The project contains multiple dimension tables that provide descriptive information for analytical queries.

DIM_TEAMS

The dim_teams table contains information related to EPL teams.

It provides team-level attributes that can be used to analyze match performance and compare teams.

DIM_PLAYERS

The dim_players table contains information related to players.

It provides player-level attributes that can be used for player-related analysis.

DIM_DATE

The dim_date table provides standardized date information for analytical reporting.

It can contain attributes such as date, month, year, and quarter.

The date dimension allows match data to be analyzed across different time periods and makes date-based reporting easier.

FACT TABLE
FACT_MATCHES

The fact_matches table contains match-level information.

It stores important match-related information such as participating teams, goals, and other available match attributes.

The fact table can be connected with the relevant dimension tables to support analytical queries and dashboard development.

DATA ENGINEERING WORKFLOW

The complete data processing workflow follows a structured pipeline.

Source files are received according to their respective data arrival frequencies.

Auto Loader detects newly arriving files and ingests the data into the Bronze layer.

The Bronze data is then processed using PySpark transformations.

The cleaned and validated data is stored in the Silver layer.

The processed Silver data is then transformed into analytical fact and dimension tables in the Gold layer.

The Gold tables can then be connected to Power BI or other analytical tools.

NOTEBOOK DESCRIPTION
TEAMS_PROCESSING

The teams_processing notebook handles the ingestion and processing of EPL team data.

The notebook performs the required data loading, transformation, cleaning, and duplicate handling operations before preparing the data for downstream processing.

PLAYERS_PROCESSING

The players_processing notebook handles the ingestion and processing of EPL player data.

The notebook performs data cleaning, standardization, validation, and duplicate handling to prepare reliable player information.

MATCHES_PROCESSING

The matches_processing notebook handles the daily processing of EPL match data.

Since match data is simulated as arriving daily, the notebook processes newly arriving match files incrementally and applies the required transformations before making the data available for analytical processing.

AUTOMATION

The pipeline is automated using Databricks Jobs.

The Databricks Job is scheduled to run daily and executes the required processing notebooks.

The automated workflow reduces manual intervention and allows newly arriving data to be processed on a regular basis.

The overall execution flow can be represented as:

Databricks Job

↓

Teams Processing

↓

Players Processing

↓

Matches Processing

↓

Bronze Layer

↓

Silver Layer

↓

Gold Layer

↓

Analytics

INCREMENTAL DATA PROCESSING

Incremental processing is an important part of the project.

The project simulates different data arrival frequencies for different datasets.

Teams and players data are treated as periodically updated datasets, while match data is treated as a daily arriving dataset.

Auto Loader is used to support incremental file ingestion. This allows newly arriving files to be processed without unnecessarily reprocessing all existing source files.

This approach makes the pipeline more suitable for a realistic data engineering environment.

DATA CLEANING AND TRANSFORMATION

The Silver layer performs the main data cleaning and transformation activities.

The processing includes:

Data type conversion

Null value handling

Duplicate detection

Duplicate removal

Data standardization

Column transformation

Data validation

Business rule implementation

The transformation process improves data quality before the information is moved to the Gold layer.

DIMENSIONAL MODELING

The Gold layer follows a fact and dimension modeling approach.

The dimension tables provide descriptive information about teams, players, and dates.

The fact_matches table contains match-related information.

This structure allows analytical queries to combine descriptive attributes with match-level measures.

The model can be used to analyze team performance, match results, goals scored, home and away performance, and seasonal trends.
