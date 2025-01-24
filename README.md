# Spotify Data Warehouse

This project aims to design and implement a **Data Warehouse** for **Spotify** using data from the Spotify platform and Kaggle for historical data. The primary objective is to analyze and process large volumes of Spotify data to derive meaningful insights using a modern data engineering approach. The project utilizes various data processing and transformation techniques, such as ETL (Extract, Transform, Load), data warehousing, and data analysis.

## Table of Contents
1. Project Overview
2. Technologies Used
3. Getting Started
4. Project Structure
5. Data Source


## Project Overview

The goal of this project is to create a data warehouse using Spotify's public data, focusing on the following:

- **Data Extraction**: Extract data from Spotify APIs or public datasets.
- **Data Transformation**: Clean, normalize, and aggregate the data to make it suitable for analysis.
- **Data Loading**: Store the processed data in a data warehouse system (e.g., AWS Redshift, Google BigQuery).
- **Analysis**: Use the processed data to derive insights, such as music trends, popular tracks, artist popularity, etc.

This project is built as part of the **DATA226** course on Data Engineering.

## Technologies Used

- **Python**: Programming language for scripting and data manipulation.
- **SQL**: For querying and managing data in the data warehouse.
- **Apache Airflow**: For automating ETL pipelines.
- **Snowflake** : Data warehouse For storing and querying large datasets.
- **Pandas / NumPy**: For data manipulation and processing.
- **Spotify API**: For accessing Spotify data such as song details, user preferences, etc.

## Getting Started

### Prerequisites

- Python 3.x installed
- Required Python libraries are present docker_compose_min.yaml
- Access to Spotify API (API key)

### System Diagram
<img src="https://github.com/user-attachments/assets/5bf98172-d9ee-4548-83c0-b89f72ad0296" width="600"/>


## Project Structure


### Data Model

We have created below data model for Spotify metadata.

<img src="https://github.com/user-attachments/assets/ed0052f7-daec-403b-929a-9601e4ae03a0" width="600"/>


## Data Source

The dataset includes metadata, artist details, and audio features for Spotify tracks, comprising **35 columns** (categorical and numerical) and approximately **1 million rows**.

### Source
- **Spotify API**
- **Kaggle**  

### Key Columns
- **Artist Metadata**: Information about the artist(s) associated with each track.  
- **Song Information**: Track name, album name, and other descriptive details.  
- **Audio Features**: Acoustic properties like tempo, danceability, energy, and more.  
- **Song Release Date**: Date when the track was first released.  
- **Popularity**: Spotify's popularity index for the track.  
- **Ranking**: Song ranking in various regions.  
- **Region**: Geographical data representing where the song is trending.


## Analysis

We have created Power BI dashboard on top of dbt models to analyse trends.

<img src="https://github.com/user-attachments/assets/2bbad158-c618-4ddb-bbfe-8324ecaa7a64" width="600"/>


