# Azure-End-to-End-ETL-Pipeline-Project

I built a comprehensive end-to-end ETL pipeline using Azure’s modern data stack, processing the Amazon Prime Movies & TV Shows dataset (Kaggle) to enable data-driven storytelling and analytics.

## Project Highlights:

#### Azure Data Factory: Automated ingestion of raw data from HTTP sources into Azure Data Lake Storage Gen2, ensuring secure and scalable data landing.

#### Azure Data Lake Storage Gen2: Implemented the Medallion Architecture (Bronze, Silver, Gold) to systematically organize raw, refined, and curated data layers for efficient processing and governance.

#### Azure Databricks (PySpark): Performed robust data cleaning, transformation, and enrichment, handling missing values, standardizing formats, and preparing data for analytics.

#### Azure Synapse Analytics: Enabled advanced querying and analytics on curated datasets, facilitating seamless integration with downstream BI tools.

#### Power BI: Connected to the Gold layer in ADLS/Synapse, creating interactive dashboards to visualize key metrics—genre distribution, release trends, ratings analysis, and more.

Tech Stack:
Azure Data Factory | Azure Data Lake Gen2 | Azure Databricks (PySpark) | Azure Synapse Analytics | Power BI

Dataset:
Kaggle – Amazon Prime Movies & TV Shows: 10,000+ titles with metadata (cast, director, ratings, release year, duration, etc.)


