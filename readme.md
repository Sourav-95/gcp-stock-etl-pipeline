# Stock Market Analytics Platform (GCP)

## Project Overview
This project is a professional-grade financial data platform built to automate the lifecycle of stock market intelligence on Google Cloud. It transforms raw, fragmented market data into a structured, query-ready analytical warehouse designed for high-scale quantitative analysis.

The core objective is to solve the challenge of "data gravity"—moving large volumes of financial information from external APIs into a centralized cloud environment where it can be cleaned, modeled, and analyzed efficiently. Instead of simple scripts, this project implements a multi-stage data lakehouse approach, ensuring reliable data acquisition, scalable distributed processing, and relational data modeling.

---

## Why These Resources?

In modern data engineering, the choice of tools is driven by scale, cost, and the nature of the workload. Here is the rationale behind the selected GCP stack:

### 1. Google Compute Engine (GCE) | *The Ingestion Engine*
* **Why:** We use GCE to host the Python-based ingestion scripts because it provides a dedicated, customizable environment for API communication.
* **Purpose:** It acts as a bridge between the Yahoo Finance API and our cloud storage. By using a virtual machine, we can manage specific networking requirements and local buffers to ensure that data is fetched and uploaded without interruption.

### 2. Google Cloud Storage (GCS) | *The Immutable Data Lake*
* **Why:** GCS is chosen for its "infinite" scalability and high durability (99.999999999%).
* **Purpose:** It serves as our **Bronze Layer**. Storing the raw API responses (JSON/CSV) directly in GCS ensures we have a permanent record of the original data. If a downstream transformation fails, we can simply re-process from GCS without having to call the API again.

### 3. Google Dataproc | *Distributed Processing at Scale*
* **Why:** Processing thousands of stock tickers over several years of history requires more RAM and CPU than a single machine can provide. Dataproc allows us to spin up a managed Spark cluster in seconds.
* **Purpose:** It handles the heavy lifting—cleaning the data, enforcing schemas, and performing complex joins. We use it for its cost-efficiency; since clusters are ephemeral, we only pay for the compute time required to finish the job.

### 4. Apache Airflow | *The Orchestration Brain*
* **Why:** A pipeline with multiple dependencies (Ingestion -> GCS -> Dataproc -> BigQuery) needs a central "manager" to handle retries, scheduling, and error alerts.
* **Purpose:** Airflow ensures that the Dataproc cluster doesn't start until the data is fully uploaded to GCS, and the BigQuery modeling doesn't start until the Spark job is successful. It provides a single pane of glass for monitoring the entire pipeline health.

### 5. Google BigQuery | *The Serverless Data Warehouse*
* **Why:** BigQuery is a highly performant, columnar database that scales to petabytes. It allows for lightning-fast SQL queries without the need for database administrators.
* **Purpose:** * **Raw Layer (Silver):** Holds the cleaned version of the data.
    * **Analytics Layer (Gold):** We use SQL to separate data into **Fact** (prices/volume) and **Dimension** (company/sector) tables. This Star Schema design is optimized for business intelligence and quantitative research.

---

## Core Value Propositions

* **Automation:** The entire workflow is managed by a centralized orchestrator, reducing manual intervention and ensuring data consistency.
* **Cost-Efficiency:** The system is designed to use ephemeral resources—compute power is spun up only when needed and shut down immediately after processing.
* **Analytical Readiness:** By using SQL-based modeling and views, the project provides an immediate interface for BI tools or data science notebooks to pull metrics like volatility and moving averages without needing to re-process raw files.

## Use Cases
1.  **Backtesting Trading Strategies:** Providing clean historical data for algorithmic testing.
2.  **Portfolio Monitoring:** Tracking performance against historical benchmarks.
3.  **Market Sentiment Analysis:** Preparing the data layer for integration with machine learning models.
