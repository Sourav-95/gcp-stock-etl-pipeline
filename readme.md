===========================================================================
STOCKS DATA ENGINEERING PIPELINE - GCP ARCHITECTURE
===========================================================================

[ SOURCE ]
    |
    | (1) REST API Call
    v
[ COMPUTE ENGINE (GCE) ] ---------------------> [ GOOGLE CLOUD STORAGE ]
    |  - Runs ingestion/main.py                     (Bronze Layer)
    |  - Fetches from Yahoo Finance                 - Raw Parquet/JSON Files
    |  - Local Buffer & Upload                      - Immutable History
    |                                                      |
    |                                                      |
[ APACHE AIRFLOW ] <---------------------------------------+
    |  - Orchestrates tasks                                |
    |  - Monitors job status                               | (2) Spark Job
    |                                                      v
    |                                           [ DATAPROC CLUSTER ]
    |                                              - PySpark Processing
    |                                              - Schema Enforcement
    |                                              - Data Cleaning
    |                                              - Feature Engineering
    |                                                      |
    |                                                      | (3) Load
    v                                                      v
[ BIGQUERY DATA WAREHOUSE ] <------------------------------+
    |
    +--- [ RAW LAYER (Silver) ]
    |    - Staging tables
    |
    +--- [ ANALYTICS LAYER (Gold) ]
    |    - Fact Tables (Price/Volume)
    |    - Dimension Tables (Company/Sector)
    |
    +--- [ REPORTING VIEWS ]
         - Moving Averages
         - Performance Metrics
===========================================================================