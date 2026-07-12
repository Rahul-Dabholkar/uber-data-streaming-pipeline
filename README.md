# Uber Real-Time Ingestion & Analytics Pipeline

An end-to-end real-time data engineering project demonstrating dynamic metadata-driven ingestion, stream-static joins, Slowly Changing Dimensions (SCD) Type 2 tracking, and watermarked aggregations using **Azure Data Factory**, **ADLS Gen2**, **Azure Event Hubs**, and **Azure Databricks (Delta Live Tables & Unity Catalog)**.

---

## 1. System Architecture

The pipeline uses a hybrid medallion architecture combining batch lookup data with real-time ride transactions:

```mermaid
graph TD
    A[GitHub / Static Config] -->|HTTP Metadata| B(Azure Data Factory)
    B -->|Ingest Raw JSON| C[ADLS Gen2 Raw Zone]
    C -->|Batch Read| D[Databricks Bronze Layer]
    
    E[Real-Time Simulator scripts/data.py] -->|OAuth Event Stream| F[Azure Event Hub]
    F -->|Spark Kafka Connector| G[DLT Ingest ingest.py]
    G -->|Streaming Ingestion| H[Bronze rides_raw]
    
    H -->|Combine with historical| I[Bronze stg_rides]
    I -->|Watermarked Stream-Static Join| J[Silver silver_obt]
    
    J -->|DLT CDC SCD Type 2| K[Gold dim_passenger]
    J -->|DLT CDC SCD Type 2| L[Gold dim_driver]
    J -->|DLT CDC SCD Type 2| M[Gold dim_location]
    J -->|DLT CDC SCD Type 1| N[Gold dim_booking]
    J -->|DLT CDC SCD Type 1| O[Gold dim_vehicle]
    J -->|Gold Star Schema| P[Gold fact]
    J -->|Sliding Watermark Aggregation| Q[Gold gold_hourly_city_metrics]
```

### Existing Azure Infrastructure Setup
Here is the active resource group overview supporting this pipeline in Azure:

![Azure Resource Group](docs/azure/resource-group.png)

---

## 2. Ingestion & Orchestration Layer

### Batch Lookup Ingestion (ADF)
* **Metadata-driven Copy:** A Lookup activity reads [config.json](file:///g:/LIVE/uber-data-pipeline/config.json) containing static metadata categories and drives a ForEach loop, running parameterized HTTP-to-Blob Copy activities to dump lookup tables into ADLS Gen2.
* **Storage Structure:** Maps configurations (`map_cities.json`, `map_payment_methods.json`, etc.) to the ADLS Bronze layer container.

![Azure Data Factory Pipeline](docs/azure/data_factory/data-factory-pipeline.png)
![ADLS Gen2 Folder Structure](docs/azure/data_lake/data-lake-container-data-files.png)

### Real-Time Stream Ingestion (Event Hub)
* **Event Simulator:** The Python script [scripts/data.py](file:///g:/LIVE/uber-data-pipeline/scripts/data.py) draws from static pools of drivers and passengers, generating ride confirmations with a 10-15% chance of updating details (ratings, email, phone) to simulate real-world updates.
* **Message broker:** Pushed dynamically via [scripts/connection.py](file:///g:/LIVE/uber-data-pipeline/scripts/connection.py) to Azure Event Hub.

![Event Hub Topic](docs/azure/events_hub/events-hub-event-topic.png)
![Event Hub Ingestion Rate](docs/azure/events_hub/events-hub-events.png)

---

## 3. Processing & Medallion Layer (Delta Live Tables)

The pipeline is defined as a unified **Databricks Delta Live Tables (DLT)** pipeline following portable, environment-independent practices:

### A. Bronze Layer
* [ingest.py](file:///g:/LIVE/uber-data-pipeline/databricks/uber-rides-ingestion-pipeline/transformations/ingest.py): Uses the Spark-Kafka streaming connector to read from Event Hub and lands raw bytes cast to String in `rides_raw`.
* [silver.py](file:///g:/LIVE/uber-data-pipeline/databricks/uber-rides-ingestion-pipeline/transformations/silver.py): Defines `stg_rides`, merging historical records from `bulk_rides` with the parsed real-time stream `rides_raw` using `append_flow`.

### B. Silver Layer
* [silver-obt.sql](file:///g:/LIVE/uber-data-pipeline/databricks/uber-rides-ingestion-pipeline/transformations/silver-obt.sql): Implements a stream-static join between `stg_rides` (stream) and the static dimension lookup tables (`map_cities`, `map_vehicle_types`, etc.).
* **Watermarking:** Sets a 3-minute delay watermark on `booking_timestamp` to handle late-arriving stream logs correctly.

### C. Gold Layer (Dimensional Modeling & SCD 2)
* [model.py](file:///g:/LIVE/uber-data-pipeline/databricks/uber-rides-ingestion-pipeline/transformations/model.py): Defines the star schema fact and dimension tables:
  * **SCD Type 1 (Overwrite):** Applied to low-volatility tables (`dim_payment`, `dim_vehicle`, `dim_booking`) and `fact`.
  * **SCD Type 2 (History Tracking):** Tracks temporal changes in `dim_location` (region shifts), `dim_passenger` (email/phone updates), and `dim_driver` (rating changes).
  * **Streaming Aggregator:** Implements `gold_hourly_city_metrics`, utilizing a 1-hour tumbling window and a 10-minute watermark to track real-time fare trends and cancellation rates.

---

## 4. Execution & Validation

### DLT Pipeline Run UI
Here is the compiled DLT DAG in Databricks showing the processing nodes passing checks successfully:

![DLT Pipeline Graph](docs/databricks/dlt-pipeline-dag.png)

### Verifying SCD Type 2 History
SCD Type 2 history tracking can be verified by running the validation notebook [silver-obt.ipynb](file:///g:/LIVE/uber-data-pipeline/databricks/silver-obt.ipynb). 

#### 1. Location Dimension Updates
When a city name shifts from "New York" to "Super New York", the pipeline creates two rows for `pickup_city_id = 1`—closing out the active flag of the old entry and inserting the new active record:

![Locations SCD2 History](docs/databricks/scd2-locations-query.png)

#### 2. Streaming Driver Rating Updates
When the simulator updates driver ratings during the stream, SCD Type 2 captures the old and new ratings:

![Drivers SCD2 History](docs/databricks/scd2-drivers-query.png)

#### 3. Real-Time Streaming Aggregations
Validation of hourly sliding aggregates per city showing completed/cancelled counts and fare averages:

![Streaming Aggregations](docs/databricks/streaming-aggregation-metrics.png)

#### 4. Unity Catalog Lineage Graph
The end-to-end data lineage graph automatically captured and displayed in Unity Catalog Explorer:

![Unity Catalog Lineage](docs/databricks/unity-catalog-lineage.png)

---

## 5. Local Setup Instructions

1. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Environment Configuration:**
   Configure a `.env` file in the `scripts/` folder:
   ```ini
   CONNECTION_STRING=your-event-hub-connection-string
   EVENT_HUBNAME=ubertopic
   ```
3. **Execute Simulator Stream:**
   ```bash
   python scripts/connection.py
   ```
4. **Force Specific Update Event:**
   To force a ride event for a specific city update (e.g. for testing SCD Type 2 on "Super New York"), run:
   ```bash
   python scripts/send_force_ride.py
   ```
