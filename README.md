# Ad Tech Real-Time Data Analysis Pipeline

## Project Overview

This project is an end-to-end real-time data engineering pipeline built for an Ad Tech use case. The goal is to process high-volume advertising events such as ad impressions and ad clicks, join related events in real time, enrich the processed data, store it in a lakehouse architecture, and make it available for analytical querying using AWS Athena.

The pipeline uses AWS Kinesis for real-time data ingestion, AWS Managed Apache Flink for stream processing, AWS Glue with Spark Streaming for post-processing, Apache Iceberg for table management, Amazon S3 for storage, AWS Glue Catalog for metadata management, and AWS Athena for analytical queries.

This project represents an industrial-style streaming architecture commonly used in advertising platforms, marketing analytics systems, clickstream analytics, and real-time customer engagement platforms.

---

## Why I Chose This Project

I chose this project because real-time data processing is one of the most important areas in modern data engineering. Many organizations no longer rely only on batch pipelines. They need to process events as they happen, especially in domains such as advertising, financial transactions, fraud detection, recommendation systems, user behavior tracking, and operational monitoring.

Ad Tech is a strong real-world use case for streaming data because every user interaction can generate an event. When a user sees an advertisement, an impression event is created. If the user clicks the ad, a click event is generated. These two events often arrive through separate streams and need to be joined within a specific time window to understand campaign performance.

This project helped me work with important data engineering concepts such as real-time ingestion, event-time processing, stream joins, schema validation, streaming transformations, lakehouse storage, and SQL-based analytics. It also connects technical implementation with business value by showing how raw event streams can be converted into campaign performance insights.

---

## Business Use Case

In an advertising platform, user interactions continuously generate events from campaigns, publishers, devices, platforms, and geographic locations.

A typical flow looks like this:

1. A user views an advertisement.
2. An ad impression event is generated.
3. The user clicks the advertisement.
4. An ad click event is generated.
5. The impression and click events are matched in real time.
6. The matched data is enriched and stored in a lakehouse table.
7. Analysts query the processed data to understand campaign performance.

The final dataset can be used to analyze:

- Campaign-level performance
- Publisher-level performance
- Click revenue
- User engagement duration
- Platform and device behavior
- Geographic distribution of clicks
- Premium ad performance
- Real-time advertising trends

---

## Tech Stack

| Category | Technology |
|---|---|
| Programming Language | Python |
| Real-Time Data Ingestion | AWS Kinesis Data Streams |
| Stream Processing | AWS Managed Apache Flink, PyFlink |
| Local Build Tool | Apache Maven |
| Post-Processing | AWS Glue, Spark Structured Streaming |
| Storage Layer | Amazon S3 |
| Table Format | Apache Iceberg |
| Metadata Catalog | AWS Glue Catalog |
| Query Engine | AWS Athena |
| Data Format | JSON, Parquet |

---

## End-to-End Architecture

```text
Mock Data Generator (Python)
        |
        v
AWS Kinesis Input Streams
- Ad Impressions Stream
- Ad Clicks Stream
        |
        v
AWS Managed Apache Flink / PyFlink
- Read streaming events
- Validate schemas
- Perform event-time join
- Write joined results
        |
        v
AWS Kinesis Output Stream
- Joined ad events
        |
        v
AWS Glue Spark Streaming
- Parse and validate JSON
- Enrich records
- Calculate derived metrics
- Merge into Iceberg
        |
        v
Apache Iceberg Table on Amazon S3
- Registered in AWS Glue Catalog
        |
        v
AWS Athena
- Run SQL aggregation queries
- Analyze campaign performance
```

---

## Architecture Explanation

The pipeline starts with a Python script that generates mock ad impression and click events. These events are published into two separate AWS Kinesis streams because, in real-world systems, impressions and clicks are usually produced by different services.

AWS Managed Apache Flink consumes both input streams and performs the core real-time processing. The PyFlink application uses the Table API to define source tables, process event-time data, and join impressions with clicks based on matching ad identifiers and a bounded time window.

The joined records are written into an output Kinesis stream. This creates a decoupled design where Flink is responsible for low-latency stream joins, while AWS Glue Spark Streaming handles downstream enrichment and storage.

The Glue Spark Streaming job reads the joined events from Kinesis, validates the records, applies business transformations, calculates metrics such as engagement duration and click revenue, and merges the final records into an Apache Iceberg table stored on Amazon S3.

The Iceberg table is registered in AWS Glue Catalog, which allows AWS Athena to query the processed data using SQL. This makes the final dataset ready for reporting, analytics, and dashboarding.

---

## Implementation Steps

### 1. Set Up Apache Flink and Maven Locally

Apache Flink and Maven are configured in the local development environment to build and test the PyFlink application before deploying it to AWS Managed Apache Flink.

### 2. Set Up Kinesis Streams

The following Kinesis streams are created:

```text
AdImpressionsStreamInput
ClicksStreamInput
AdResultantOutput
```

The first two streams act as input streams, while the third stream stores the joined output generated by the Flink application.

### 3. Create PyFlink Application

The PyFlink application uses the Table API to:

- Create source tables for impression events
- Create source tables for click events
- Create a sink table for joined output
- Apply event-time processing
- Perform a time-bounded stream join
- Write joined records to the output Kinesis stream

### 4. Package Flink Application Using Maven

The Flink application dependencies are packaged locally using Maven. This creates the required dependency JAR file needed by the PyFlink job to communicate with AWS Kinesis.

### 5. Publish Mock Data to Kinesis

The mock data producer publishes simulated impression and click events into the input Kinesis streams. This allows the pipeline to be tested with continuous real-time data.

### 6. Test Flink Application Locally

The Flink application is tested locally to validate Kinesis connectivity, schema definitions, stream join logic, runtime configuration, and output stream writing.

### 7. Deploy to AWS Managed Apache Flink

After local validation, the application is deployed to AWS Managed Apache Flink. Runtime parameters are provided using the application properties configuration file.

### 8. Create AWS Glue Spark Streaming Job

A Glue Spark Streaming job consumes the joined output stream from Kinesis, validates the records, applies transformations, and prepares the data for lakehouse storage.

### 9. Merge Data into Apache Iceberg

The transformed data is merged into an Apache Iceberg table stored on S3. Merge logic helps avoid duplicates and keeps the table updated.

### 10. Query Data Using AWS Athena

AWS Athena is used to run SQL aggregation queries on the Iceberg table through Glue Catalog.

---

## Data Model

### Impression Event

```json
{
  "ad_id": "ad-1001",
  "impression_id": "imp-123456",
  "campaign_id": "cmp001",
  "campaign_name": "Black Friday Sale",
  "publisher_id": "pub001",
  "publisher_name": "TechCrunch",
  "event_time": "2026-05-07T10:15:30",
  "device_type": "smartphone",
  "geo_location": "US",
  "platform": "mobile",
  "ad_type": "banner",
  "bid_price": 12.50
}
```

### Click Event

```json
{
  "ad_id": "ad-1001",
  "impression_id": "imp-123456",
  "campaign_id": "cmp001",
  "campaign_name": "Black Friday Sale",
  "publisher_id": "pub001",
  "publisher_name": "TechCrunch",
  "event_time": "2026-05-07T10:15:45",
  "device_type": "smartphone",
  "geo_location": "US",
  "platform": "mobile",
  "click_price": 4.25
}
```

### Enriched Output Record

```json
{
  "ad_id": "ad-1001",
  "impression_id": "imp-123456",
  "campaign_id": "cmp001",
  "publisher_id": "pub001",
  "impression_time": "2026-05-07T10:15:30",
  "click_time": "2026-05-07T10:15:45",
  "geo_location": "US",
  "platform": "mobile",
  "device_type": "smartphone",
  "bid_price": 12.50,
  "click_price": 4.25,
  "is_premium_ad": "Yes",
  "engagement_duration": 15,
  "platform_category": "Mobile",
  "click_revenue": 5.10,
  "processed_time": "2026-05-07T10:15:50"
}
```

---

## Main Components

| File | Description |
|---|---|
| `mock_data_gen.py` | Generates mock ad impression and click events and publishes them to AWS Kinesis |
| `main.py` | PyFlink streaming application that reads, validates, joins, and writes streaming data |
| `glue_post_processing_streaming_job.py` | AWS Glue Spark Streaming job that consumes joined data and writes enriched records into Iceberg |
| `application_properties.json` | Runtime configuration for AWS Managed Flink |
| `pom.xml` | Maven build file for packaging dependencies |
| `assembly.xml` | Maven assembly configuration for dependency packaging |

---

## Project Structure

```text
ad-tech-real-time-data-analysis/
│
├── README.md
├── main.py
├── mock_data_gen.py
├── glue_post_processing_streaming_job.py
├── application_properties.json
├── pom.xml
├── assembly.xml
│
├── architecture/
│   └── architecture-diagram.png
│
├── queries/
│   └── athena_queries.sql
│
├── sample_data/
│   └── sample_enriched_ad_data.csv
│
└── screenshots/
    ├── kinesis-streams.png
    ├── flink-job.png
    ├── glue-job.png
    ├── iceberg-table.png
    └── athena-query-results.png
```

---

## Athena Query Examples

### Total Revenue by Campaign

```sql
SELECT
    campaign_id,
    ROUND(SUM(click_revenue), 2) AS total_revenue
FROM ad_campaign.enriched_ad_data
GROUP BY campaign_id
ORDER BY total_revenue DESC;
```

### Average Engagement Duration by Platform

```sql
SELECT
    platform_category,
    ROUND(AVG(engagement_duration), 2) AS avg_engagement_duration
FROM ad_campaign.enriched_ad_data
GROUP BY platform_category
ORDER BY avg_engagement_duration DESC;
```

### Click Revenue by Geo Location

```sql
SELECT
    geo_location,
    ROUND(SUM(click_revenue), 2) AS total_click_revenue,
    COUNT(*) AS total_clicks
FROM ad_campaign.enriched_ad_data
GROUP BY geo_location
ORDER BY total_click_revenue DESC;
```

### Publisher-Level Performance

```sql
SELECT
    publisher_id,
    COUNT(*) AS total_clicks,
    ROUND(SUM(click_revenue), 2) AS total_revenue,
    ROUND(AVG(click_price), 2) AS avg_click_price
FROM ad_campaign.enriched_ad_data
GROUP BY publisher_id
ORDER BY total_revenue DESC;
```

### Premium vs Non-Premium Ad Performance

```sql
SELECT
    is_premium_ad,
    COUNT(*) AS total_records,
    ROUND(SUM(click_revenue), 2) AS total_revenue,
    ROUND(AVG(engagement_duration), 2) AS avg_engagement_duration
FROM ad_campaign.enriched_ad_data
GROUP BY is_premium_ad;
```

---

## Key Features

- Real-time ad impression and click event simulation
- Kinesis-based streaming ingestion
- Two-stream processing architecture
- PyFlink Table API implementation
- Event-time stream join
- Maven-based Flink dependency packaging
- AWS Managed Apache Flink deployment
- Runtime parameter configuration
- Spark Streaming post-processing using AWS Glue
- JSON parsing and schema validation
- Data enrichment and derived metric calculation
- Merge operation into Apache Iceberg table
- S3-based lakehouse storage
- Glue Catalog metadata registration
- Athena-based SQL analytics

---

## Challenges Solved

### Real-Time Stream Joining

The project joins impression and click events arriving from two different streams. This requires event-time logic and a bounded time window so that clicks can be matched with the correct impressions.

### Dependency Packaging

The Flink application requires external connectors to communicate with AWS Kinesis. Maven is used to package the required dependencies for local and managed execution.

### Decoupled Streaming Design

The pipeline separates real-time joining and post-processing into different stages. Flink handles low-latency stream joins, while Glue Spark Streaming handles enrichment and lakehouse writes.

### Lakehouse Storage

Instead of writing raw files directly to S3, the project uses Apache Iceberg to manage analytical data as structured tables. This makes the data easier to query, maintain, and evolve.

### Analytical Querying

The final dataset is registered in Glue Catalog and queried using Athena, allowing business-level aggregations without managing database infrastructure.

---

## Possible Dashboard Extension

The processed Iceberg table can be connected to a dashboarding layer using Amazon QuickSight, Streamlit, Power BI, or Tableau.

Recommended dashboard metrics include:

- Total click revenue
- Total clicks
- Average bid price
- Average click price
- Average engagement duration
- Revenue by campaign
- Revenue by publisher
- Revenue by geo location
- Engagement by platform
- Premium vs non-premium ad performance

---

## Future Enhancements

- Add real-time dashboard using Amazon QuickSight
- Add Streamlit dashboard for local portfolio demonstration
- Add Terraform scripts for AWS infrastructure provisioning
- Add Docker support for local development
- Add CI/CD pipeline using GitHub Actions
- Add error handling and dead-letter stream for failed records
- Add data quality checks using AWS Glue Data Quality or Great Expectations
- Add fraud detection logic for suspicious click patterns
- Add campaign-level click-through rate calculation
- Add CloudWatch monitoring and alerting
- Add schema registry support for better event governance

---

## Skills Demonstrated

This project demonstrates hands-on experience with:

- Real-time data engineering
- AWS streaming services
- Apache Flink
- PyFlink Table API
- Apache Maven
- AWS Managed Apache Flink
- AWS Kinesis Data Streams
- AWS Glue Spark Streaming
- Apache Spark
- Apache Iceberg
- Amazon S3 data lake architecture
- AWS Glue Catalog
- AWS Athena
- Event-time processing
- Stream joins
- Lakehouse table design
- SQL analytics
- Cloud-based data pipeline development

---

## Conclusion

This project implements a complete real-time Ad Tech data analysis pipeline using modern cloud data engineering tools. It starts from event generation, processes streaming data through Kinesis and Flink, enriches the output using Glue Spark Streaming, stores the final data in Apache Iceberg on S3, and enables analytical querying through Athena.

The project demonstrates how real-time advertising data can be converted into analytics-ready business insights using an end-to-end streaming lakehouse architecture.
