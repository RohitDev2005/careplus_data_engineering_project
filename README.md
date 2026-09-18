# CarePlus — Serverless Data Engineering Pipeline

This repository contains an end-to-end **serverless data engineering pipeline** built using **Python and AWS**.  
The project demonstrates how support ticket data and application logs are ingested, processed, transformed, stored, and made available for analytics and reporting.

The pipeline uses an **event-driven architecture** with Amazon S3, AWS Lambda, AWS Glue, Amazon Redshift Serverless, Amazon Athena, and Power BI.

This project is built as a **portfolio project** to showcase practical skills in:

- Python
- AWS Cloud Services
- Data Engineering
- ETL Development
- Data Ingestion
- Data Transformation
- Data Warehousing
- SQL Analytics
- Business Intelligence

---

## 🏗️ Data Architecture

The pipeline follows a **Raw → Processed → Warehouse → Analytics** architecture.

### Support Tickets

```text
MySQL → Python → S3 Raw → Lambda → Glue ETL
                                      ↓
                           S3 Processed (Parquet)
                                      ↓
                            Redshift / Athena
                                      ↓
                                  Power BI
```

### Application Logs

```text
Log Files → Python → S3 Raw → Lambda
                              ↓
                   S3 Processed (Parquet)
                              ↓
                       Redshift / Athena
                              ↓
                           Power BI
```

---

## 📌 Project Overview

The project covers the complete lifecycle of a cloud-based data pipeline:

1. **Data Ingestion**
   - Support ticket data is extracted from MySQL using Python.
   - Daily application log files are ingested using Python.
   - Raw data is stored in Amazon S3.

2. **Event-Driven Processing**
   - S3 events trigger AWS Lambda when new files arrive.
   - Lambda starts the Glue ETL workflow for support tickets.
   - Lambda directly processes application logs.

3. **Data Transformation**
   - Support tickets are transformed using AWS Glue Visual ETL.
   - Application logs are parsed using Python, Pandas, PyArrow, and regular expressions.
   - Invalid records, duplicates, null values, and inconsistent values are handled.

4. **Processed Data Storage**
   - Transformed datasets are stored in Amazon S3.
   - Processed data is stored in **Parquet format**.

5. **Data Warehousing**
   - Processed Parquet data is loaded into Amazon Redshift Serverless.
   - Separate warehouse tables are maintained for support tickets and application logs.

6. **Analytics**
   - Amazon Athena is used for ad-hoc SQL analysis directly on processed S3 data.
   - Power BI connects to Redshift using Import mode for dashboard reporting.

---

## 🛠️ Technology Stack

- **Programming:** Python, SQL
- **Source Database:** MySQL
- **Cloud Storage:** Amazon S3
- **Event Trigger:** Amazon S3 Events
- **Serverless Processing:** AWS Lambda
- **ETL:** AWS Glue Visual ETL
- **Data Format:** Apache Parquet
- **Data Processing:** Pandas, PyArrow
- **Data Warehouse:** Amazon Redshift Serverless
- **Analytics:** Amazon Athena
- **Visualization:** Microsoft Power BI

---

## 🔄 Pipeline Workflow

### 🎫 Support Ticket Pipeline

Support-ticket data is extracted from **MySQL using Python** and uploaded to the **S3 Raw layer as CSV files**.

When a new file arrives, an **S3 event triggers AWS Lambda**. Lambda identifies the uploaded file and starts the **AWS Glue Visual ETL** job while passing the input S3 path to Glue.

The Glue workflow performs:

- Schema mapping and type conversion
- Null handling
- Column renaming
- Invalid-record filtering
- Priority standardization
- Basic data-quality validation
- Selection of required analytical columns

The transformed data is stored in the **S3 Processed layer as Parquet**.

---

### 📋 Application Log Pipeline

Application logs are generated as daily `.log` files and ingested into the S3 Raw layer using Python.

A **date tracker** determines the next log file to process, allowing the ingestion process to work incrementally.

When a new log file arrives, an **S3 event triggers AWS Lambda**. Unlike the support-ticket pipeline, the log Lambda performs the transformation directly.

The Lambda workflow performs:

- Log extraction using regular expressions
- Data type conversion
- Invalid response-time filtering
- Log-level standardization
- Duplicate removal
- Boolean conversion
- Parquet generation

The transformed logs are stored in the **S3 Processed layer**.

---

## 🧱 Data Layers

### 📥 Raw Layer

The **S3 Raw layer** stores incoming data before transformation.

It contains:

- Support ticket CSV files
- Application log files

This layer preserves the source data used by downstream processing.

### ⚙️ Processed Layer

The **S3 Processed layer** contains cleaned and transformed datasets in **Apache Parquet format**.

Transformations include data type conversion, standardization, invalid-record filtering, duplicate removal, and data-quality checks.

### 🗄️ Warehouse Layer

Processed Parquet data is loaded into **Amazon Redshift Serverless** using SQL `COPY` commands.

Main warehouse tables:

```text
support_tickets
support_logs
```

### 📊 Analytics Layer

- **Amazon Athena** is used for ad-hoc SQL analysis directly on processed S3 data.
- **Power BI** connects to Redshift using Import mode to provide interactive dashboards.

The dashboards cover areas such as support operations, ticket resolution, response times, system performance, and application log activity.

---

## ✨ Key Project Highlights

- Python-based data ingestion from MySQL and daily log files
- Incremental log ingestion using a date tracker
- Event-driven S3 → Lambda processing
- AWS Glue Visual ETL for support-ticket transformation
- Regex-based semi-structured log parsing
- Data cleaning and quality validation
- Parquet-based processed data storage
- Amazon Redshift Serverless data warehouse
- Amazon Athena ad-hoc SQL analytics
- Power BI dashboards for operational analysis

---

## 🤖 Development Note

This is a **learning and portfolio project** developed using a combination of hands-on implementation, teacher-guided learning, and AI-assisted development.

AI tools and instructional material were used to understand concepts, develop parts of the implementation, troubleshoot issues, and build the pipeline incrementally.

The project focuses on understanding the **architecture, AWS services, data flow, transformations, SQL, and analytics workflow**, rather than claiming that every line of code was independently written from scratch.

The support-ticket transformation workflow was designed using **AWS Glue Visual ETL**, which generated the corresponding Glue/PySpark script.

---

## 🔐 Security Note

Credentials, passwords, access keys, tokens, and sensitive configuration values are excluded from the repository.

Project-specific infrastructure identifiers in public SQL scripts are replaced with placeholders where appropriate.

> **Note:** This project is intended for learning and portfolio purposes. Sensitive or real customer information should not be published in a public repository.

---

## 🌟 About Me

Hi! I’m a Computer Engineering student with a strong interest in **Data Analytics and Data Engineering**.  
I enjoy working with SQL, data modeling, and building end-to-end data pipelines.  
This project reflects my hands-on learning and my goal of building reliable, analytics-ready data systems.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge\&logo=linkedin\&logoColor=white)](https://www.linkedin.com/in/rohit-devshatwar-178249296)  
[![LeetCode](https://img.shields.io/badge/LeetCode-FFA116?style=for-the-badge\&logo=leetcode\&logoColor=black)](https://leetcode.com/u/Rohit_Devshatwar/)  
[![GeeksforGeeks](https://img.shields.io/badge/GeeksforGeeks-2F8D46?style=for-the-badge\&logo=geeksforgeeks\&logoColor=white)](https://www.geeksforgeeks.org/profile/devshatwxqs5)
