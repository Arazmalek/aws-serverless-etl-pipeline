# 🌟 Enterprise-Grade Serverless Data Lake Platform (AWS ETL/MLOps Foundation)

## EXECUTIVE SUMMARY
This repository showcases a highly resilient, cost-optimized **Serverless Data Lake Architecture** designed for high-throughput ETL processing and mission-critical financial data validation. The system automates the ingestion, transformation (using AWS Glue/Spark), and cataloging of proprietary client data, setting a robust, governance-focused foundation for advanced analytics and MLOps initiatives.

**🎯 Core Business Achievement: Zero-Touch Automation**
The bespoke transformation logic successfully automated the complex extraction and validation of sensitive, multi-source financial reports. This solution resulted in a **99% reduction in manual data processing time** and enabled real-time reporting accuracy for the BI team.

---

## 🏗️ SYSTEM ARCHITECTURE & DATA FLOW
The diagram illustrates the decoupled, event-driven architecture, highlighting the flow from secure client upload to BI consumption.

<img width="1034" height="451" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/9f6d01a7-40c8-47d6-9f64-d81bc04dcfcd" />

---

## ⚙️ TECHNICAL SPECIFICATIONS & ARCHITECTURAL CHOICES

### 1. SECURE & DECOUPLED INGESTION
* **AWS API Gateway & Lambda:** Utilized for a high-throughput, event-driven entry point. The use of **Presigned URLs** enforces strict security protocols, limiting direct bucket access and ensuring data integrity upon arrival (Ingress Control).
* **Ingestion Logic (Python):** Lambda functions are responsible for initial validation and writing raw files to the **S3 Raw Layer**, establishing the Single Source of Truth (SSOT).

### 2. DATA LAKE CORE & GOVERNANCE
* **AWS S3 (Multi-Tiered):** Data is organized into segregated Raw and Processed (Clean) S3 buckets, optimizing cost and governance.
* **AWS Glue Data Catalog & Crawler:** Implemented for **Schema-on-Read** capability. The Catalog acts as the central Metadata Repository, providing structured access to data for all consumption tools.

### 3. ADVANCED TRANSFORMATION (ETL)
* **AWS Glue Spark (PySpark):** PySpark scripts are executed in a managed Glue environment for parallel and distributed processing. This ensures **linear scalability** for large datasets and maximizes cost-efficiency by paying only for compute time.
* **Validation Engine:** The core transformation logic includes advanced data quality checks and specialized validation rules for complex financial datasets.

## 🚀 DEPLOYMENT & MLOPS FOUNDATION

| Feature | Tool / Service | Senior Justification |
| :--- | :--- | :--- |
| **Containerization** | **Docker** | Used for local development, dependency management, and ensuring environment parity between local development and the **AWS deployment environment**. |
| **Infrastructure as Code (IaC)** | **CloudFormation / SAM** | The entire AWS infrastructure (Lambda, API Gateway, Glue Jobs) is defined and deployed using IaC templates (located in the `/infra` folder), ensuring automated, repeatable, and version-controlled deployment. |
| **Monitoring & Logging** | **AWS CloudWatch & Grafana** | Integrated metrics and detailed logging via CloudWatch for proactive pipeline health checks, performance alerting, and historical data analysis, visualized via Grafana dashboards. |

---
**Araz Malekazari** | *Senior Data Engineer & System Architect*
