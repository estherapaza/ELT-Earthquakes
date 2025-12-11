# Author Esther Guadalupe Apaza Hacho


# 🌟 The ELT Pipeline for Earthquakes Risk Analysis

This document outlines the design, implementation, and analytical output of the **Earthquakes Event ELT Pipeline**, a robust data solution developed by **Esther Guadalupe Apaza Hacho**.

At its core, this project directly confronts a critical global challenge: the need for rapid, accurate **seismic risk assessment** in volatile regions. Leveraging modern data engineering principles, the pipeline simulates an hourly ingestion of real-world seismic data to produce actionable intelligence for disaster management.

## Project Summary and Core Value

| Component | Description | Key Result |
| :--- | :--- | :--- |
| **Architecture** | Implements a complete **Extract, Load, Transform (ELT)** paradigm.  | **Data Integrity** is guaranteed by preserving the raw, untransformed source data. |
| **Orchestration** | Utilizes **Apache Airflow** (simulated DAG) for reliable scheduling of data tasks (every 6 hours). | Ensures **Timeliness and Consistency** of risk data updates. |
| **Technology** | Employs SQL-based transformation on a high-performance **DuckDB** data warehouse. | Facilitates **Flexible Transformation** logic (e.g., dynamic risk classification) without modifying raw files. |
| **Social Impact** | Calculates a daily `risk_level` based on **magnitude, depth, and location**. | Provides **Actionable Insights** to Civil Protection and Urban Planners, prioritizing resource allocation in the most vulnerable zones (e.g., **Zone\_Continental**). |

The successful implementation of this pipeline validates the ELT approach as superior for scientific auditing and evolving risk models, concluding with clear **Key Performance Indicators (KPIs)** and **Visualizations** that directly inform policy formulation for increased public safety.

## 1. Project Justification (Fase 1)
### 1.1 Social/Environmental Problem

The analysis addresses the critical issue of **seismic risk assessment** and **early warning response** in regions prone to frequent **earthquakes**. This is a significant **social and environmental problem** worldwide, particularly in areas located along active tectonic plate boundaries. To effectively mitigate potential disaster damage and protect human lives and critical infrastructure, the consistent and timely analysis of key **seismic** parameters—specifically **magnitude, depth, and location**—is absolutely crucial. A robust understanding of these factors enables authorities to develop accurate vulnerability maps and implement efficient **Earthquake Early Warning Systems (EEWS)**.

### 1.2 Beneficiaries
The primary beneficiaries are **Civil Protection Agencies** and **Urban Planners**. This analysis helps prioritize resources, reinforce infrastructure in high-risk zones, and formulate better public safety policies.

### 1.3 Why ELT is an Appropriate Approach
ELT is ideal for Earthquakes data because:

* **Preservation of Raw Data (L):** The original sensor readings (*raw magnitude*, potentially malformed coordinates) must be preserved in the `raw` layer for scientific auditing and future modeling (e.g., re-evaluating magnitude scales).
* **Continuous Growth (E):** The dataset grows constantly, requiring scheduled extraction and loading (simulated every 6 hours in the DAG).
* **Flexible Transformation (T):** Risk classifications (*e.g., defining High\_Risk*) are done post-load, allowing the business logic to evolve without affecting the massive volume of preserved raw data.

***

## 2. Pipeline Architecture and Implementation (Fase 2)

The pipeline is designed using an **ELT architecture**, orchestrated by Apache Airflow (simulated).

### 2.1 ELT Flow Diagram
The process ensures raw data integrity by performing cleaning and aggregation *after* the initial load. 

### 2.2 Airflow DAG Structure (Simulated)
The pipeline is structured with tasks linked sequentially (**E $\to$ L $\to$ T**).

| Task | Phase | Description | Key Requirement Implemented |
| :--- | :--- | :--- | :--- |
| `extract_data_from_api` | **Extract** | Generates or fetches raw data (CSV). | **Scheduling** (`schedule_interval=timedelta(hours=6)`). |
| `load_raw_to_duckdb` | **Load** | Loads data *as-is* into the `raw_data_seismic_events` table. | **Scalability** (Uses DuckDB/SQL warehouse). |
| `transform_and_aggregate` | **Transform** | Executes SQL cleaning, feature creation (`risk_level`), and aggregation. | **Error Handling** (`retries=3`). |

***

## 3. Evidence of ELT Architecture (Data Integrity)

The following tables prove the core principle of ELT: preserving the raw, messy state (`raw_data_seismic_events`) and transforming it later to create clean analytical data (`analytics_seismic_risk`).

### 3.1 Raw Layer Evidence (Preserved)
The `magnitude_raw` column still contains **invalid strings** (`N/A`, `X.XM`), proving no cleaning occurred during the **Load (L)** phase.

| event\_id | time | magnitude\_raw | latitude |
| :--- | :--- | :--- | :--- |
| EVT00000 | 2025-11-01T00:00:00Z | 4.8 | 1.8384 |
| EVT00015 | 2025-11-01T06:00:00Z | N/A | 3.5 |
| EVT00021 | 2025-11-01T08:24:00Z | 6.5M | -12.42 |
| EVT00030 | 2025-11-01T12:00:00Z | N/A | 14.9 |
| EVT00036 | 2025-11-01T14:24:00Z | 5.2M | -3.93 |

### 3.2 Analytics Layer Evidence (Transformed)
The data is clean, aggregated daily, and enriched with the derived feature **`risk_level`**.

| event\_day | location\_zone | risk\_level | avg\_magnitude |
| :--- | :--- | :--- | :--- |
| 2025-11-01 | Zone\_Continental | Low\_Risk | 3.20 |
| 2025-11-01 | Zone\_Pacific\_North | Medium\_Risk | 4.95 |
| 2025-11-02 | Zone\_Continental | High\_Risk | 6.15 |
| 2025-11-02 | Zone\_Pacific\_South | Low\_Risk | 3.82 |
| 2025-11-03 | Zone\_Continental | Medium\_Risk | 5.10 |

***

## 4. Dashboard and Insights (Fase 3)

### 4.1 Key Performance Indicator (KPI)
The KPI tracks high-impact events for immediate prioritization by disaster agencies.

> **KPI: Total High-Risk Events (Magnitude $\ge$ 6.0): 749**

### 4.2 Visualizations

#### Daily Seismic Event Count by Risk Level
*This time-series chart shows the temporal distribution of risk.* **Medium\_Risk** events (Orange line) dominate the frequency, but **High\_Risk** events (Green line) have a consistent baseline, demanding constant monitoring. 

#### Seismic Risk Composition by Location Zone
*This stacked bar chart clearly identifies risk concentration by geography:* 

* The **Zone\_Continental** experiences the overwhelming majority of total seismic events.
* The **Zone\_Continental** also has the **highest concentration of High\_Risk events** (top green portion of the bar).

### 4.3 Insights for Policy Formulation
The analysis proves that the **Zone\_Continental** is the most volatile region, not only in terms of frequency but also in the **absolute number of high-impact events**. This outcome allows **Urban Planners** and **Civil Protection** to prioritize resource allocation, structural reinforcement (engineering), and public awareness campaigns specifically in the Zone\_Continental, validating the initial hypothesis of using ELT for actionable social impact.

