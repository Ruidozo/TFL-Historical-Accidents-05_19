# Transport Accident Analysis & Correlation with Weather in London

## Overview

The **TFL-Accidents\_05-19** project is a data engineering pipeline designed to analyze historical road traffic accidents in London. The dataset includes all recorded accidents in the London area between **2005 and 2019**. It integrates data from multiple sources, including the Transport for London (TfL) AccidentStats API and weather data, to study correlations between weather conditions and accident occurrences.

This project follows an end-to-end pipeline approach, leveraging cloud storage, batch processing, a data warehouse, and analytical transformations to generate insights. The final output is visualized using an interactive dashboard.

## Project Objectives & Key Insights

### **Objective**

The goal of this project is to analyze and uncover patterns in road traffic accidents by correlating accident data with various external factors such as location, weather, transportation type, and date-based trends. Since the dataset does not explicitly include time of day, the analysis is conducted based on **dates only**, rather than specific hours or timeframes within a day. This will help in identifying risk factors, trends, and potential safety improvements.

---

### **Key Insights to Extract from the Dataset**

#### **1. Location-Based Analysis**

- Identify accident hotspots based on **geographical location**.
- Determine whether **specific road types** (highways, intersections, residential streets) have a higher frequency of accidents.

#### **2. Weather Conditions & Accident Severity**

- Analyze the impact of **weather conditions** (rain, fog, snow, clear, etc.) on accident occurrence.
- Correlate **weather with severity** (minor injuries, severe injuries, fatalities).
- Identify which weather conditions lead to higher accident rates for **specific vehicle types**.

#### **3. Type of Transportation & Accident Risk**

- Compare accident frequency between different **modes of transport** (cars, motorcycles, bicycles, public transport, pedestrians).
- Analyze which **transport type** is most vulnerable to severe injuries.
- Identify whether certain **types of vehicles** (e.g., motorcycles, trucks) are overrepresented in accident data.

#### **4. Seasonal & Temporal Trends**

- Identify how accidents fluctuate across **seasons** and **months** (e.g., do accidents increase in winter due to road conditions?).
- Determine which **days of the week** have the highest accident occurrences.
- Compare accident trends on **weekdays vs. weekends**.

#### **5. Injuries & Fatalities by Transport Type**

- Determine which **transportation types** are associated with **higher injury rates**.
- Compare the **average number of injuries** per accident across different modes of transport.
- Assess the likelihood of **fatalities vs. non-fatal injuries** for specific vehicle categories.

#### **6. Risk Factors for Different Demographics**

- Analyze how **age** correlate with accident risks.
- Identify if specific **demographics** (e.g., younger drivers, elderly pedestrians) are more prone to accidents or severe injuries.

#### **7. Impact of Road & Traffic Conditions**

- Identify the influence of **road surface conditions** (wet, dry, icy) on accident severity.
- Assess the role of **traffic density** in accident frequency.



---

### **Potential Applications**

- **Traffic Safety Policies:** Provide insights for improving **road safety regulations**.
- **Urban Planning:** Help **city planners** identify areas needing infrastructure improvements (better lighting, pedestrian crossings, speed limits).
- **Public Awareness Campaigns:** Guide the design of **targeted safety campaigns** based on the most affected groups and risk factors.
- **Emergency Response Optimization:** Improve resource allocation for **emergency services** based on high-risk locations and seasonal trends.

---

## Project Architecture

### Components

- **Data Ingestion**: Extracts accident and weather data from external APIs.
- **Data Processing**: Cleans, transforms, and standardizes data.
- **Data Storage**: Utilizes a cloud-based data warehouse (e.g., PostgreSQL) for structured analysis.
- **ETL Pipeline**: Automates the ingestion, transformation, and loading process.
- **Dashboard**: Provides an interactive visualization of accident trends and correlations.

### Tools & Technologies

- **Python**: Primary language for data extraction and transformation.
- **Docker**: Containerization of all components for easy deployment.
- **Airflow**: Orchestration of data pipelines.
- **Google Cloud Storage (GCS)**: Data lake for storing raw datasets.
- **PostgreSQL**: Data warehouse for structured queries.
- **dbt (Data Build Tool)**: Data transformation and modeling.
- **Streamlit**: Used for building the interactive dashboard.

---

## Setup & Deployment

There are two ways to set up and deploy the project:

### Option 1: Using the Setup Script

1. Execute the `setup.sh` script:
   ```bash
   ./setup.sh
   ```
2. Follow the steps prompted by the terminal to configure the environment.

### Option 2: Manual Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/Ruidozo/TFL-Accidents_05-19.git
   cd TFL-Accidents_05-19
   ```

2. Copy the `.env.template` file and configure environment variables:

   ```bash
   cp .env.template .env
   ```

   Edit `.env` with the required values.

3. Build and start the services using Docker Compose:

   ```bash
   docker-compose up --build
   ```

4. Access the services:

   - **Airflow**: Visit `http://localhost:8082`
   - **Dashboard**: Visit `http://localhost:8501`

---

## Contributors & Contact

- **Author**: Rui Carvalho
- **Contributions**: Open-source contributions are welcome!
- **GitHub Issues**: [https://github.com/Ruidozo/TFL-Accidents\_05-19](https://github.com/Ruidozo/TFL-Accidents_05-19)
- **Email**: [ruimcarv@gmail.com](mailto\:ruimcarv@gmail.com)

---

# tfl-de-zoomcamp-project
