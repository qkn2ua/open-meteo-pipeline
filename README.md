# open-meteo-pipeline

## Overview
This project implements a containerized data pipeline that collects real-time weather data using the Open-Meteo API. The pipeline runs on a Kubernetes CronJob and continuously gathers, processes, and stores weather data over time. The system is designed to demonstrate a scalable, automated data pipeline that persists data and generates evolving visualizations.

---

## Data Source
The data for this pipeline comes from the Open-Meteo API:
https://open-meteo.com/en/docs

This API provides free and open access to weather data without requiring authentication. In this project, the pipeline retrieves hourly weather data such as temperature and timestamp for a specific geographic location. The API is well-suited for this application because it is reliable, easy to use, and provides structured JSON responses.

---

## Scheduled Process
The pipeline is deployed as a Kubernetes CronJob that runs once per hour. Each time the job runs, it performs the following steps:

1. Sends a request to the Open-Meteo API to fetch the latest weather data.
2. Extracts relevant fields such as timestamp and temperature.
3. Appends the new data point to an existing dataset stored in S3.
4. Rebuilds the full dataset into a structured format using pandas.
5. Generates an updated plot that visualizes temperature over time.
6. Uploads both the updated dataset and plot back to an S3 bucket.

This process ensures that data is continuously collected and updated over time, allowing for long-term tracking and visualization.

---
