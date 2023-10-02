# Stock Price Prediction and Real-time Monitoring with Apache Spark, Kafka, PostgreSQL, and Machine Learning

# Overview
This project aims to predict daily Apple stock prices using various machine learning algorithms, including Linear Regression, Decision Tree Regression, and Gradient Boosting Regression, all implemented in PySpark. The predicted stock prices are saved for future use. Additionally, the project fetches daily historical stock price data from Yahoo Finance, writes it to a Kafka topic, integrates Kafka with Spark, and uses the pre-trained machine learning models to predict real-time stock prices. The results are stored in a PostgreSQL database and visualized using Grafana as a time series dashboard.
# Technologies Used
Apache Spark: A fast and distributed data processing framework for machine learning tasks.

Apache Kafka: A distributed streaming platform for real-time data feeds.

PostgreSQL: An open-source relational database for storing prediction results.

Grafana: A powerful visualization tool for creating interactive dashboards.

Machine Learning Algorithms: Linear Regression, Decision Tree Regression, and Gradient Boosting Regression are used for stock price prediction.

# Machine Learning Models
We employ three machine learning algorithms for stock price prediction:
1.Linear Regression: A linear approach to modeling the relationship between a dependent variable (stock open price) and one or more independent variables (historical stock data).

2.Decision Tree Regression: A tree-like model that splits the dataset into segments to predict stock prices based on historical data.

3.Gradient Boosting Regression: An ensemble learning technique that combines multiple decision trees to make more accurate stock price predictions.

# Data Collection
We collect daily historical Apple stock price data from Yahoo Finance. This data is used both for training our machine learning models and as a real-time data source for predicting future stock prices.

# Real-time Data Streaming
We use Apache Kafka to facilitate real-time data streaming. Historical data is continuously fed into a Kafka topic. Apache Spark subscribes to this topic, utilizes the pre-trained machine learning models, and predicts real-time stock prices. These predictions are then stored in PostgreSQL for further analysis

# Filtering Data for Real-time Visualization in Grafana
In Grafana, we use SQL queries to filter and retrieve real-time data from the Kafka topic, ensuring that we display the latest available information. The following SQL query accomplishes this task:
![Local Image](relative/path/to/image.png)
  ```sql
  SELECT
    a."date_A",
    a."open_A",
    a."prediction"
  FROM
    apple1 a
  JOIN
    (SELECT
      "date_A",
      MAX("kafka_time") AS max_kafka_time
     FROM
      apple1
     GROUP BY
      "date_A"
    ) b
  ON
    a."date_A" = b."date_A" AND a."kafka_time" = b.max_kafka_time
  LIMIT
    50;



