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
