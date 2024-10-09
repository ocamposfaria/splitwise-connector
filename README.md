# My Splitwise Connector


<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8%2B-blue?logo=python" alt="Python">
  <img src="https://img.shields.io/badge/FastAPI-0.112.0+-brightgreen?logo=fastapi" alt="FastAPI">
  <img src="https://img.shields.io/badge/Polars-1.5.0+-blue?logo=polars" alt="Polars">
  <img src="https://img.shields.io/badge/AWS%20S3-Storage-orange?logo=amazons3" alt="AWS S3">
  <img src="https://img.shields.io/badge/DuckDB-1.0.0-yellow?logo=duckdb" alt="DuckDB">
  <img src="https://img.shields.io/badge/dbt-1.8.5+-orange?logo=dbt" alt="dbt">

</p>


## Overview

This project is a Python application that uses FastAPI to act as a proxy, facilitating the consumption of information from [Splitwise](https://splitwise.com). The obtained data is stored in its raw format as Delta Lake tables in an S3 bucket on AWS. Furthermore, the project has connectors to create and store tables in a DuckDB database. A dbt (Data Build Tool) project is also integrated with DuckDB to perform transformations and analysis on the stored data.

## Project Architecture

![Project Architecture](images/image-1.png)


## Features

- **Splitwise Data Collection:** Use the Splitwise API to obtain planned information on expenses, balances, and groups.
- **Storage in Delta Lake:** Ensure persistence of raw data with ACID transactions and versioning.
- **Integration with DuckDB:** Creates tables in DuckDB from data stored in S3.
- **Transformations with dbt:** Apply transformation models to raw data to generate insights and analysis.
- **Scalability and Resilience:** The use of AWS S3 and Delta Lake ensures that the system is highly scalable and resilient.

## Prerequisites

- **Python 3.8 or higher** and libraries in `requirements.txt`, or a Docker container that runs this environment.
- **AWS account** and user with read and write permissions on S3 - IAM recommended.
- **Splitwise account** with developer permissions.

## Installation

1. Clone the repository:
    ```
    git clone https://github.com/ocamposfaria/splitwise-connector.git
    ```

2. Install project dependencies:
    ```
    pip install -r requirements.txt
    ```

3. Configure allowed environment variables, such as Splitwise API keys and AWS credentials, using an .env file.
    ```
    # splitwise
    API_KEY="<your value here>"

    # aws
    AWS_SECRET_ACCESS_KEY="<your value here>"
    AWS_ACCESS_KEY_ID="<your value here>"

    # google
    TYPE="<your value here>"
    PROJECT_ID="<your value here>"
    PRIVATE_KEY_ID="<your value here>"
    PRIVATE_KEY="<your value here>"
    CLIENT_EMAIL="<your value here>"
    CLIENT_ID="<your value here>"
    AUTH_URI="<your value here>"
    TOKEN_URI="<your value here>"
    AUTH_PROVIDER_X509_CERT_URL="<your value here>"
    CLIENT_X509_CERT_URL="<your value here>"
    UNIVERSE_DOMAIN="<your value here>"

    # deltalake
    AWS_S3_ALLOW_UNSAFE_RENAME=true
    ```
4. Run a FastAPI application:
    ```
    uvicorn main:app --reload
    ```

5. Access the URL to execute requests: http://127.0.0.1:8000/docs#/

## Contributions
Feel free to contribute to the project. To do this, fork the repository and send a pull request with your improvements.