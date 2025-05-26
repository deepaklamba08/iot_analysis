<H1>IOT Analysis</H1>
<p>A configuration driven and customizable python based module for analysis. It is driven by JSON configuration, example of which is present in iot_analysis/config/iot_analysis.json</p>

## Project Structure

- `src/utils.py`: Contains utility functions and classes for credential management, logging, and configuration handling.
- `test/sample_commands`: Sample shell commands for running analysis scripts.
- `test/solar_pump_query.sql`: SQL query for calculating net energy usage of solar pumps.
- `test/sch.json`: JSON configuration for a simple task scheduler.

## Prerequisites

- Python 3.8 or higher
- Pip for managing Python dependencies
- PyCharm IDE (optional but recommended)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/deepaklamba08/your-repo-name.git
   cd your-repo-name
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables as needed for the `EnvironmentVariableCredentialProvider`.

## Usage

### Running the Analysis Script

Use the sample commands in `test/sample_commands` to execute the analysis script:

# IoT Analysis Project

This project is designed to analyze IoT data, manage credentials, and execute various tasks using Python and SQL. It includes utilities for credential management, logging, configuration handling, and data processing.

## Features

- **Credential Management**: Supports simple and environment variable-based credential providers.
- **Logging**: Configurable logging for debugging and monitoring.
- **Configuration Handling**: Reads and parses YAML configuration files.
- **SQL Queries**: Includes SQL scripts for IoT data analysis.
- **Task Scheduling**: JSON-based task scheduler configuration.

## Sources

The project processes data from the following sources:
- **IoT Database**: The `iotdatabase.meterdata` table is queried to retrieve energy data for specific meters and dates.
- **Configuration Files**: YAML and JSON files are used for application configuration and task scheduling.
- **Environment Variables**: Used for dynamic credential management and runtime configurations.

## Transformations

The project performs the following transformations:
- **SQL Query Transformations**: 
  - Calculates net energy usage by comparing energy readings from consecutive days.
  - Filters and orders data based on specific conditions (e.g., meter ID, calculation date).
- **Placeholder Replacement**: Dynamically replaces placeholders in SQL queries and configuration files with runtime parameters.
- **Data Parsing**: Parses YAML and JSON files to extract structured configuration data.

## Actions

The project supports the following actions:
- **Data Analysis**: Executes SQL queries to analyze IoT data, such as calculating net energy usage for solar pumps.
- **Task Scheduling**: Manages tasks using a JSON-based scheduler, allowing for flexible task execution based on time and conditions.