# DBX-SF Benchmark

This repository is designed to benchmark query performance between **Databricks** and **Snowflake** for different data sources such as Iceberg and Delta Lake tables. 
The project supports multiple query categories, customizable execution, and automated logging of results.

## Project Structure

```yaml
├── config/
│   ├── connections.yaml        # YAML file with connection details for Snowflake and Databricks
│   └── queries.yaml            # YAML file with benchmark queries and configurations
├── benchmark.py                # Python script to run the benchmark queries and collect performance metrics
├── requirements.txt            # List of Python dependencies for the project
└── README.md                   # Project documentation (this file)
```

## Prerequisites

1. **Python 3.8+**: Ensure Python is installed on your machine.
2. **Virtual Environment**: It's recommended to use a virtual environment to manage dependencies.

### Install Python Dependencies

Once you have cloned the repository, run the following commands to set up the environment and install the required dependencies:

```bash
# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration Files

### 1. `connections.yaml`

This file contains the connection details for both **Snowflake** and **Databricks**. 
Ensure sensitive information such as passwords and access tokens are handled securely, either by using environment variables or secure vaults.

Here’s an example structure:

```yaml
snowflake:
  user: 'your_snowflake_user'
  password: '${SNOWFLAKE_PASSWORD}'  # Retrieved from environment variable
  account: 'your_snowflake_account'
  warehouse: 'XS'
  warehouses:
    XS: 'your_warehouse_name'
    S: 'your_warehouse_name'
    M: 'your_warehouse_name'
  # other configurations...

databricks:
  server_hostname: 'your_databricks_hostname'
  access_token: '${DATABRICKS_ACCESS_TOKEN}'  # Retrieved from environment variable
  http_paths:
    XS: '/sql/1.0/warehouses/your_http_path'
    S: '/sql/1.0/warehouses/your_http_path'
    M: '/sql/1.0/warehouses/your_http_path'
  # other configurations...
```

### 2. `queries.yaml`

This file contains the benchmark queries for both platforms. It includes multiple categories, and each query is mapped with configurations such as whether to use cached results, warehouse size, and the SQL queries for each platform.

Example structure:

```yaml
queries:
  data_profiling:
    - name: 'Iceberg - Total Count Query'
      source_type: 'iceberg'
      is_skipped: false
      use_cached_result: true
      warehouse_type: 'XS'
      databricks_query: "SELECT COUNT(*) AS total_count FROM poc_unified_data_lake.main.ice_rd_webhook_order;"
      snowflake_query: "SELECT COUNT(*) AS total_count FROM DW_UNIFORM.ICEBERG.RD_WEBHOOK_ORDER;"
  # More categories and queries...
```

## Running the Benchmark

To execute the benchmark:

1. Activate your virtual environment:

    ```bash
    source venv/bin/activate  # For Linux/macOS
    ```

2. Run the `benchmark.py` script:

    ```bash
    python benchmark.py
    ```

This will:

- Load the queries from `queries.yaml`.
- Execute the queries on **Databricks** and **Snowflake** as configured.
- Log results including execution time and query IDs in a CSV file.
- Optionally fetch historical data for executed Snowflake queries.

## Logging

The script uses the `logging` module to log key information during execution. The logs are printed to the console and show detailed information about each query, including:

- Query execution time in milliseconds.
- Any errors encountered during execution.
- Query IDs and associated warehouse sizes.

## CSV Payload

Each run of the benchmark generates a CSV file that contains detailed results for each query executed on Snowflake and Databricks. The CSV file is named with a timestamp in the format `query_benchmark_results_YYYYMMDD_HHMMSS.csv`.

### CSV Columns

| Column Name                | Description                                                                 |
|----------------------------|-----------------------------------------------------------------------------|
| `guid`                     | A deterministic GUID assigned to each query based on query name and platform.|
| `category`                 | The category of the query (e.g., `data_profiling`, `performance_benchmarking`).|
| `user_email`               | Email of the user running the benchmark (from the configuration file).       |
| `platform`                 | The platform on which the query was executed (`Snowflake`, `Databricks`).    |
| `warehouse_type`           | The size/type of the warehouse used for the query (e.g., `XS`, `S`, `M`, `L`).|
| `use_cached_result`        | Indicates whether cached results were used (`true` or `false`).              |
| `source_type`              | The source type of the query (`iceberg`, `delta`, or `combined`).            |
| `query_id`                 | The unique ID of the query generated by the platform.                        |
| `query`                    | The SQL query that was executed.                                             |
| `query_name`               | The name of the query as defined in `queries.yaml`.                          |
| `script_execution_time_sec`| The execution time of the query as measured by the script, in seconds.       |
| `execution_status`         | The status of the query (fetched from Snowflake's query history).            |
| `db_total_duration_time_ms`| Total elapsed time in milliseconds (fetched from Snowflake query history).   |
| `compilation_time_ms`      | Compilation time in milliseconds (fetched from Snowflake query history).     |
| `execution_time_ms`        | Execution time in milliseconds (fetched from Snowflake query history).       |
| `error_message`            | Any error encountered during query execution (if applicable).                |

### Example CSV Output

Below is an example of what the CSV file might look like after running a benchmark:

```csv
guid,category,user_email,platform,warehouse_type,use_cached_result,source_type,query_id,query,query_name,script_execution_time_sec,execution_status,db_total_duration_time_ms,compilation_time_ms,execution_time_ms,error_message
6f9fa7c93a1a4ed5af0f3f274d5bdf53,data_profiling,user@example.com,Snowflake,XS,false,iceberg,01b7617f-030a-f56e-0007-8c43000d0412,SELECT COUNT(*) AS total_count FROM DB.SCHEMA.TABLE;,Iceberg - Total Count Query,0.456,SUCCEEDED,456,56,400,,
cf9de66fbdc64bfb9446894c576cdd56,data_profiling,user@example.com,Databricks,XS,false,iceberg,01a22be-564a-ab21-f3a45,SELECT COUNT(*) AS total_count FROM db.schema.table;,Iceberg - Total Count Query,0.987,,,
```

## Query History

For Snowflake queries, after execution, the script fetches additional performance data (such as elapsed time, execution status, and credits used) from the Snowflake **query history** and appends this information to the CSV file.

Example query used to retrieve Snowflake query history:

```sql
SELECT 
    query_id, 
    execution_status, 
    total_elapsed_time, 
    credits_used_cloud_services 
FROM table(information_schema.query_history()) 
WHERE query_id IN ('<query_id_1>', '<query_id_2>');
```

## Customization

- **Modify Queries:** You can modify or add new queries in the `queries.yaml` file, categorized by their function (e.g., `data_profiling`, `performance_benchmarking`).
- **Warehouse Sizes:** Adjust the warehouse types in `connections.yaml` to allocate different resources depending on query requirements.
- **Logging Levels:** You can adjust the logging level in `benchmark.py` to control the verbosity of logs (INFO, DEBUG, etc.).

## License

This project is licensed under the MIT License.

## Contributions

Feel free to submit issues or pull requests to improve the functionality of the benchmark.
