import hashlib
import os
import time
from datetime import datetime

import pandas as pd
import yaml
from databricks import sql
from sqlalchemy import create_engine, text
import logging


# Configure Logging
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)


# Load configuration from YAML file
def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


# Function to run queries and collect performance metrics
def run_queries(engine, platform_name, queries, date_filters, user_email, filename, warehouse_mapping):
    if platform_name == 'Snowflake':
        with engine.connect() as connection:
            for category_name, queries_list in queries.items():
                logging.info(f"\nCategory: '{category_name}'")
                for query_info in queries_list:
                    query_name = query_info.get('name', 'Unnamed Query')
                    if query_info.get('is_skipped', False):
                        logging.info(f"\n[Skipping] Query '{query_name}' on {platform_name}")
                        continue

                    source_type = query_info.get('source_type', 'combined')
                    use_cached_result = query_info.get('use_cached_result', False)
                    warehouse_type = query_info.get('warehouse_type', 'S')  # Default to 'S'

                    logging.info(
                        f"\nRunning Query '{query_name}' on {platform_name} using warehouse type '{warehouse_type}'...")

                    # Select the appropriate query based on the platform
                    query_template = query_info.get(f"{platform_name.lower()}_query")
                    if not query_template:
                        logging.warning(f"No query provided for '{query_name}' on {platform_name}. Skipping.")
                        continue

                    # Replace date placeholders
                    formatted_query = query_template
                    logging.info(f"\nFormatted query being executed on {platform_name}:\n{formatted_query}")

                    # Set the warehouse and use_cached_result parameter per query
                    try:
                        # Switch to the appropriate warehouse
                        warehouse_name = warehouse_mapping.get(warehouse_type)
                        if not warehouse_name:
                            logging.warning(
                                f"Warehouse type '{warehouse_type}' not found in configuration. Skipping query.")
                            continue
                        connection.execute(text(f"USE WAREHOUSE {warehouse_name};"))
                        logging.info(f"Snowflake: Using warehouse '{warehouse_name}' for this query.")

                        cached_result_value = 'TRUE' if use_cached_result else 'FALSE'
                        connection.execute(text(f"ALTER SESSION SET USE_CACHED_RESULT={cached_result_value};"))
                        logging.info(f"Snowflake: USE_CACHED_RESULT set to {cached_result_value} for this query.")
                    except Exception as e:
                        logging.error(f"Failed to set warehouse or use_cached_result parameter on {platform_name}: {e}")
                        continue  # Skip to the next query if we can't set up the environment

                    # Execute the query and measure execution time
                    try:
                        start_time = time.time()
                        result = connection.execute(text(formatted_query))
                        execution_time = time.time() - start_time

                        # Get the query ID
                        query_id = result.cursor.sfqid

                        logging.info(f"[{platform_name}] Query executed in {execution_time:.4f} seconds")
                        logging.info(f"[{platform_name}] Query ID: {query_id}")

                        # Prepare the result entry
                        result_entry = {
                            'guid': query_info.get('guid'),  # Include GUID
                            'category': category_name,
                            'user_email': user_email,
                            'platform': platform_name,
                            'warehouse_type': warehouse_type,
                            'use_cached_result': use_cached_result,
                            'source_type': source_type,
                            'query_id': query_id,
                            'query': formatted_query,
                            'query_name': query_name,
                            'script_execution_time_sec': execution_time
                        }

                        # Append the result to the CSV file
                        df = pd.DataFrame([result_entry])
                        write_mode = 'a' if os.path.exists(filename) else 'w'
                        header = not os.path.exists(filename)
                        df.to_csv(filename, mode=write_mode, header=header, index=False)
                        logging.info(f"[{platform_name}] Results written to '{filename}'")

                    except Exception as e:
                        error_message = str(e)
                        logging.error(f"[{platform_name}] Failed to execute query '{query_name}': {error_message}")

                        # Prepare the error result entry
                        result_entry = {
                            'guid': query_info.get('guid'),  # Include GUID even on error
                            'category': category_name,
                            'user_email': user_email,
                            'platform': platform_name,
                            'warehouse_type': warehouse_type,
                            'use_cached_result': use_cached_result,
                            'source_type': source_type,
                            'query_id': None,
                            'query': formatted_query,
                            'query_name': query_name,
                            'script_execution_time_sec': None,
                            'error_message': error_message
                        }

                        # Append the error result to the CSV file
                        df = pd.DataFrame([result_entry])
                        write_mode = 'a' if os.path.exists(filename) else 'w'
                        header = not os.path.exists(filename)
                        df.to_csv(filename, mode=write_mode, header=header, index=False)
                        logging.error(f"[{platform_name}] Error results written to '{filename}'")

    elif platform_name == 'Databricks':
        for category_name, queries_list in queries.items():
            logging.info(f"\nCategory: '{category_name}'")
            for query_info in queries_list:
                query_name = query_info.get('name', 'Unnamed Query')
                if query_info.get('is_skipped', False):
                    logging.info(f"\n[Skipping] Query '{query_name}' on {platform_name}")
                    continue

                source_type = query_info.get('source_type', 'combined')
                use_cached_result = query_info.get('use_cached_result', False)
                warehouse_type = query_info.get('warehouse_type', 'S')  # Default to 'S'

                logging.info(
                    f"\nRunning Query '{query_name}' on {platform_name} using warehouse type '{warehouse_type}'...")

                # Select the appropriate query based on the platform
                query_template = query_info.get(f"{platform_name.lower()}_query")
                if not query_template:
                    logging.warning(f"No query provided for '{query_name}' on {platform_name}. Skipping.")
                    continue

                # Replace date placeholders
                formatted_query = query_template.format(**date_filters)
                logging.info(f"\nFormatted query being executed on {platform_name}:\n{formatted_query}")

                # Get the connection for the specified warehouse type
                connection = warehouse_mapping.get(warehouse_type)
                if not connection:
                    logging.warning(f"Warehouse type '{warehouse_type}' not found in configuration. Skipping query.")
                    continue

                try:
                    cursor = connection.cursor()
                    # Set the use_cached_result parameter per query
                    try:
                        cached_result_value = 'true' if use_cached_result else 'false'
                        cursor.execute(f"SET use_cached_result = {cached_result_value}")
                        logging.info(f"Databricks: use_cached_result set to {cached_result_value} for this query.")
                    except Exception as e:
                        logging.error(f"Failed to set use_cached_result parameter on {platform_name}: {e}")
                        continue  # Skip to the next query if we can't set up the environment

                    # Execute the query and measure execution time
                    start_time = time.time()
                    cursor.execute(formatted_query)
                    execution_time = time.time() - start_time

                    # Get the query ID
                    query_id = cursor.query_id

                    logging.info(f"[{platform_name}] Query executed in {execution_time:.4f} seconds")
                    logging.info(f"[{platform_name}] Query ID: {query_id}")

                    # Prepare the result entry
                    result_entry = {
                        'guid': query_info.get('guid'),  # Include GUID
                        'category': category_name,
                        'user_email': user_email,
                        'platform': platform_name,
                        'warehouse_type': warehouse_type,
                        'use_cached_result': use_cached_result,
                        'source_type': source_type,
                        'query_id': query_id,
                        'query': formatted_query,
                        'query_name': query_name,
                        'script_execution_time_sec': execution_time
                    }

                    # Append the result to the CSV file
                    df = pd.DataFrame([result_entry])
                    write_mode = 'a' if os.path.exists(filename) else 'w'
                    header = not os.path.exists(filename)
                    df.to_csv(filename, mode=write_mode, header=header, index=False)
                    logging.info(f"[{platform_name}] Results written to '{filename}'")

                except Exception as e:
                    error_message = str(e)
                    logging.error(f"[{platform_name}] Failed to execute query '{query_name}': {error_message}")

                    # Prepare the error result entry
                    result_entry = {
                        'guid': query_info.get('guid'),  # Include GUID even on error
                        'category': category_name,
                        'user_email': user_email,
                        'platform': platform_name,
                        'warehouse_type': warehouse_type,
                        'use_cached_result': use_cached_result,
                        'source_type': source_type,
                        'query_id': None,
                        'query': formatted_query,
                        'query_name': query_name,
                        'script_execution_time_sec': None,
                        'error_message': error_message
                    }

                    # Append the error result to the CSV file
                    df = pd.DataFrame([result_entry])
                    write_mode = 'a' if os.path.exists(filename) else 'w'
                    header = not os.path.exists(filename)
                    df.to_csv(filename, mode=write_mode, header=header, index=False)
                    logging.error(f"[{platform_name}] Error results written to '{filename}'")

                finally:
                    if 'cursor' in locals():
                        cursor.close()

        # Close all connections
        for conn in warehouse_mapping.values():
            conn.close()


# Retrieve the value from environment variables if specified
def get_env_variable(value):
    if value.startswith('${') and value.endswith('}'):
        env_var = value[2:-1]
        value = os.environ.get(env_var)
        if not value:
            raise ValueError(f"Environment variable '{env_var}' is not set.")
    return value


# Snowflake connection string
def get_snowflake_engine(cfg, warehouse_history=False):
    import urllib.parse
    user = cfg['user']
    password = cfg['password']
    account = cfg['account']
    if warehouse_history is not True:
        database = cfg.get('database', '')
        schema = cfg.get('schema', '')
        warehouse = cfg.get('warehouse', '')
    else:
        database = cfg.get('database', 'DW_UNIFORM')
        schema = cfg.get('schema', 'PUBLIC')
        warehouse = cfg.get('warehouse', 'UNIFIED_POC_XS')

    if password.startswith('${') and password.endswith('}'):
        env_var = password[2:-1]
        password = os.environ.get(env_var)
        if not password:
            raise ValueError(f"Environment variable '{env_var}' for Snowflake password is not set.")

    password = urllib.parse.quote_plus(password)

    snowflake_url = (
        f"snowflake://{user}:{password}@{account}/"
        f"{database}/{schema}?warehouse={warehouse}"
    )
    engine = create_engine(
        snowflake_url,
        connect_args={'session_parameters': {'AUTOCOMMIT': True}}
    )
    return engine


# Databricks connection string
def get_databricks_connections(cfg):
    server_hostname = cfg.get('server_hostname', '').strip()
    access_token = get_env_variable(cfg.get('access_token', '').strip())
    http_paths = cfg.get('http_paths', {})

    connections = {}
    for size, http_path in http_paths.items():
        conn = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        connections[size] = conn

    return connections


# Fetches total_elapsed_time from Snowflake's query history for given query_ids.
def fetch_snowflake_query_history(engine, query_ids, history_warehouse):
    if not query_ids:
        logging.info("No query_ids provided for fetching query history.")
        return {}

    # Prepare the query with the list of query_ids
    placeholders = ', '.join([f"'{qid}'" for qid in query_ids])
    history_query = f"""
    select 
        query_id, 
        execution_status,
        execution_time as execution_time_ms,
        compilation_time as compilation_time_ms, 
        total_elapsed_time as db_total_duration_time_ms, 
        credits_used_cloud_services as credits_dbus 
    from table(information_schema.query_history()) 
    where query_id IN ({placeholders})
    """

    try:
        with engine.connect() as connection:
            # Set the warehouse
            connection.execute(text(f"USE WAREHOUSE {history_warehouse};"))
            logging.info(f"Snowflake: Using warehouse '{history_warehouse}' for fetching query history.")

            # Execute the history query
            result = connection.execute(text(history_query))
            history_data = result.fetchall()

            # Construct a dictionary mapping query_id to metrics
            history_dict = {}
            for row in history_data:
                history_dict[row[0]] = {
                    'execution_status': row[1],
                    'execution_time_ms': row[2],
                    'compilation_time_ms': row[3],
                    'db_total_duration_time_ms': row[4],
                    'credits_dbus': row[5]
                }
            return history_dict
    except Exception as e:
        logging.error(f"Error fetching query history from Snowflake: {e}")
        return {}


# Appends total_elapsed_time from Snowflake's query history to the existing CSV.
def append_snowflake_history_to_csv(engine, filename, warehouse_name):
    # Read the existing CSV into a DataFrame
    try:
        df = pd.read_csv(filename)
    except FileNotFoundError:
        logging.error(f"CSV file '{filename}' not found. Skipping history append.")
        return
    except Exception as e:
        logging.error(f"Error reading CSV file '{filename}': {e}")
        return

    # Filter Snowflake queries with non-null query_id
    snowflake_queries = df[(df['platform'] == 'Snowflake') & (df['query_id'].notnull())]
    query_ids = snowflake_queries['query_id'].unique().tolist()

    if not query_ids:
        logging.warning("No Snowflake query_ids found in the CSV. Skipping history fetch.")
        return

    logging.info(f"Fetching query history for {len(query_ids)} Snowflake queries...")

    # Fetch query history
    history_dict = fetch_snowflake_query_history(engine, query_ids, warehouse_name)

    if not history_dict:
        logging.warning("No query history fetched. Skipping append.")
        return

    # Create a DataFrame from history_dict
    history_df = pd.DataFrame.from_dict(history_dict, orient='index').reset_index()
    history_df = history_df.rename(columns={'index': 'query_id'})

    # Merge with the original DataFrame
    df = df.merge(history_df, on='query_id', how='left')

    # Save the updated DataFrame back to CSV
    try:
        df.to_csv(filename, index=False)
        logging.info(f"Updated CSV with Snowflake query history saved to '{filename}'")
    except Exception as e:
        logging.error(f"Error writing updated CSV file '{filename}': {e}")


# Generates a deterministic GUID based on query name and platform.
def generate_deterministic_guid(query_name, platform_name):
    unique_string = f"{platform_name}_{query_name}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()


# Assigns GUIDs to each query if not already present.
def assign_guids(queries, platform_name):
    for category, queries_list in queries.items():
        for query in queries_list:
            if 'guid' not in query or not query['guid']:
                query['guid'] = generate_deterministic_guid(query['name'], platform_name)
                logging.info(f"Assigned GUID {query['guid']} to query '{query['name']}' on {platform_name}.")
            else:
                logging.info(f"Using existing GUID {query['guid']} for query '{query['name']}' on {platform_name}.")


# Main function to run benchmark
def benchmark():
    # Load configurations
    setup_logging()
    config = load_config('config/connections.yaml')
    queries_config = load_config('config/queries.yaml')

    date_filters = config.get('date_filters', {})
    queries = queries_config.get('queries', {})
    user_email = config.get('user_email', 'unknown@example.com')

    # Assign GUIDs to all queries for each platform
    platforms = ['Snowflake', 'Databricks']
    for platform in platforms:
        platform_queries = queries  # Assuming same structure for both platforms
        assign_guids(platform_queries, platform)

    # Generate a unique filename with current timestamp including seconds
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"query_benchmark_results_{timestamp}.csv"

    platforms = ['Snowflake', 'Databricks']

    for platform_name in platforms:
        connection_config = config.get(platform_name.lower(), {})
        if not connection_config:
            logging.warning(f"No configuration found for platform '{platform_name}'. Skipping.")
            continue

        logging.info(f"\nConnecting to {platform_name}...")
        try:
            if platform_name == 'Snowflake':
                # Get the base engine
                engine = get_snowflake_engine(connection_config)
                # Get the warehouse mapping
                warehouse_mapping = connection_config.get('warehouses', {})
            elif platform_name == 'Databricks':
                # For Databricks, get connections per warehouse type
                warehouse_mapping = get_databricks_connections(connection_config)
                # For Databricks, the warehouse_mapping is a dict of connections
                engine = None  # Not used for Databricks
            else:
                logging.warning(f"Unsupported platform '{platform_name}'. Skipping.")
                continue
        except Exception as e:
            logging.error(f"Failed to connect to {platform_name}: {e}")
            continue

        # Run queries
        run_queries(engine, platform_name, queries, date_filters, user_email, filename, warehouse_mapping)

        if platform_name == 'Snowflake':
            logging.info(f"\nFetching and appending query history for Snowflake...")
            # Ensure you have a separate connection for history if needed
            append_snowflake_history_to_csv(engine, filename, warehouse_name="UNIFIED_POC_XS")
            # Dispose the engine after use
            engine.dispose()

    logging.info(f"\nBenchmark completed. Results saved to '{filename}'")


if __name__ == "__main__":
    benchmark()
