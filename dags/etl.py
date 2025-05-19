from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from datetime import timedelta

# Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=timezone.utcnow() - timedelta(days=1),  # Start the DAG from 1 day ago
    schedule='@daily',  # Schedule to run daily
    catchup=False,  # Do not run missed intervals on startup
) as dag:

    # Step 1: Create the table if it does not exist
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

    # Step 2: Extract data from NASA API (APOD)
    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',  # Airflow connection ID for NASA API
        endpoint='planetary/apod',  # NASA API endpoint for APOD
        method='GET',
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},  # Get API key from connection extras
        response_filter=lambda response: response.json(),  # Parse response as JSON
        do_xcom_push=True,
    )

    # Step 3: Transform the extracted APOD data to only the fields we need
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', ''),
        }
        return apod_data

    # Step 4: Load the transformed data into Postgres
    @task
    def load_data_to_postgres(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        insert_query = """
        INSERT INTO apod_data(title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type'],
        ))

    # Define task dependencies
    create = create_table()
    extract = extract_apod
    transformed = transform_apod_data(extract.output)
    load = load_data_to_postgres(transformed)

    create >> extract >> transformed >> load
