import boto3
import pandas as pd
import re
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import io
import os


def save_parquet_to_s3(df, bucket, key):
    # Convert the DataFrame into a PyArrow table.
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Convert the timestamp column from string to a proper timestamp type.
    # PyArrow is used here because pandas datetime conversion caused issues on Lambda.
    idx = table.schema.get_field_index('timestamp')
    ts_column = table.column('timestamp').combine_chunks()
    ts_parsed = pc.strptime(
        ts_column,
        format='%Y-%m-%d %H:%M:%S',
        unit='ms',
        error_is_null=True
    )
    table = table.set_column(idx, 'timestamp', ts_parsed)

    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)

    # Upload the generated Parquet file to S3.
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue()
    )

    print(f"Parquet saved to s3://{bucket}/{key}")


def read_log_from_s3(bucket, key):
    # Read the raw log file from S3.
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    log_data = response['Body'].read().decode('utf-8')
    return log_data


def lambda_handler(event, context):
    # Get the bucket name and object key from the S3 event.
    record = event['Records'][0]
    bucket_name = record['s3']['bucket']['name']
    input_key = record['s3']['object']['key']

    print(f"Triggered by: s3://{bucket_name}/{input_key}")

    # Read the raw log data from S3.
    raw_logs = read_log_from_s3(bucket_name, input_key)

    # Split the log file into individual log entries.
    entries = [
        entry.strip()
        for entry in raw_logs.split('---')
        if entry.strip()
    ]

    # Define the pattern used to extract structured fields from each log entry.
    log_pattern = re.compile(
        r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(?P<log_level>[A-Za-z0-9_]+)\] '
        r'(?P<component>[^\s]+) - TicketID=(?P<ticket_id>[^\s]+) SessionID=(?P<session_id>[^\s]+)\s*'
        r'IP=(?P<ip>.*?) \| ResponseTime=(?P<response_time>-?\d+)ms \| CPU=(?P<cpu>[\d.]+)% \| EventType=(?P<event_type>.*?) \| Error=(?P<error>\w+)\s*'
        r'UserAgent="(?P<user_agent>.*?)"\s*'
        r'Message="(?P<message>.*?)"\s*'
        r'Debug="(?P<debug>.*?)"\s*'
        r'TraceID=(?P<trace_id>.*)'
    )

    # Extract structured fields from each valid log entry.
    parsed_entries = []

    for entry in entries:
        match = log_pattern.search(entry)

        if match:
            parsed_entries.append(match.groupdict())

    # Create a DataFrame from the parsed log records.
    df = pd.DataFrame(parsed_entries)

    # Remove the trace ID because it is not required for downstream analysis.
    df = df.drop('trace_id', axis=1)

    # Remove records with negative response times.
    df = df[df['response_time'].astype(int) >= 0]

    # Standardize inconsistent log-level values.
    fix_log_level = {
        'INF0': 'INFO',
        'DEBG': 'DEBUG',
        'warnING': 'WARNING',
        'EROR': 'ERROR'
    }

    df['log_level'] = df['log_level'].replace(fix_log_level)

    # Remove duplicate log records.
    df = df.drop_duplicates()

    # Convert columns to appropriate data types for analysis.
    df['response_time'] = df['response_time'].astype(int)
    df['cpu'] = df['cpu'].astype(float)
    df['error'] = df['error'].str.lower().map({
        'true': True,
        'false': False
    })

    # The timestamp remains a string here and is converted to a timestamp
    # type when the Parquet file is created.
    print(df.shape)
    print(df.head())

    # Generate the processed Parquet file name and S3 destination.
    output_file_name = os.path.basename(input_key).replace(".log", ".parquet")
    output_key = f'support-logs/processed/{output_file_name}'

    # Save the processed data as Parquet in S3.
    save_parquet_to_s3(df, bucket_name, output_key)
