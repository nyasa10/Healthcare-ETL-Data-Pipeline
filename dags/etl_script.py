from datetime import datetime
import logging



# --- 1. EXTRACT ---
def extract_data():
    
    import pandas as pd
    raw_file_path = '/opt/airflow/data/healthcare_dataset.csv'
    try:
        df = pd.read_csv(raw_file_path)
        logging.info(f"Successfully extracted data. Shape: {df.shape}")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {raw_file_path}")
        return None


# --- 2. VALIDATE ---
def validate_data(df):
    
    if df is None:
        logging.warning("No data received for validation.")
        return None

    if not df['Age'].between(0, 120).all():
        raise ValueError("Invalid ages detected.")
    if df['Gender'].isnull().any():
        raise ValueError("Missing gender data detected.")
    if df['Date of Admission'].isnull().any() or df['Discharge Date'].isnull().any():
        raise ValueError("Missing admission/discharge dates.")

    logging.info("Data validation passed successfully.")
    return df



def transform_data(df):
    
    if df is None:
        logging.warning("No data received from extract task. Skipping transformation.")
        return None

    import pandas as pd

    df['Date of Admission'] = pd.to_datetime(df['Date of Admission'])
    df['Discharge Date'] = pd.to_datetime(df['Discharge Date'])
    df.dropna(inplace=True)
    df['Gender'] = df['Gender'].str.capitalize()
    df['Medical Condition'] = df['Medical Condition'].str.capitalize()

   
    df['Length of Stay (days)'] = (df['Discharge Date'] - df['Date of Admission']).dt.days
    bins = [0, 18, 35, 60, 120]
    labels = ['0-18', '19-35', '36-60', '60+']
    df['Age Group'] = pd.cut(df['Age'], bins=bins, labels=labels, right=False)

    
    df['Readmitted'] = df['Readmission Status'].map({'Yes': 1, 'No': 0})
    avg_stay = df['Length of Stay (days)'].mean()
    readmission_rate = df['Readmitted'].mean() * 100

    metrics = {
        'average_length_of_stay_days': round(avg_stay, 2),
        'readmission_rate_percent': round(readmission_rate, 2),
        'record_count': len(df)
    }

    logging.info(f"Transformation complete. Metrics: {metrics}")
    return {'data': df, 'metrics': metrics}


# --- 4. LOAD ---
def load_to_s3(result):
    
    if result is None:
        logging.warning("No data received from transform task. Skipping load.")
        return

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    df = result['data']
    metrics = result['metrics']

    s3_hook = S3Hook(aws_conn_id='aws_default')

   
    data_key = f'healthcare_data/processed/cleaned_data_{datetime.now().strftime("%Y-%m-%d")}.csv'
    csv_string = df.to_csv(index=False)
    s3_hook.load_string(
        string_data=csv_string,
        key=data_key,
        bucket_name='healthcare-etl-nyasa',
        replace=True
    )

  
    metrics_key = f'healthcare_data/metrics/kpi_summary_{datetime.now().strftime("%Y-%m-%d")}.json'
    import json
    s3_hook.load_string(
        string_data=json.dumps(metrics, indent=2),
        key=metrics_key,
        bucket_name='healthcare-etl-nyasa',
        replace=True
    )


    
