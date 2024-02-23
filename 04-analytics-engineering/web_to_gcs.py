import io
import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from google.cloud import storage

# cd to working directory and run -> source secrets.sh


init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")


def clean_dataframe(df, color):
    if color == "fhv":
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
        df["PUlocationID"] = df["PUlocationID"].astype('Int64')
        df["DOlocationID"] = df["DOlocationID"].astype('Int64')
        df["SR_Flag"] = df["SR_Flag"].astype('Int64')
    else: 
        df["VendorID"] = df["VendorID"].astype('Int64')
        df["RatecodeID"] = df["RatecodeID"].astype('Int64')
        df["PULocationID"] = df["PULocationID"].astype('Int64')
        df["DOLocationID"] = df["DOLocationID"].astype('Int64')
        df["passenger_count"] = df["passenger_count"].astype('Int64')
        df["payment_type"] = df["payment_type"].astype('Int64')
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("string")
        if color == "yellow":
            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
            df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
        elif color == "green":
            df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
            df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
            df["trip_type"] = df["trip_type"].astype('Int64')
            df["ehail_fee"] = df["ehail_fee"].astype('Float64')

    return df


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        request_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' + file_name
        print(request_url)
        
        # Download file to local
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")
        
        # Used for uploading green and yellow tripdata 
        # df = pd.read_parquet(file_name, dtype_backend="pyarrow")
        
        # For FHV February file, there was record with dropOff_datetime of 3019 causing conversion errors.
        # Read table using pyarrow to remove outlier and convert back to dataframe
        table = pq.read_table(file_name)
        df = table.filter(
            pc.less_equal(table["dropOff_datetime"], pa.scalar(pd.Timestamp.max))
        ).to_pandas()
        
        df = clean_dataframe(df, service)
        df.to_parquet(file_name, compression="gzip")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")



# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')

