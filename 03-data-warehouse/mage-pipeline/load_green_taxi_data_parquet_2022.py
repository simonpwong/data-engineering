import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    df = []
    for i in range(1,13):
        if i < 10:
            url = ('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-0' + str(i) + '.parquet')
        else:
            url = ('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-' + str(i) + '.parquet')

        data = pd.read_parquet(url)
        df.append(data)

    return pd.concat(df, ignore_index=True)

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
