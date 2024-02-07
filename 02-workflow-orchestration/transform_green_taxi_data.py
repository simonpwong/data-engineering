import pandas as pd
import numpy as np
import re
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer    
def transform(data, *args, **kwargs):

    # Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]    
    
    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    data.columns = (data.columns
                    .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                    .str.lower()
                )
    data.rename({'pulocation_id': 'pu_location_id', 'dolocation_id': 'do_location_id'}, axis=1, inplace=True)
    
    return data


@test
def test_passenger_count_output(output, *args) -> None:
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 passengers'

@test
def test_trip_distance_output(output, *args) -> None:
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with trip distance of 0'

@test
def test_vendor_id_output(output, *args) -> None:
    assert 'vendor_id' in output, 'Column vendor_id does not exist in table'