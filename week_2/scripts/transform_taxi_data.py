import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def convert_snake(camel_input):
    words = re.findall(r'[A-Z]?[a-z]+|[A-Z]{2,}(?=[A-Z][a-z]|\d|\W|$)|\d+', camel_input)
    return '_'.join(map(str.lower, words))

@transformer
def transform(data, *args, **kwargs):
    
    print('passenger_count with zero: ', data[data.passenger_count == 0].passenger_count.count())
    
    print('trip_distance with zero distance: ', data[data.trip_distance == 0].trip_distance.count()) 

    print('passenger count is greater than 0 and the trip distance is greater than zero: ', len(data[(data.passenger_count > 0) & (data.trip_distance > 0)]))

    print('unique VendorID: ', data.VendorID.unique()) 

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    data.columns = [convert_snake(col) for col in data.columns]
    #data.columns = (data.columns
    #            .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
    #            .str.lower())
    # Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    return data[(data.passenger_count > 0) & (data.trip_distance > 0)]


@test
def test_output(output, *args) -> None:
    """
    Add three assertions:
        - vendor_id is one of the existing values in the column (currently)
        - passenger_count is greater than 0
        - trip_distance is greater than 0
    """
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'
    assert output['vendor_id'].isin([1,2]).count() == output['vendor_id'].count(), 'There are vendor_id error'
