import pandas as pd
from utilities import *

database_name = "NYC_green_taxi"

def extract_clean(input_filename: str, output_filename: str) -> None:
    """
    Cleanses, imputes, and transforms a dataset from `input_file_name` and saves the transformed data to `output_file_name`.

    Args:
        input_filename: The path to the file containing the raw dataset (CSV format expected).
        output_filename: The path to the file where the transformed data will be saved (CSV format).

    Returns:
        None

    Assumptions:
        - The input file is in CSV format.
    """

    if(not file_exists(datasets_dir + output_filename)):
    
        df = pd.read_csv(datasets_dir+input_filename)

        df_cleaned = rename_columns(df)

        # Remove the duplicates and leave only one record from each duplicate
        df_cleaned = df_cleaned.drop_duplicates()

        df_cleaned = df_cleaned.apply(remove_negative)

        # Imputation

        df_cleaned = impute_passenger_count(df_cleaned)

        df_cleaned = impute_extra(df_cleaned)

        df_cleaned.drop("congestion_surcharge", axis=1, inplace=True)

        # Transformation

        # convert the string dates to Timestamp object
        df_cleaned = convert_date_to_datetime(df_cleaned)

        # Adding the pick up and drop off neighborhood
        df_cleaned = add_neighborhood(df_cleaned)

        # Adding a column indicating wether the trip took place in a weekend or not
        df_cleaned = add_weekend(df_cleaned)

        # Create week_number feature
        df_cleaned = create_week_number(df_cleaned)

        # Compute the difference between the dropoff and pickup times in seconds
        df_cleaned = create_date_range(df_cleaned)


        #### ADD GPS Locations


        # Encode the vendors using one hot encoding
        df_cleaned = encode_vendor(df_cleaned)

        # Encode the vendors using one hot encoding
        df_cleaned = encode_store_and_fwd_flag(df_cleaned)

        # Encode the rate_type using label encoding
        df_cleaned = encode_rate_type(df_cleaned)

        # Encode the locations using label encoding
        df_cleaned = encode_locations(df_cleaned)

        # Encode the payment_type using label encoding
        df_cleaned = encode_payment_type(df_cleaned)

        # Encode Payment type using binary encoding
        df_cleaned = encode_trip_type(df_cleaned)

        # Save Results
        print("Saving results ...")
        df_cleaned.to_csv(datasets_dir + output_filename, index=False)
        get_lookup().to_csv(datasets_dir + "lookup_" + output_filename, index=False)

    else:
        print("Cleaned dataset already exists on the file system")




def extract_additional_resources(transformed_csv_filename: str) -> None:
    """
    Calls an API to extract longitude and latitude of each of the pickup and dropoff locations.

    Args:
        locations: All the addresses that is required to get their longitude and latitude
    
    Returns:
        None
    """
    # Read the transformed dataset
    lookup = pd.read_csv(datasets_dir + "lookup_" + transformed_csv_filename)

    get_gps_locations(lookup)


def integrate_and_load(transformed_csv_filename: str) -> None:
    """
    Integerates the additional resources extracted by 'extract_additional_resources' with the dataset and loads them into postgres database

    Args:
        transformed_csv_filename: the name of the transformed dataset file which we need to integrate some additional data to it
    Returns:
        None
    """

    df_cleaned = pd.read_csv(datasets_dir + transformed_csv_filename)
    lookup = pd.read_csv(datasets_dir + "lookup_" + transformed_csv_filename)

    # include only those records related to location
    lookup = lookup[lookup[column_name_label] == "location"]

    # integrate the extracted data from the api with the cleaned dataset
    df_with_gps = integrate_gps_locations(df_cleaned, lookup)

    # establish connection with the database and load the integrated data and the lookup table
    con = establish_connection(database_name)
    upload_csv(df_with_gps, "M4_green_taxis_11_2017", con)
    upload_csv(lookup, "lookup_table", con)