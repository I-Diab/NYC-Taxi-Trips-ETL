import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import requests
from datetime import datetime
import os
import json
from sqlalchemy import create_engine
import time

column_name_label = "Column name"
original_value_label = "Original value"
imputed_value_label = "Imputed Value"

# The dir where all of the data reside, including intermedite results
datasets_dir = "/opt/airflow/data/"

# Lookup table for the transformed values in the dataset
lookup = pd.DataFrame(columns=[column_name_label, original_value_label, imputed_value_label])

def establish_connection(db_name):
    engine = create_engine(f"postgresql://root:root@pgdatabase:5432/{db_name}")
    if(engine.connect()):
        print("Database connect successfully")
    else:
        print("Failed to connect to the databse")

    return engine

def upload_parquet(filename,table_name,engine):  
	df = pd.read_parquet(filename)
	try:
		df.to_sql(table_name, con = engine, if_exists='fail', index=False)
		print('parquet file uploaded to the db as a table')
	except ValueError as e:
		print("Table already exists. Error:", e)

def upload_csv(df: pd.DataFrame, table_name: str, engine):
    try:
        df.to_sql(table_name, con = engine, if_exists='fail', index=False)
        print('csv file uploaded to the db as a table')
    except ValueError as e:
        print("Table already exists. Error:", e)


def file_exists(file_to_check):
	if os.path.exists(file_to_check):
		return True
	return False

def rename_columns(df: pd.DataFrame):
    df_copy = df.copy()
    df_copy.columns = df_copy.columns.str.lower()
    df_copy.columns = [col.replace(" ","_") for col in df_copy.columns]
    return df_copy

def get_drop_off_city(row: pd.Series):
    if("do_location" in row.index):
        return str(row["do_location"]).split(",")[0]
    else:
       return str(row["DO Location"]).split(",")[0]

def get_pick_up_city(row: pd.Series):
    if("pu_location" in row.index):
        return str(row["pu_location"]).split(",")[0]
    else:
       return str(row["PU Location"]).split(",")[0]


def get_gps_location(row: pd.Series):
    while(True):
        try:
            if(np.isnan(row.longitude)):
                city = str(row.city).replace(' ','%20').replace('/',',')
                response = requests.get(f"http://api.positionstack.com/v1/forward?access_key={os.getenv('POSITION_STACK_KEY')}&query={city}")
                response = json.loads(response.text)
                print(city)
                print(response)
                if("data" in response.keys() and len(response['data'])>0):
                    gps_location = response['data'][0]
                    row.longitude = gps_location['longitude']
                    row.latitude = gps_location['latitude']
                    # gps_info.loc[len(gps_info)] = row
                    break
        except Exception as err:
            print(err, "Trying Again ...")
    
    return row

def get_gps_locations(lookup: pd.DataFrame):
    global datasets_dir

    if(os.path.exists(datasets_dir+"gps_location.csv")):
        gps_info = pd.read_csv(datasets_dir+"gps_location.csv")
    else:
        # get the dropoff and pickup cities using the lookup table
        addresses = lookup[lookup[column_name_label] == "location"][original_value_label]

        # get all known cities to avoid calling the API with unknown location
        known_addresses = pd.Series(filter(lambda element: element.split(",")[0] != "Unknown", addresses))

        # create a dataframe with all cities
        gps_info = pd.DataFrame(columns=["city","longitude","latitude"])
        gps_info["city"] = known_addresses

        gps_info.reset_index(drop=True, inplace=True)

        # call the api endpoint and fill in the gps_info dataframe
        gps_info = gps_info.apply(get_gps_location, axis=1)
        
        # save the gps_info
        gps_info.to_csv(datasets_dir+"gps_location.csv",index=False)

    return gps_info


def integrate_gps_locations(df_cleaned: pd.DataFrame, lookup: pd.DataFrame):
    global datasets_dir

    if(os.path.exists(datasets_dir+"gps_location.csv")):
        gps_info = pd.read_csv(datasets_dir+"gps_location.csv")
    else:
        gps_info = get_gps_locations(df_cleaned)

    # get the original values of the locations
    df_cleaned = df_cleaned.merge(lookup, how="left", left_on="do_location", right_on = imputed_value_label)
    df_cleaned = df_cleaned.rename(columns={original_value_label:"do_location_address"})
    df_cleaned = df_cleaned.drop(column_name_label, axis=1)
    df_cleaned = df_cleaned.drop(imputed_value_label, axis=1)

    df_cleaned = df_cleaned.merge(lookup, how="left", left_on="pu_location", right_on = imputed_value_label)
    df_cleaned = df_cleaned.rename(columns={original_value_label:"pu_location_address"})
    df_cleaned = df_cleaned.drop(column_name_label, axis=1)
    df_cleaned = df_cleaned.drop(imputed_value_label, axis=1)


    # merge our dataset and the gps_info
    df_with_gps = df_cleaned.merge(gps_info, how="left", left_on="do_location_address", right_on="city")
    df_with_gps = df_with_gps.rename(columns={"longitude":"do_longitude","latitude":"do_latitude"})
    df_with_gps = df_with_gps.drop("city", axis=1)
    df_with_gps = df_with_gps.merge(gps_info, how="left", left_on="pu_location_address", right_on="city")
    df_with_gps = df_with_gps.rename(columns={"longitude":"pu_longitude","latitude":"pu_latitude"})
    df_with_gps = df_with_gps.drop("city", axis=1)
    
    return df_with_gps

def add_row_to_lookup(row: []):
    global lookup
    lookup.loc[len(lookup.index)] = row 

def add_mappings_to_lookup(attribute_name: str, original_value:pd.Series, new_value:pd.Series):
    global lookup
    new_lookup = pd.DataFrame({column_name_label:pd.Series([attribute_name]*len(original_value)), original_value_label: original_value, imputed_value_label: new_value})
    lookup = pd.concat([lookup,new_lookup], ignore_index=True)
    
def get_lookup():
    global lookup
    return lookup

# Cleaning 

def remove_negative(col: pd.Series):
    if(col.dtype == np.float64): 
        return col.abs()
    else:
        return col

def handle_duplicates(df_cleaned: pd.DataFrame):
    return df_cleaned.drop_duplicates()

def handle_negative_attributes(df_cleaned: pd.DataFrame):
    return df_cleaned.apply(remove_negative)

def impute_passenger_count(df_cleaned: pd.DataFrame):
    df = df_cleaned.copy()
    df.passenger_count.fillna(1.0, inplace = True)
    return df

def impute_extra(df_cleaned: pd.DataFrame):
    df = df_cleaned.copy()
    df.extra.fillna(0.0, inplace=True)
    return df

def remove_congestion_surcharge(df_cleaned: pd.DataFrame):
    return df_cleaned.drop("congestion_surcharge", axis=1)


# Transformations

def convert_date_to_datetime(df_cleaned: pd.DataFrame):
    df = df_cleaned.copy()
    df.lpep_pickup_datetime = pd.to_datetime(df_cleaned.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df_cleaned.lpep_dropoff_datetime)
    
    return df

def create_week_number(df_cleaned: pd.DataFrame):
    df = df_cleaned.copy()
    df["week_number"] = pd.cut(df_cleaned['lpep_pickup_datetime'].dt.day, bins=[0, 7, 14, 21, 28, 31], labels=[1,2,3,4,5])

    return df

def create_date_range(df_cleaned:pd.DataFrame):
    df = df_cleaned.copy()
    df["date_range"] = (df.lpep_dropoff_datetime - df.lpep_pickup_datetime).dt.total_seconds()
    
    return df

def encode_vendor(df_cleaned: pd.DataFrame):
    one_hot_encoded_vendors = pd.get_dummies(df_cleaned.vendor) # I could have removed one columns
    
    return pd.concat([df_cleaned.drop("vendor",axis=1),one_hot_encoded_vendors], axis = 1)

def encode_store_and_fwd_flag(df_cleaned: pd.DataFrame):
    global datasets_dir

    df = df_cleaned.copy()
    df["store_and_fwd_flag"] = df.store_and_fwd_flag.replace({'N': 0, 'Y': 1})

    if(not os.path.exists(datasets_dir+"lookup_green_taxi_11_2017.parquet")):
        print("Adding store_and_fwd_flag map ...")
        add_row_to_lookup(["store_and_fwd_flag","N",0])
        add_row_to_lookup(["store_and_fwd_flag","Y",1])

    return df

def encode_rate_type(df_cleaned: pd.DataFrame):
    global datasets_dir

    df = df_cleaned.copy()
    label_encoder = LabelEncoder()
    df["rate_type"] = label_encoder.fit_transform(df.rate_type)
    
    if(not os.path.exists(datasets_dir+"lookup_green_taxi_11_2017.parquet")):
        print("Adding rate_type map ...")
        add_mappings_to_lookup("rate_type", label_encoder.classes_, label_encoder.transform(label_encoder.classes_))

    return df
    
def encode_locations(df_cleaned: pd.DataFrame):
    """
    This function encodes both pu_location and do_location together to make sure that 
    the same location takes the same encoded value in both features.
    """
    global datasets_dir

    df = df_cleaned.copy()
    
    # get all locations and removing duplicates
    locations = pd.concat([df.pu_location, df.do_location]).drop_duplicates()

    # creating and fitting the label encoder on the locations
    label_encoder = LabelEncoder()
    label_encoder.fit(locations)

    # overriding the location features with their encoding
    df["pu_location"] = label_encoder.transform(df.pu_location)
    df["do_location"] = label_encoder.transform(df.do_location)

    if(~os.path.exists(datasets_dir+"lookup_green_taxi_11_2017.parquet")):
        print("Adding location map ...")
        add_mappings_to_lookup("location", label_encoder.classes_, label_encoder.transform(label_encoder.classes_))

    return df

def encode_payment_type(df_cleaned: pd.DataFrame):
    global datasets_dir

    df = df_cleaned.copy()
    label_encoder = LabelEncoder()
    df["payment_type"] = label_encoder.fit_transform(df.payment_type)

    if(not os.path.exists(datasets_dir+"lookup_green_taxi_11_2017.parquet")):
        print("Adding payment_type map ...")
        add_mappings_to_lookup("payment_type", label_encoder.classes_, label_encoder.transform(label_encoder.classes_))

    return df

def encode_trip_type(df_cleaned: pd.DataFrame):
    global datasets_dir

    df = df_cleaned.copy()
    df["ordered_through_phone_or_app"] = df.trip_type.replace({'Street-hail': 0, 'Dispatch': 1})
    df.drop("trip_type", axis=1, inplace=True)

    if(not os.path.exists(datasets_dir+"lookup_green_taxi_11_2017.parquet")):
        print("Adding trip_type map ...")
        add_row_to_lookup(["ordered_through_phone_or_app","Street-hail",0])
        add_row_to_lookup(["ordered_through_phone_or_app","Dispatch",1])

    return df

def add_neighborhood(df_cleaned: pd.DataFrame):
    df = df_cleaned.copy()
    df["drop_off_neighborhood"] = df.apply(get_drop_off_city, axis=1)
    df["pick_up_neighborhood"] = df.apply(get_pick_up_city, axis=1)
    
    return df

def add_weekend(df_cleaned: pd.DataFrame):
    df = df_cleaned.copy()
    # 5 and 6 correspond to Saturday and Sunday
    df["weekend"] = df.apply(lambda row: row.lpep_pickup_datetime.dayofweek in (5, 6), axis=1)

    return df
