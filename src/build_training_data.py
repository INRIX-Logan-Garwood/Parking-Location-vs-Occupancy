'''
This script is used to build the training data for the model.
Possible features include:
- wasserstein distance
- hotspot distance
- log probability
- in/out ratio
'''

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import geopandas as gpd
import folium
import os
from pathlib import Path
from shapely import wkt
from shapely.geometry import Point, Polygon, MultiPolygon
import shapely
import random
import inrix_data_science_utils.maps.quadkey as qkey
from inrix_data_science_utils.maps import get_distance_km, get_initial_bearing
import time
from sklearn.neighbors import KernelDensity
from scipy.stats import wasserstein_distance_nd

# os.chdir('../src')
from utils import *


## Constants ##
data_path = Path('data')
result_path = Path('results\csvs')
VERBOSE = True
TIME_ATT = 'timestamp'
GEO_ATT = 'geometry'
HUNTING_MODE = True
HUNTING_ATT = 'hunting_time'
PROJECTION = 'EPSG:4326'
# LOT_IDS = [329825, 375750, 380308, 381380, 381381, 387459]
LOT_IDS = [93059, 93121, 93224, 93240, 93313, 93318, 118208]
FEATURES = ['wasserstein', 'hotspot', 'log_prob', 'hunting_time', 'in_out_ratio']
NUM_HOTSPOTS = 3
TZ = 'America/Detroit'


def get_file_time_att(filename):
    '''
    Get the time attribute for that the filename
    Can customize to fit each different file
    '''
    if filename.startswith('trips_with_parking_time'):
        att = 'stop_time'
    else:
        att = 'start_time'
    return att

def convert_to_timezone(df, tz=TZ):
    '''
    Convert the time to the specified timezone
    '''
    if df[TIME_ATT].dt.tz is None:
        df[TIME_ATT] = df[TIME_ATT].dt.tz_localize('UTC')
    df[TIME_ATT] = df[TIME_ATT].dt.tz_convert(tz)
    return df

def load_data(dest_filepath, orig_filepath, lots_filepath):
    '''
    Load the data from the csv files.

    Dest/orig csv must have the following columns:
        - some time column name recognized by get_file_time_att
        - end_lat
        - end_lon
    lots csv must have the following columns:
        - geometry
    Returns:
        destination_trips_gpd: geopandas dataframe
        origin_trips_gpd: geopandas dataframe
        both will now have column called pk_lot from lots csv
    '''
    dest_trips = pd.read_csv(data_path / dest_filepath)
    orig_trips = pd.read_csv(data_path / orig_filepath)
    lots = pd.read_csv(data_path /  lots_filepath)

    # prelim preprocess
    if HUNTING_MODE:
        dest_trips[HUNTING_ATT] = pd.to_datetime(dest_trips['stop_time']) - pd.to_datetime(dest_trips['entry_time'])
        dest_trips[HUNTING_ATT] = dest_trips[HUNTING_ATT].dt.total_seconds()

    # time
    dest_trips[TIME_ATT] = pd.to_datetime(dest_trips[get_file_time_att(dest_filepath)])
    orig_trips[TIME_ATT] = pd.to_datetime(orig_trips[get_file_time_att(orig_filepath)])
    dest_trips = convert_to_timezone(dest_trips)
    orig_trips = convert_to_timezone(orig_trips)

    # geometry
    dest_trips[GEO_ATT] = [Point(lon, lat) for lon, lat in zip(dest_trips['end_lon'], dest_trips['end_lat'])]
    orig_trips[GEO_ATT] = [Point(lon, lat) for lon, lat in zip(orig_trips['start_lon'], orig_trips['start_lat'])]
    lots[GEO_ATT] = lots['geometry'].apply(wkt.loads)
    dest_trips = gpd.GeoDataFrame(dest_trips, geometry=GEO_ATT, crs=PROJECTION)
    orig_trips = gpd.GeoDataFrame(orig_trips, geometry=GEO_ATT, crs=PROJECTION)
    lots = gpd.GeoDataFrame(lots, geometry=GEO_ATT, crs=PROJECTION)

    # join and filter with parking lots
    dest_trips = gpd.sjoin(dest_trips, lots, how='inner', predicate='intersects')
    orig_trips = gpd.sjoin(orig_trips, lots, how='inner', predicate='intersects')
    dest_trips = dest_trips.drop(columns='index_right')
    orig_trips = orig_trips.drop(columns='index_right')

    return dest_trips, orig_trips

def get_KDE_dict(dest_trips, bw=0.00008):
    '''
    Get the lot_kde_dict storing info about KDE for each parking lot
    '''
    lot_kde_dict = {}
    for lot_id in LOT_IDS:
        dest_trips_lot = dest_trips[dest_trips['pk_lot'] == lot_id]
        (kde, xx, yy, zz) = get_KDE(dest_trips, bandwidth=bw, xbins=100j, ybins=100j)
        lot_kde_dict[lot_id] = (kde, xx, yy, zz)
    return lot_kde_dict

def get_features(f, df, long_term_df, lot_kde_dict, prefix=''):
    '''
    Extract features from the df. Df should be a relevant sample
    of data and could be a single row or a dataframe of n rows.
    Does not include the in_out_ratio feature

    Arguments:
    
        f: dictionary to store the features
        df: pd.DataFrame, the sample of data to extract features from
        long_term_df: pd.DataFrame, the long term data to calculate wasserstein distance
        lot_kde_dict: dictionary, storing the KDEs for each parking lot
        prefix: str, prefix to add to the feature names

    Returns:
        dictionary with keys of prefix-feature_name and values of the feature
    '''
    lot_id = df['pk_lot'].iloc[0]
    (kde, xx, yy, zz) = lot_kde_dict[lot_id]
    inference_coords = df[['end_lon', 'end_lat']].values

    # hotspot
    hot_distances = get_distance_to_nearest_hotspots(xx, yy, zz, inference_coords, k=NUM_HOTSPOTS)
    avg_hotspot_distance = np.mean(hot_distances)
    f[f'{prefix}hotspot'] = avg_hotspot_distance

    # wasserstein
    agg_coords = long_term_df[['end_lon', 'end_lat']].values
    mu = np.mean(agg_coords, axis=0)
    sigma = np.std(agg_coords, axis=0)
    normed_agg_coords = (agg_coords - mu) / sigma
    N = min(100, agg_coords.shape[0])   # simplify the computation
    normed_agg_coords = normed_agg_coords[np.random.choice(normed_agg_coords.shape[0], N, replace=False)]
    normed_inference_coords = (inference_coords - mu) / sigma
    w_distance = wasserstein_distance_nd(normed_inference_coords, normed_agg_coords)
    f[f'{prefix}wasserstein'] = w_distance

    # log probs
    log_prob = kde.score_samples(inference_coords)
    avg_log_prob = log_prob.mean()
    f[f'{prefix}log_prob'] = avg_log_prob

    # hunting mode
    if HUNTING_MODE:
        hunting_time = df[HUNTING_ATT].mean()
        f[f'{prefix}hunting_time'] = hunting_time

    return f

def main():
    ## Load data ##
    print('Loading data...')
    date_suffix = '2023-01-17_to_2023-01-23'
    # dest_filepath = f'trips_with_parking_time_{date_suffix}.csv'
    location = 'AnnArbor'
    dest_filepath = f'trips_with_parking_time_2022-12-20_to_2023-01-23_{location}.csv'
    orig_filepath = f'orig_trips_{location}_2022-11-01_to_2023-03-31.csv'
    lots_filepath = f'{location}_lot_geometries.csv'
    dest_trips, orig_trips = load_data(dest_filepath, orig_filepath, lots_filepath)
    print('Data loaded!')


    ## Kernel Density Estimation ##
    print('Calculating KDEs...')
    lot_kde_dict = get_KDE_dict(dest_trips)
    print("KDEs calculated!")


    ## Create the training data ##
    print('Creating training data...')
    dest_trips = dest_trips.sort_values(by=[TIME_ATT, 'pk_lot'])
    X = pd.DataFrame()
    medium_term_size = 8
    long_term_size = 50
    term_to_time = {'short': pd.Timedelta('1 hour'), 'medium': pd.Timedelta('4 hour'), 'long': pd.Timedelta('24 hour')}
    # iterate through dest_trips
    i = 0
    print_every = 500
    total = dest_trips.shape[0]
    print('Total rows:', total)
    for idx, row in dest_trips.iterrows():
        if VERBOSE and i % print_every == 0:
            print(f'{round(i / total * 100, 2)}% done')
            print('Current row processing:', row)
            print('Last row processed:', X.tail(1))
        timestamp = row[TIME_ATT]
        timestamp = pd.Timestamp(timestamp)
        timestamp = timestamp.replace(microsecond=0)  # remove the microseconds
        pk_lot = row['pk_lot']
        agg_trips = dest_trips[(dest_trips['pk_lot'] == pk_lot)]  # need this to calculate wasserstein distance
        dest_long_term = dest_trips[(dest_trips[TIME_ATT] <= timestamp) & (dest_trips['pk_lot'] == pk_lot)]
        dest_long_term = dest_long_term.sort_values(by=TIME_ATT, ascending=False)
        f = {}
        if dest_long_term.shape[0] > 0:
            for term in ['short', 'medium', 'long']:
                # get the in_out_ratio
                tradius = term_to_time[term]
                dest_time_window = dest_trips[(dest_trips[TIME_ATT] <= timestamp) & (dest_trips[TIME_ATT] > timestamp - tradius) & (dest_trips['pk_lot'] == pk_lot)]
                orig_time_window = orig_trips[(orig_trips[TIME_ATT] <= timestamp) & (orig_trips[TIME_ATT] > timestamp - tradius) & (orig_trips['pk_lot'] == pk_lot)]
                in_out_ratio = dest_time_window.size / max(1, orig_time_window.size)

                # get the lag dataframes
                if term == 'short':
                    dest_term = dest_long_term.head(1)
                elif term == 'medium':
                    dest_term = dest_long_term.head(medium_term_size)
                elif term == 'long':
                    dest_term = dest_long_term.head(long_term_size)

                # get the features
                f = get_features(f, dest_term, agg_trips, lot_kde_dict, prefix=f'{term}_')
                # add in_out_ratio
                f[f'{term}_in_out_ratio'] = in_out_ratio
            f['pk_lot'] = pk_lot
            f[TIME_ATT] = timestamp

            row = pd.DataFrame([f])
            X = pd.concat([X, row])
        else:
            print(f"No data for {row}")
        i += 1
    print('Training data created!')
    print(X)
    X.to_csv(result_path / f'training_data_{location}_{date_suffix}.csv', index=False)







if __name__ == '__main__':
    main()
