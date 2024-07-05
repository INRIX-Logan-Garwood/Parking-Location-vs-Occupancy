'''
This file contains utility functions that are used in the project.
'''

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import folium
import inrix_data_science_utils.maps.quadkey as qkey


def add_qks_to_map(map : folium.Map, qk_list):
    '''
    Add quadkeys to the map and return the new map
    qk_list can be a list of strings or of QuadKey objects
    '''
    if qk_list[0].__class__ == str:
        qk_list = [qkey.QuadKey(qk) for qk in qk_list]

    for qk in qk_list:
        qk.show(map)
    return map

def add_trips_to_map(map: folium.Map, trips_df: pd.DataFrame,
                     mode='lines',
                     color='blue',
                     weight=1,
                     opacity=0.8):
    '''
    Add trips to the map and return the new map
    mode can be either 'lines', 'start', or 'end'
    '''
    if mode == 'lines':
        for i in range(trips_df.shape[0]):
            trip = trips_df.iloc[i]
            folium.PolyLine(
                locations=[[trip['start_lat'], trip['start_lon']], [trip['end_lat'], trip['end_lon']],
                        [trip['end_lat'], trip['end_lon']]],
                color=color,
                weight=weight,
                opacity=opacity
            ).add_to(map)
    else:
        for i in range(trips_df.shape[0]):
            trip = trips_df.iloc[i]
            folium.CircleMarker(
                location=[trip[f'{mode}_lat'], trip[f'{mode}_lon']],
                radius=weight,
                opacity=opacity
            ).add_to(map)
    return map


def main():
    qk_list = ['023112130', '023112131', '023112132']
    m = folium.Map(location=[47.6062, -122.3321], zoom_start=12)
    m = add_qks_to_map(m, qk_list)
    trips = pd.DataFrame({
        'start_lat': [47.6062, 47.6062],
        'start_lon': [-122.3321, -122.3321],
        'end_lat': [47.6062, 47.6062],
        'end_lon': [-122.3321, -122.3321]
    })
    m = add_trips_to_map(m, trips)

if __name__ == "__main__":
    main()