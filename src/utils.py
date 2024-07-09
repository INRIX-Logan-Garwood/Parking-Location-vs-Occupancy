'''
This file contains utility functions that are used in the project.
'''

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import folium
import inrix_data_science_utils.maps.quadkey as qkey
import shapely
import matplotlib
import matplotlib.cm as cm
from matplotlib.colors import rgb2hex
from sklearn.neighbors import KernelDensity


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
                     opacity=0.8,
                     N=1):
    '''
    Add trips to the map and return the new map
    mode can be either 'lines', 'start', or 'end'
    '''
    if mode == 'lines':
        for i in range(0, trips_df.shape[0], N):
            trip = trips_df.iloc[i]
            folium.PolyLine(
                locations=[[trip['start_lat'], trip['start_lon']], [trip['end_lat'], trip['end_lon']],
                        [trip['end_lat'], trip['end_lon']]],
                color=color,
                weight=weight,
                opacity=opacity
            ).add_to(map)
    else:
        for i in range(0, trips_df.shape[0], N):
            trip = trips_df.iloc[i]
            folium.CircleMarker(
                location=[trip[f'{mode}_lat'], trip[f'{mode}_lon']],
                color=color,
                radius=weight,
                opacity=opacity
            ).add_to(map)
    return map

def poly_to_qkeys(poly: shapely.geometry.multipolygon, level: int):
    '''
    Convert a polygon to a list of quadkeys
    '''
    qk_list = []
    qk = qkey.QuadKey('0')
    centroid = poly.centroid
    qk = qk.from_geo((centroid.y, centroid.x), level)  # the center quadkey

    # breadth first search add quadkeys to qk_list until they intersect the polygon
    stack = [str(qk)]
    seen = set()
    while stack:
        qk_str = stack.pop()
        if qk_str in seen:
            continue
        else:
            qk = qkey.QuadKey(qk_str)
            if poly.intersects(qk.as_shapely_polygon()):
                qk_list.append(str(qk))
                seen.add(str(qk))
                neighbors = qk.nearby()
                for neighbor in neighbors:
                    stack.append(neighbor)
    return qk_list

def count_to_colour(variable, min_variable=0, max_variable=20, str_cmap='RdPu', scale='lin'):
    """Transforms given value to 0-1 range and then finds corresponding hex colour.
    Choose the type of normalisation: linear or logarithmic, as well as range
    of scaled variable and matplotlib colourmap.
    """
    norm_func = (
        matplotlib.colors.PowerNorm(.5, vmin=min_variable, vmax=max_variable, clip=True) if scale == "log"
        else matplotlib.colors.Normalize(vmin=min_variable, vmax=max_variable, clip=True)
    )
    # return matplotlib.colors.to_hex(cm.get_cmap(str_cmap)(norm_func(variable))) 
    return matplotlib.colors.to_hex(matplotlib.colormaps[str_cmap](norm_func(variable)))


def get_KDE(df, bandwidth=0.00005, xbins=100j, ybins=100j):
    '''
    Fit a kernel density estimator to the data
    Arguments:
        df: pd.DataFrame with columns 'end_lat' and 'end_lon'
        bandwidth: float, bandwidth of the KDE
    Returns:
        kde, xx, yy, zz: kde object and np.arrays, giving the mesh values for lon, lat, and probability respectively
    '''
    y = np.array(df['end_lat'])
    x = np.array(df['end_lon'])
    kde = KernelDensity(bandwidth=bandwidth, kernel='gaussian') #, metric='haversine')

    xx, yy = np.mgrid[x.min() : x.max() : xbins,
                    y.min() : y.max():ybins]
    xy_sample = np.vstack([xx.ravel(), yy.ravel()]).T
    xy_train = np.vstack([x, y]).T
    kde.fit(xy_train)

    z = np.exp(kde.score_samples(xy_sample))

    zz = z.reshape(xx.shape)
    return kde, xx, yy, zz

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