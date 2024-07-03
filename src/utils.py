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


def main():
    qk_list = ['023112130', '023112131', '023112132']
    m = folium.Map(location=[47.6062, -122.3321], zoom_start=12)
    m = add_qks_to_map(m, qk_list)

if __name__ == "__main__":
    main()