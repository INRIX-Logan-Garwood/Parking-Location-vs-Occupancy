'''
Used to download trips data from s3
Need a dev role on analytics
'''
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd
import folium
from shapely import wkt

from trips_data_retrieval_utils import get_agg_trips_by_market