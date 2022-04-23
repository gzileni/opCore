import json
import pathlib
import os

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

import shapely.geometry
import xarray as xr
import geopandas
import pandas as pd
import dask
import logging
import datetime

from .config import load

# ----------------------------
# Info:
# https://medium.com/@robertbracco1/how-to-write-a-telegram-bot-to-send-messages-with-python-bcdf45d0a580
import telegram_send

shapely.speedups.disable()

file_checked = ''

# get engine database
def _get_engine(config):
    url = "postgresql://" + config["username"] + ":" + config["password"] + "@" + \
          config["url"] + ":" + \
          str(config["port"]) + "/" + config["database"]
    engine = create_engine(url)
    return engine

# | places | degrees    | distance |
# | ------ | ---------- | -------- |
# | 0      | 1.0        | 111 km   |
# | 1      | 0.1        | 11.1 km  |
# | 2      | 0.01       | 1.11 km  |
# | 3      | 0.001      | 111 m    |
# | 4      | 0.0001     | 11.1 m   |
# | 5      | 0.00001    | 1.11 m   |
# | 6      | 0.000001   | 0.111 m  |
# | 7      | 0.0000001  | 1.11 cm  |
# | 8      | 0.00000001 | 1.11 mm  |
def _convert_meters_degrees(meters):
    degres = meters / 111
    degres = degres * 0.001
    return degres

# get geojson from postgis
def get_json(params):
    
    db = load()["postgis"]
    # system reference
    crs = "EPSG:" + str(params["crs"])
    engine = _get_engine(db)

    sql = "SELECT * FROM " + db["schema"] + "." + params["config"]["table"] + \
          " WHERE ST_DWithin(geometry, ST_SetSRID(ST_Point(" + str(params["lng"]) + "," + \
          str(params["lat"]) + "), 4326), " + str(_convert_meters_degrees(params["meters"]))  + ")" + \
          " AND delta_time between '" + params["start"] + "' AND '" + params["end"] + "'"

    df = geopandas.read_postgis(
        sql,
        con=engine,
        geom_col="geometry",
        crs=crs)
    df['delta_time'] = df['delta_time'].astype(str)
    df['created_at'] = df['created_at'].astype(str)

    return df.to_json(na="drop", show_bbox=True)
            
