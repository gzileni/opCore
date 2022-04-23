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

# ----------------------------
# Info:
# https://medium.com/@robertbracco1/how-to-write-a-telegram-bot-to-send-messages-with-python-bcdf45d0a580
import telegram_send

shapely.speedups.disable()

from pycopernicus import app

file_checked = ''
sentinel5p_config = {}

# get configuration product
def getConfig(product):
     # read db parameters
    parent_folder = pathlib.Path(__file__).parent.absolute()
    path_config = str(parent_folder) + "/config/" + "sentinel5p.json"
    f = open(path_config)
    product_config = json.load(f)
    f.close()

    return product_config

# get engine database
def getEngine(app):
    url = "postgresql://" + app.config["USERNAME_PG"] + ":" + app.config["PASSWORD_PG"] + "@" + \
        app.config["POSTGRESQL"] + ":" + \
        str(app.config["PORT"]) + "/" + app.config["DATABASE"]
    engine = create_engine(url)
    return engine

# check integrity file
def check_integrity_file(f):
    try:
        datas = xr.open_dataset(f,
                                engine="netcdf4",
                                group="PRODUCT",
                                decode_times=True,
                                decode_timedelta=True,
                                decode_coords=True,
                                parallel=True)
        msg = '\n' + str(datetime.datetime.now()) + \
            ' -  Check integrity file NETCD OK by ' + file_checked
        print(msg)
        logging.info(msg)
    except:
        msg = '\n' + str(datetime.datetime.now()) + \
            ' -  ERROR. check integrity to: ' + file_checked
        print(msg)
        logging.error(msg)
        os.remove(f)

# check integrity directory and removed 
def check_integrity_dir(path):
    for root, dirs, files in os.walk(path, topdown=False):
        files.sort()
        # check corrupted files
        for f in files:
            check_integrity_file(os.path.join(root, f))

#
def _update_db(data, bbox, table, path, engine):

    try:
        # create databrame
        pdataf = data.to_dataframe().dropna()
        
        s = geopandas.GeoSeries.from_xy(
            pdataf.latitude, pdataf.longitude,
            crs=app.config["S5_CRS"])  # .buffer(0.035, resolution=4, join_style=1)

        print('--- creating geometries ... ')
        # create geometry from bbox to intersect dataframe
        p1 = shapely.geometry.box(*bbox, ccw=True)
        geodf_l = geopandas.GeoDataFrame(
            geometry=geopandas.GeoSeries([p1]), crs=app.config["S5_CRS"])

        # ---------------------------------------------------------------
        # create dataframe with all dataframe's coordinates
        geodf_r = geopandas.GeoDataFrame(
            pdataf, geometry=s, crs=app.config["S5_CRS"])

        # update db to schema warning with all coordinates
        geodf = geopandas.sjoin(geodf_r, geodf_l)
        print('--- run spatial joins ... ')
        # geodf = geodf.drop['index_right']
        geodf.to_postgis(table,
                        engine,
                        schema=app.config["SCHEMA_DB"],
                        if_exists="append",
                        chunksize=app.config["CHUNKSIZE"])
        os.remove(path)

        if (len(geodf) > 0):
            # update db
            print(geodf.head(5))
            msg = '\nNuovi dati per inquinante ' + \
                table + \
                ' aggiornati.'
            logging.info(msg)
            print(msg)
            telegram_send.send(messages=[msg])
    except OSError as err:
            print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data.")
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")
    except:
        msg = 'Error to open path ' + path
        print(msg)
        logging.error(msg)
        os.remove(path)
# update postgis
def send_ncfiles(app, path, product, bbox):

    config = getConfig(product)
    product_config = config[product]
    
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        try:
            datas = xr.open_mfdataset(path,
                                    engine="netcdf4",
                                    group="PRODUCT",
                                    decode_times=True,
                                    decode_timedelta=True,
                                    decode_coords=True,
                                    parallel=True)

            # check integrity files
            print('\n --- reading ' + str(len(datas)) + ' rows...')

            # select quality data qa_value >= 0.75 
            datas_q = datas.where(datas.qa_value >= 0.75, drop=True)
            print('--- filtering values to ' + str(len(datas_q)))
            
            # get engine postgresql
            engine = getEngine(app)

            datas_q['platform'] = config["platform"]
            datas_q['description'] = product_config["description"]
            datas_q['created_at'] = datetime.datetime.now()

            _update_db(datas_q, bbox, product_config["table"], path, engine)
            
        except OSError as err:
            print("OS error: {0}".format(err))
        except ValueError:
            print("Could not convert data.")
        except BaseException as err:
            print(f"Unexpected {err=}, {type(err)=}")
        except:
            msg = 'Error to open path ' + path
            print(msg)
            logging.error(msg)
            os.remove(path)

# get geojson from postgis
def get_GeoJSON(app, product, code, lat, lng):
    config = getConfig(product)
    product_config = config[product]

    df = None
    # system reference
    crs = "EPSG:" + str(code)
    engine = getEngine(app)

    sql = "SELECT * FROM " + app.config["SCHEMA"] + "." + product_config["table"] + \
        " WHERE ST_Intersects('SRID=" + str(code) + \
        ";POINT(" + lng + " " + lat + ")'::geometry)"
    
    df = geopandas.read_postgis(
        sql,
        con=engine,
        geom_col="geometry",
        crs=crs)

    return df.to_json(na="drop", show_bbox=True)

# impot shapefile to postgis
def send_shape(app, shape, crs, table):
    gp = geopandas.read_file(shape).to_crs(crs=crs)
    pg_url = 'postgresql://' + app.config["USERNAME_PG"] + ':' + app.config["PASSWORD_PG"] + \
        '@' + app.config["POSTGRESQL"] + ':' + \
        str(app.config["PORT"]) + '/' + app.config["DATABASE"]
    engine = create_engine(pg_url)
    gp.to_postgis(table, engine, schema=app.config["SCHEMA"],
                  if_exists="replace", chunksize=app.config["CHUNKSIZE"])
            
