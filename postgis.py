from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

import shapely.geometry
import geopandas
import logging
import telegram_send

from .core import load

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

# update postgis         
def update_db(data, params):

    try:

        db = load()["postgis"]
        engine = _get_engine(db)

        # create databrame
        pdataf = data.to_dataframe().dropna()
        
        s = geopandas.GeoSeries.from_xy(
            pdataf.latitude, pdataf.longitude,
            crs=params["crs"])  # .buffer(0.035, resolution=4, join_style=1)

        print('--- creating geometries ... ')
        # create geometry from bbox to intersect dataframe
        p1 = shapely.geometry.box(*params["bbox"], ccw=True)
        geodf_l = geopandas.GeoDataFrame(
            geometry=geopandas.GeoSeries([p1]), crs=params["crs"])

        # ---------------------------------------------------------------
        # create dataframe with all dataframe's coordinates
        geodf_r = geopandas.GeoDataFrame(
            pdataf, geometry=s, crs=params["crs"])

        # update db to schema warning with all coordinates
        geodf = geopandas.sjoin(geodf_r, geodf_l)
        print('--- run spatial joins ... ')
        # geodf = geodf.drop['index_right']
        geodf.to_postgis(table,
                        engine,
                        schema=db["schema"],
                        if_exists="append",
                        chunksize=db["chunksize"])

        if (len(geodf) > 0):
            # update db
            print(geodf.head(5))
            msg = '\nNuovi dati per inquinante ' + \
                table + \
                ' aggiornati.'
            logging.info(msg)
            print(msg)
            telegram_send.send(messages=[msg])
            return '', False
    except OSError as err:
        msg = "OS error: {0}".format(err)
        return msg, True
    except ValueError:
        msg = "Could not convert data."
        return msg, True
    except BaseException as err:
        msg = "Unexpected " + type(err)
        return msg, True
    except:
        msg = 'Error to save to database '
        return msg, False
        #print(msg)
        #logging.error(msg)
        #os.remove(path)