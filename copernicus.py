import requests
import os
import logging
import datetime
import xml.etree.ElementTree as ET
import shapely.geometry
import sys
import uuid
import dask
import xarray as xr

from .postgis import update_db
from .core import init_download, delete_folder

shapely.speedups.disable()

__CHUNKSIZE__ = 10000

# get footprint to query
def footprint(bbox):
    polygon = shapely.geometry.box(*bbox, ccw=True)
    return 'footprint:"Intersects(' + str(polygon) + ')"'

# range to dataset download
def range(days):
      return 'ingestiondate:[NOW-' + str(days) + 'DAYS TO NOW]'

# ----------------------------------------------------------------
# download dataset NETCDFile .nc
def _download(params):

      path, dirs, files = next(os.walk(params["path"]))
      print('------------------------------')
      msg = str(datetime.datetime.now()) + '\nstart downloading from ' + params["product"] + ' to ' + path
      logging.info(msg)
      print(msg)
      print('------------------------------')

      index_file = 1
      params["list"].sort()
      for datasetNC in params["list"]:
            # ----------------------------------------------
            # download netcd file from sentinel hub
            f = path + '/' + params["product"] + "_" + str(index_file) + params["ext"]
            print('\nDownloading: ' + f, end="\n")

            netCDFile = open(f, "wb")

            try:
                  # get .nc files from datahub if don't exist
                  fileNC = requests.get(datasetNC,
                                        timeout=120,
                                        verify=True,
                                        auth=(params["username"],
                                              params["password"]),
                                        stream=True)
                  total_length = int(fileNC.headers.get('content-length'))
                  dl=0
                  for chunk in fileNC.iter_content(chunk_size=__CHUNKSIZE__):
                        # An approximation as the chunks don't have to be 512 bytes
                        dl += len(chunk)
                        done = int(50 * dl / total_length)
                        sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)))
                        sys.stdout.flush()
                        netCDFile.write(chunk)
                  netCDFile.close()

                  logging.info(str(datetime.datetime.now()) +
                        ' -  Download NETCD datasets Ok to ' + path)
                  
                  data = open_dataset_NC(f)
                  if (data is not None):
                        data['platform'] = params["platform"]
                        data['description'] = params["description"]
                        data['created_at'] = datetime.datetime.now()
                        params_db = {
                              "crs": params["crs"],
                              "bbox": params["bbox"]
                        }
                        # update postgis
                        errorMsg, error = update_db(data, params_db)
                        if (error == True):
                              print(errorMsg)
                              logging.error(errorMsg)
                              # remove dataset
                              os.remove(f)
                  
                  return True

            except requests.ReadTimeout:
                  logging.error(str(datetime.datetime.now()) +
                              ' -  Readtimeout from : ' + params["url"])
                  return False
                  
            except:
                  logging.error(str(datetime.datetime.now()) +
                              ' -  errror')
                  return False

            index_file += 1

      # remove path
      delete_folder(path)

# parse xml response from copernicus
def _parseXML(xmlfile):

      # create element tree object
      tree = ET.fromstring(xmlfile)

      # get root element
      root = tree.iter('*')
      
      # create empty list for news items
      linkitems = []

      # iterate news items
      for child in root:
            if ('link' in child.tag):
                  if ('href' in child.attrib):
                        for item in child.attrib:
                              if ('$value' in child.attrib[item] and not 'Quicklook' in child.attrib[item]):
                                    linkitems.append(child.attrib[item])

      return linkitems

# create list's url download datasets from sentinel hub
def datasets(params):

      # -----------------------------------------
      # create download folder if not exists          
      pathFiles, rootPath = init_download(params["product"])

      result = {
            "error": '',
            "status": 200,
            "datasets": []
      }

      try:
            # ----------------------------------------------
            # request HTTP GET data
            response = requests.get(params["url"], 
                                    verify=True, 
                                    stream=True, 
                                    timeout=120, 
                                    auth=(params["username"], params["password"]))
            if (response.status_code == 200):
                  ncFiles = _parseXML(response.content)
                  result["datasets"] = ncFiles
                  result["status"] = 200
                  result["error"] = ''

                  if (len(ncFiles) > 0):

                        params_download = {
                              "path": pathFiles,
                              "list": ncFiles,
                              "product": params["product"],
                              "ext": ".nc",
                              "username": params["username"],
                              "password": params["password"],
                              "bbox": params["bbox"],
                              "platform": params["platform"],
                              "description": params["description"],
                              "crs": params["crs"]
                        }

                        # -----------------------------------------
                        # download datasets
                        _download(params_download)
                  
            else:
                  logging.warning(str(datetime.datetime.now()) +
                        ' -  Status Code : ' + str(response.status_code) + ' from ' + params["url"])
                  response.raise_for_status()
                  result["status"] = response.status_code
                  result["error"] = 'Can\'t download datasets'

            return result
      except requests.ReadTimeout:
            logging.error(str(datetime.datetime.now()) +
                  ' -  Readtimeout from : ' + params["url"])
            result["status"] = 500
            result["error"] = requests.ReadTimeout

            return result

# open dataset and save to postgis
def open_dataset_NC(path):

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

            return datas_q
            
        except OSError as err:
            print("OS error: {0}".format(err))
            return None
        except ValueError:
            print("Could not convert data.")
            return None
        except BaseException as err:
            print(f"Unexpected {err=}, {type(err)=}")
            return None
        except:
            msg = 'Error to open path ' + path
            print(msg)
            logging.error(msg)
            os.remove(path)
            return None
