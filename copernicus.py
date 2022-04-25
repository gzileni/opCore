import requests
import os
import logging
import datetime
import xml.etree.ElementTree as ET
import shapely.geometry
import sys
import uuid

from .postgis import send_ncfiles

shapely.speedups.disable()

# delete file downloaded
def delete_folder(path):
    for root, dirs, files in os.walk(path, topdown=False):
        files.sort()
        # check corrupted files
        for f in files:
            os.remove(os.path.join(root, f))
    os.rmdir(path)
    logging.info(str(datetime.datetime.now()) +
                 ' -  delete datasets from : ' + path)

# create download files
def create_download_folder(app, product):

    rootpath = os.path.abspath(os.getcwd()) + '/' + app.config['DOWNLOAD_FOLDER']
    if (not os.path.isdir(rootpath)):
        os.mkdir(rootpath)

    rootpath += '/' + str(uuid.uuid4())
    if (not os.path.isdir(rootpath)):
        os.mkdir(rootpath)

    pathFiles = rootpath + "/" + product
    if (not os.path.isdir(pathFiles)):
        os.mkdir(pathFiles)

    return pathFiles, rootpath

# get footprint to query
def getFootprint(bbox):
    polygon = shapely.geometry.box(*bbox, ccw=True)
    return 'footprint:"Intersects(' + str(polygon) + ')"'

# download
def run(app, url, f, username, password, product, path, bbox):
      
      result = ''
      netCDFile = open(f, "wb")
      try:
            # get .nc files from datahub if don't exist
            fileNC = requests.get(url,
                                  timeout=120,
                                  verify=True,
                                  auth=(username,
                                        password),
                                  stream=True)
            total_length = int(fileNC.headers.get('content-length'))
            dl=0
            for chunk in fileNC.iter_content(chunk_size=app.config["CHUNKSIZE"]):
                  # An approximation as the chunks don't have to be 512 bytes
                  dl += len(chunk)
                  done = int(50 * dl / total_length)
                  sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)))
                  sys.stdout.flush()
                  netCDFile.write(chunk)
            netCDFile.close()

            logging.info(str(datetime.datetime.now()) +
                  ' -  Download NETCD datasets Ok to ' + path)
            result = path

            # -----------------------------------------
            # update postgis
            send_ncfiles(app, f, product, bbox)

            return result

      except requests.ReadTimeout:
            logging.error(str(datetime.datetime.now()) +
                        ' -  Readtimeout from : ' + url)
            return False
      except:
            logging.error(str(datetime.datetime.now()) +
                        ' -  errror')
            return False

# ----------------------------------------------------------------
# download file .nc
def download(app, path, ncFiles, product, ext, username, password, bbox):

      path, dirs, files = next(os.walk(path))
      print('------------------------------')
      msg = str(datetime.datetime.now()) + '\nstart downloading from ' + product + ' to ' + path
      logging.info(msg)
      print(msg)
      print('------------------------------')

      index_file = 1
      ncFiles.sort()
      for ncFile in ncFiles:
            # ----------------------------------------------
            # download netcd file from sentinel hub
            pathFile = path + '/' + product + "_" + str(index_file) + ext
            print('\nDownloading: ' + pathFile, end="\n")

            # download dataset
            run(app, 
                ncFile, 
                pathFile,
                username,
                password, 
                ext == '.zip', 
                product, 
                index_file,
                path,
                bbox)

            index_file += 1

# parse xml response from copernicus
def parseXML(xmlfile):

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
def getDatasets(app, url, username, password):

      try:
            # ----------------------------------------------
            # request HTTP GET data
            response = requests.get(url, verify=True, stream=True, timeout=120, auth=(username, password))
            if (response.status_code == 200):
                  return parseXML(response.content), True, 200
            else:
                  logging.warning(str(datetime.datetime.now()) +
                        ' -  Status Code : ' + str(response.status_code) + ' from ' + url)
                  response.raise_for_status()
                  return [], True, response.status_code
      except requests.ReadTimeout:
            logging.error(str(datetime.datetime.now()) +
                  ' -  Readtimeout from : ' + url)
            return [], True, requests.ReadTimeout


