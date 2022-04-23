
import zipfile
import os
import uuid
import logging
import datetime

# check file allowed to upload
def allowed_file(app, filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def unZip(filezip, path):
    with zipfile.ZipFile(filezip, 'r') as zip_ref:
        extracted = zip_ref.namelist()
        zip_ref.extractall(path)
    return os.path.join(path, extracted[0])

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