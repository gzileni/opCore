import json
import os
import uuid

# get configuration product
def load():
    # read db parameters
    directory = os.getcwd()
    print(directory)
    f = open(directory + "/core/config.json")
    product_config = json.load(f)
    f.close()

    return product_config

# create download files
def init_download(key):

    cfg = load()

    rootpath = os.path.abspath(os.getcwd()) + '/' + cfg['download']
    if (not os.path.isdir(rootpath)):
        os.mkdir(rootpath)

    rootpath += '/' + str(uuid.uuid4())
    if (not os.path.isdir(rootpath)):
        os.mkdir(rootpath)

    pathFiles = rootpath + "/" + key
    if (not os.path.isdir(pathFiles)):
        os.mkdir(pathFiles)

    return pathFiles, rootpath

# delete file downloaded
def delete_folder(path):
    for root, dirs, files in os.walk(path, topdown=False):
        files.sort()
        # check corrupted files
        for f in files:
            os.remove(os.path.join(root, f))
    os.rmdir(path)
    