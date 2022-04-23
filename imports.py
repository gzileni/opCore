import os

from pycopernicus import app

from flask import flash, request, redirect, url_for, send_from_directory
from werkzeug.utils import secure_filename
from .geoserver import publish_shape
from .postgis import send_shape
from .functions import allowed_file, unZip

uploads_path = os.path.abspath(os.getcwd()) + '/' + app.config['UPLOAD_FOLDER']

# POST /imports 
# imports shapefile to postgis
@app.route('/imports', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(app, file.filename):
            filename = secure_filename(file.filename)
            # create uploads folder if not exists
            if (not os.path.isdir(uploads_path)):
                os.mkdir(uploads_path)
            # save file
            shapefileZipped = os.path.join(uploads_path, filename)
            file.save(shapefileZipped)
            pathShape = unZip(shapefileZipped, uploads_path)

            # imports to postgres
            send_shape(app, pathShape, request.form['crs'], request.form['layer'])
            # publish layer
            publish_shape(app, request.form['layer'])

            # send response to clients
            return redirect(url_for('download_file', name=filename))

# GET uploads
@app.route('/uploads/<name>')
def download_file(name):
    return send_from_directory(uploads_path, name)