import sys,os,re
from flask import Flask, flash, request, redirect, render_template
from werkzeug.utils import secure_filename
from config import *
from google.cloud import storage

app=Flask(__name__)
app.secret_key = app_key

app.config['MAX_CONTENT_LENGTH'] = file_mb_max * 1024 * 1024
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in extensions

## on page '/upload' load display the upload file
@app.route('/upload')
def upload_form():
    return render_template('upload.html')

def upload_to_gcp(file_name):
    # Setting credentials using the downloaded JSON file
    storage_client = storage.Client.from_service_account_json("/home/dafauti/git_repo/voyager/src/main/python/flask_gcp_api/gcp_api_key.json")
    # Creating bucket object
    bucket = storage_client.get_bucket("demo_gfs")
    #Name of the object to be stored in the bucket
    blob = bucket.blob(file_name)
    #Name of the object in local file system
    blob.upload_from_filename(file_name)

#############################
# Additional Code Goes Here #
#############################

@app.route('/upload', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        if 'files[]' not in request.files:
            flash('No files found, try again.')
            return redirect(request.url)
        files = request.files.getlist('files[]')
        print(files)
        for file in files:
            if file and allowed_file(file.filename):
                if not os.path.isdir(upload_dest):
                    os.mkdir(upload_dest)
                filename = secure_filename(file.filename)
                filepath = os.path.join(upload_dest, filename)
                file.save(filepath)
                # Setting credentials using the downloaded JSON file
                storage_client = storage.Client.from_service_account_json("gcp_api_key.json")
                # Creating bucket object
                bucket = storage_client.get_bucket("demo_gfs")
                filename = secure_filename(file.filename)
                #Name of the object to be stored in the bucket
                blob = bucket.blob(filename)

                #Name of the object in local file system
                blob.upload_from_filename(filepath)
                if os.path.isfile(filepath):
                    os.remove(filepath)
        flash('File(s) uploaded')
        return redirect('/upload')


if __name__ == "__main__":
    print('to upload files navigate to http://127.0.0.1:4000/upload')
    app.run(host='127.0.0.1',port=4000,debug=True,threaded=True)
