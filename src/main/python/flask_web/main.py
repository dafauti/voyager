import os

from flask import Flask,render_template,request
from google.cloud import storage
from werkzeug.utils import secure_filename, redirect
from config import *
app = Flask(__name__)


@app.route("/")
def home():
    return "Hello !!"

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in extensions

## on page '/upload' load display the upload file
@app.route('/upload')
def upload_form():
    return render_template('upload.html')

#############################
# Additional Code Goes Here #
#############################

@app.route('/upload', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        if 'files[]' not in request.files:
            #flash('No files found, try again.')
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
        #flash('File(s) uploaded')
        return redirect('/upload')

'''
@app.route("/<string:username>")
def name(username):
    return "<html><h1> Welcome {} </h1></html>".format(username)

@app.route("/html_page")
def html():
    return render_template("demo.html")

@app.route("/css")
def css():
    return render_template("css.html")
'''
if __name__ == "__main__":
    app.run(port=8080)