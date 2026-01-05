from flask import jsonify
import functions_framework
from google.cloud import storage
# (No imports necessary in this section—tempfile is used and should be left as is)
import tempfile
import os

GCS_BUCKET_NAME = "data-innerai"   # Replace with your bucket
GCS_FOLDER = "data-and-analytics/raw-data/stripe/SOMETHING"                 # Replace with your folder (or leave as "")

@functions_framework.http
def upload_csv(request):
    """
    Google Cloud Function endpoint to receive .csv file uploads via POST request.
    Saves the file in a Google Cloud Storage folder.
    """
    if request.method != 'POST':
        return jsonify({"error": "Only POST method allowed"}), 405

    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    # Allow file extensions that are valid for BigQuery load jobs
    BQ_ALLOWED_EXTENSIONS = {'.csv', '.json', '.avro', '.parquet', '.orc', '.dat', '.txt'}

    def allowed_file_extension(filename):
        ext = os.path.splitext(filename.lower())[1]
        return ext in BQ_ALLOWED_EXTENSIONS

    if not allowed_file_extension(file.filename):
        return jsonify({"error": f"Uploaded file type not allowed. Allowed extensions: {', '.join(BQ_ALLOWED_EXTENSIONS)}"}), 400

    # Use the original extension for saving the temp file
    ext = os.path.splitext(file.filename)[1] or ".tmp"
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        file.save(tmp)
        tmp_path = tmp.name

    # Upload the file to Google Cloud Storage
    storage_client = storage.Client()
    destination_blob_name = f"{GCS_FOLDER}/{file.filename}" if GCS_FOLDER else file.filename
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(tmp_path)

    # Cleanup the temp file
    os.remove(tmp_path)

    return jsonify({"message": f"CSV file '{file.filename}' uploaded successfully to GCS at '{destination_blob_name}'"}), 200