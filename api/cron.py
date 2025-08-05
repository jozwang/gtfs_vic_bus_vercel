# File: api/cron.py

from flask import Flask, request
# Import your main function from the other file
from data_processor import process_and_store_schedules

app = Flask(__name__)

# This route will be triggered by the Vercel Cron Job
@app.route('/run-job', methods=['GET'])
def handler():
    # You can add a security check here if you want
    # For example, check a secret header to ensure only Vercel calls this
    
    # Run your main data processing function
    print("Cron job triggered. Starting data processing...")
    process_and_store_schedules()
    print("Data processing finished.")
    
    return "Cron job executed successfully.", 200
