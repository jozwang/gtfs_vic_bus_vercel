# File: api/cron.py

from flask import Flask
# Import your main function from the other file

from data_processor import process_and_store_schedules

# Vercel specifically looks for a variable named 'app' in this file
app = Flask(__name__)

# This single "catch-all" route will handle any request sent to /api/cron
# This is more reliable than defining a specific sub-path like /run-job
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    print("--- Cron job triggered via Flask. Starting data processing... ---")
    
    # Run your main data processing function
    process_and_store_schedules()
    
    print("--- Data processing finished. Sending success response. ---")

    # Send a 200 OK response back
    return "Cron job executed successfully.", 200


