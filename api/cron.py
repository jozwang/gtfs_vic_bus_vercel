# File: api/cron.py

# This uses Python's standard library to create a serverless function handler
from http.server import BaseHTTPRequestHandler
# Import your main function from the other file
from data_processor import process_and_store_schedules

# Vercel looks for a class named 'handler' that inherits from this
class handler(BaseHTTPRequestHandler):

    # This function will run when a GET request is received
    def do_GET(self):
        print("--- Cron job triggered via HTTP GET. Starting data processing... ---")
        
        # Run your main data processing function
        process_and_store_schedules()
        
        print("--- Data processing finished. Sending success response. ---")

        # Send a 200 OK response back
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        self.wfile.write('Cron job executed successfully.'.encode('utf-8'))
        return
