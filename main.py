import csv
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Lock for thread-safe writing
write_lock = Lock()

# Function to check redirect for a single URL
def check_redirect(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=30)
        if response.history:  # Check if there's any redirect
            return url, response.url, response.status_code
        else:
            return url, "No redirect", response.status_code
    except requests.Timeout:
        logger.error(f"Timeout occurred while processing URL: {url}")
        return url, "Timeout", None
    except requests.ConnectionError as e:
        logger.error(f"Connection error occurred while processing URL: {url}: {e}")
        return url, "Connection Error", None
    except requests.RequestException as e:
        logger.error(f"Error occurred while processing URL: {url}: {e}")
        return url, "Error", None
    

# Function to clean data (remove empty rows), remove any rows that are not urls sort and remove duplicates
def clean_data(data):
    return [row for row in data if any(row)]

# Function to process URLs with ThreadPoolExecutor
def process_urls_with_threadpool(urls):
    results = []
    total_domains = len(urls)
    domains_processed = 0
    with ThreadPoolExecutor(max_workers=20) as executor:
        for result in executor.map(check_redirect, urls):
            results.append(result)
            domains_processed += 1
            logger.info(f"Processed URL: {result[0]}")
            logger.info(f"Domains left to process: {total_domains - domains_processed}")
    return results

# Function to write data back to CSV
def write_data_to_csv(file_path, header, updated_data):
    with write_lock:
        with open(file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(header)  # Write header
            csv_writer.writerows(updated_data)  # Write updated data

# Main function
def main(file_path):
    try:
        start_time = time.time()
        # Read CSV file
        with open(file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            # Clean the data removing empty rows
            data = clean_data(list(csv_reader))

        header = data[0]  # Default header = Domain,Redirect URL, Status Code
        urls = [row[0] for row in data[1:]]

        # Process URLs using ThreadPoolExecutor
        results = process_urls_with_threadpool(urls)

        # Combine the results with the original data
        updated_data = [[url, redirect_url, status_code] for url, redirect_url, status_code in results]

        # Write data back to CSV
        write_data_to_csv(file_path, header, updated_data)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
        
        logger.info("CSV file successfully modified.")
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        logger.error("Please make sure the file exists and has data.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")

# File path
file_path = 'domains.csv'

# Entry point
if __name__ == "__main__":
    main(file_path)
