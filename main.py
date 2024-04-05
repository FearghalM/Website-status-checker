import csv
import requests
from concurrent.futures import ThreadPoolExecutor
import logging
from threading import Lock


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
    except requests.RequestException as e:
        logger.error(f"Error occurred while processing URL: {url}: {e}")
        return url, "Error", None

def cleanData(data):
    # Remove empty rows
    data = [row for row in data if any(row)]
    return data

def process_urls_with_threadpool(urls):
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        for result in executor.map(check_redirect, urls):
            results.append(result)
            logger.info(f"Processed URL: {result[0]}")
    return results

def write_data_to_csv(file_path, header, updated_data):
    """
    Write the modified data back to the CSV file after processing all URLs.
    
    Parameters:
    - file_path (str): The path to the CSV file.
    - header (list): The header of the CSV file.
    - updated_data (list of lists): The updated data to be written to the CSV file.
    """
    with write_lock:
        with open(file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(header)  # Write header
            csv_writer.writerows(updated_data)  # Write updated data

# Main function
def main(file_path):
    try:
        with open(file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            # Clean the data removing empty rows
            data = cleanData(list(csv_reader))

        header = data[0]# Default header = Domain,Redirect URL, Status Code
        urls = [row[0] for row in data[1:]]

        # Call the function to process URLs using ThreadPoolExecutor
        results = process_urls_with_threadpool(urls)

        # Combine the results with the original data
        updated_data = [[url, redirect_url, status_code] for url, redirect_url, status_code in results]

        # Call the function to write data to CSV
        write_data_to_csv(file_path, header, updated_data)

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
