import csv
import requests
from concurrent.futures import ThreadPoolExecutor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    except Exception as e:
        logger.error(f"Error occurred while processing URL: {url}: {e}")
        return url, "error", e


# Main function
def main(file_path):
    try:
        with open(file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            data = list(csv_reader)

        header = data[0]
        urls = [row[0] for row in data[1:]]

        results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            for result in executor.map(check_redirect, urls):
                results.append(result)

        # Combine the results with the original data
        updated_data = [[url, redirect_url, status_code] for url, redirect_url, status_code in results]

        # Write the modified data back to the CSV file after processing all URLs
        with open(file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(header)  # Write header
            csv_writer.writerows(updated_data)  # Write updated data

        logger.info("CSV file successfully modified.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")

# File path
file_path = 'domains.csv'

# Entry point
if __name__ == "__main__":
    main(file_path)
