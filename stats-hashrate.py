#!/usr/bin/env python3
"""
Hashrate Data Scraper
Scrapes hashrate data from ocean.xyz and stores in CSV format
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import datetime
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import re


class HashrateDataScraper:
    """
    Web scraper class to fetch hashrate data from ocean.xyz
    """

    def __init__(self, url):
        """Initialize the scraper with target URL"""
        self.url = url
        self.driver = None
        print(f"Initializing scraper for URL: {url}")

    def setup_driver(self):
        """Setup Chrome driver with headless options"""
        print("Setting up Chrome driver...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        try:
            from selenium.webdriver.chrome.service import Service
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            print("Chrome driver setup successfully")
            return True
        except Exception as e:
            print(f"Error setting up driver: {e}")
            return False

    def fetch_data(self):
        """
        Fetch hashrate data from the website
        Returns dictionary with hashrate values or None if failed
        """
        print("Fetching data from website...")

        if not self.driver:
            if not self.setup_driver():
                print("ERROR: Failed to setup driver")
                return None

        try:
            # Load the page
            print("Loading page...")
            self.driver.get(self.url)
            print("Page loaded, waiting for content...")

            # Wait for the hashrate table to load
            wait = WebDriverWait(self.driver, 15)
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                print("Table element found")
            except:
                print("WARNING: Table not found, proceeding with current page content")

            # Get page source and parse with BeautifulSoup
            print("Parsing page content...")
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # Find the hashrate table
            hashrate_data = self.parse_hashrate_table(soup)

            if hashrate_data:
                print("Data fetched successfully")
                return hashrate_data
            else:
                print("ERROR: Failed to parse hashrate data")
                return None

        except Exception as e:
            print(f"ERROR: Exception during data fetch: {e}")
            print(f"ERROR: Exception type: {type(e).__name__}")
            return None

    def parse_hashrate_table(self, soup):
        """
        Parse the hashrate table from HTML soup
        Returns dictionary with time periods as keys and hashrate values
        """
        print("Parsing hashrate table...")

        hashrate_data = {}

        try:
            # Look for table rows containing hashrate data
            rows = soup.find_all('tr')
            print(f"Found {len(rows)} table rows")

            for i, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    # Extract time period and hashrate value
                    time_text = cells[0].get_text(strip=True)
                    hashrate_text = cells[1].get_text(strip=True)

                    print(f"Row {i}: Time='{time_text}', Hashrate='{hashrate_text}'")

                    # Check if this looks like a time period (24 hrs, 3 hrs, etc.)
                    if any(period in time_text.lower() for period in ['hr', 'min', 'sec']):
                        # Extract numeric value from hashrate (remove "Th/s" etc.)
                        hashrate_match = re.search(r'(\d+\.?\d*)', hashrate_text)
                        if hashrate_match:
                            hashrate_value = float(hashrate_match.group(1))
                            hashrate_data[time_text] = hashrate_value
                            print(f"PARSED: {time_text} -> {hashrate_value} Th/s")
                        else:
                            print(f"WARNING: Could not extract numeric value from '{hashrate_text}'")

            print(f"Total hashrate data points parsed: {len(hashrate_data)}")
            return hashrate_data if hashrate_data else None

        except Exception as e:
            print(f"ERROR: Exception during table parsing: {e}")
            print(f"ERROR: Exception type: {type(e).__name__}")
            return None

    def close(self):
        """Close the browser driver"""
        if self.driver:
            self.driver.quit()
            print("Browser driver closed")


class CSVManager:
    """
    Handles CSV file operations for storing hashrate data
    """

    def __init__(self, filename="hashrate_data.csv"):
        """Initialize CSV manager with filename"""
        self.filename = filename
        self.columns = ["timestamp", "24_hrs", "3_hrs", "10_min", "5_min", "60_sec"]
        print(f"CSV Manager initialized with file: {filename}")

    def initialize_csv(self):
        """Create CSV file with headers if it doesn't exist"""
        if not os.path.exists(self.filename):
            print("Creating new CSV file with headers...")
            df = pd.DataFrame(columns=self.columns)
            df.to_csv(self.filename, index=False)
            print("CSV file created successfully")
        else:
            print("CSV file already exists")

    def append_data(self, hashrate_data):
        """
        Append new hashrate data to CSV file
        hashrate_data: dictionary with time periods as keys
        """
        print("Appending data to CSV...")

        # Create timestamp in mm/dd hr:m:s format
        timestamp = datetime.datetime.now().strftime("%m/%d %H:%M:%S")
        print(f"Timestamp: {timestamp}")

        # Create row data with None as default for missing values
        row_data = {
            "timestamp": timestamp,
            "24_hrs": hashrate_data.get("24 hrs", None),
            "3_hrs": hashrate_data.get("3 hrs", None),
            "10_min": hashrate_data.get("10 min", None),
            "5_min": hashrate_data.get("5 min", None),
            "60_sec": hashrate_data.get("60 sec", None)
        }

        print(f"Row data to append: {row_data}")

        try:
            # Create DataFrame and append to CSV
            df = pd.DataFrame([row_data])
            df.to_csv(self.filename, mode='a', header=False, index=False)
            print("Data successfully appended to CSV")
        except Exception as e:
            print(f"ERROR: Failed to append data to CSV: {e}")
            print(f"ERROR: Exception type: {type(e).__name__}")

        print(f"CSV append completed: {row_data}")

    def get_latest_data(self, n=5):
        """Get the latest n records from CSV"""
        try:
            df = pd.read_csv(self.filename)
            return df.tail(n)
        except Exception as e:
            print(f"Error reading CSV: {e}")
            return None


class HashrateMonitor:
    """
    Main class to coordinate scraping and data storage
    """

    def __init__(self, url, csv_filename="hashrate_data.csv"):
        """Initialize monitor with URL and CSV filename"""
        self.url = url
        self.scraper = HashrateDataScraper(url)
        self.csv_manager = CSVManager(csv_filename)
        print("Hashrate Monitor initialized")

    def run_once(self):
        """Run a single data collection cycle"""
        print("\n" + "=" * 50)
        print(f"Starting data collection at {datetime.datetime.now()}")
        print("=" * 50)

        # Fetch data from website
        hashrate_data = self.scraper.fetch_data()

        if hashrate_data:
            print(f"Successfully collected {len(hashrate_data)} hashrate values")
            # Store data in CSV
            self.csv_manager.append_data(hashrate_data)
            print("Data collection cycle completed successfully")
            return True
        else:
            print("ERROR: Data collection cycle failed - no data retrieved")
            return False

    def run_continuous(self, interval_minutes=1):
        """
        Run continuous monitoring with specified interval
        interval_minutes: minutes between data collections
        """
        print(f"Starting continuous monitoring (every {interval_minutes} minute(s))")
        print("Press Ctrl+C to stop...")

        # Initialize CSV file
        self.csv_manager.initialize_csv()

        cycle_count = 0
        success_count = 0

        try:
            while True:
                cycle_count += 1
                print(f"\n--- CYCLE {cycle_count} ---")

                success = self.run_once()

                if success:
                    success_count += 1
                    print(f"SUCCESS: Cycle {cycle_count} completed. Total successes: {success_count}/{cycle_count}")
                else:
                    print(f"FAILURE: Cycle {cycle_count} failed. Total successes: {success_count}/{cycle_count}")
                    print("Will retry in next cycle...")

                # Wait for next interval
                wait_seconds = interval_minutes * 60
                print(f"Waiting {wait_seconds} seconds until next collection...")
                print(
                    f"Next collection at: {(datetime.datetime.now() + datetime.timedelta(seconds=wait_seconds)).strftime('%m/%d %H:%M:%S')}")
                time.sleep(wait_seconds)

        except KeyboardInterrupt:
            print(f"\nStopping continuous monitoring...")
            print(f"Final stats: {success_count} successes out of {cycle_count} cycles")
            self.cleanup()
        except Exception as e:
            print(f"ERROR: Unexpected error in continuous monitoring: {e}")
            print(f"ERROR: Exception type: {type(e).__name__}")
            print(f"Final stats: {success_count} successes out of {cycle_count} cycles")
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        print("Cleaning up resources...")
        self.scraper.close()
        print("Cleanup completed")


def main():
    """Main function to run the hashrate monitor"""
    print("Hashrate Data Scraper Starting...")
    print("=" * 50)

    # Configuration
    url = "https://ocean.xyz/stats/bc1qf6kklyuzuq7sg9dw8duqr7uu5xhl07xvvny4dk"
    csv_filename = "hashrate_data.csv"

    # Create monitor instance
    monitor = HashrateMonitor(url, csv_filename)

    print("\nStarting continuous monitoring mode (every 1 minute)...")
    print("This will append data to CSV file with timestamp format: mm/dd hr:m:s")
    print("=" * 50)

    try:
        monitor.run_continuous(interval_minutes=1)
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        monitor.cleanup()


if __name__ == "__main__":
    main()