#!/usr/bin/env python3
"""
Hashrate Data Scraper v89
Scrapes hashrate data from ocean.xyz and stores in CSV format
"""

VERSION = "v89"

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


def load_config():
    """Load configuration from config.txt"""
    if not os.path.exists("config.txt"):
        print("config.txt needed - see github for description")
        sys.exit(1)

    bitcoin_address = None
    worker_names = []
    refresh_interval = 1

    try:
        with open("config.txt", 'r') as f:
            lines = f.readlines()

        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()

                    if key == 'bitcoin_address':
                        bitcoin_address = value
                    elif key == 'worker_name':
                        if value:  # Only add non-empty worker names
                            worker_names.append(value)
                    elif key == 'refresh':
                        refresh_interval = int(value)

        return bitcoin_address, worker_names, refresh_interval

    except Exception as e:
        print(f"Error reading config.txt: {e}")
        sys.exit(1)


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

    def fetch_data(self, target_workers):
        """
        Fetch hashrate data from the website
        Returns tuple (hashrate_data, workers_data) or (None, None) if failed
        """
        print("Fetching data from website...")

        if not self.driver:
            if not self.setup_driver():
                print("ERROR: Failed to setup driver")
                return None, None

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

            # Find the hashrate table and workers data
            hashrate_data = self.parse_hashrate_table(soup)
            workers_data = self.parse_workers_data(soup, target_workers)

            if hashrate_data:
                print("Hashrate data fetched successfully")
            else:
                print("ERROR: Failed to parse hashrate data")

            if workers_data:
                print("Workers data fetched successfully")
            else:
                print("ERROR: Failed to parse workers data")

            return hashrate_data, workers_data

        except Exception as e:
            print(f"ERROR: Exception during data fetch: {e}")
            print(f"ERROR: Exception type: {type(e).__name__}")
            return None, None

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

    def parse_workers_data(self, soup, target_workers):
        """
        Parse workers data from HTML soup
        Returns dictionary with worker names as keys and their data as values
        """
        print("Parsing workers data...")
        print(f"Target workers: {target_workers}")

        workers_data = {}

        try:
            # Look for table rows containing worker data
            rows = soup.find_all('tr')
            print(f"Found {len(rows)} table rows for workers")

            for i, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 5:  # Nickname, Status, Last Share, Hashrate(60s), Hashrate(3hr), Earnings
                    nickname = cells[0].get_text(strip=True)
                    status = cells[1].get_text(strip=True)
                    last_share = cells[2].get_text(strip=True)
                    hashrate_60s = cells[3].get_text(strip=True)
                    hashrate_3hr = cells[4].get_text(strip=True)
                    earnings = cells[5].get_text(strip=True) if len(cells) > 5 else "0"

                    print(f"Row {i}: Worker='{nickname}', Status='{status}'")

                    # Only collect data for target workers
                    if nickname in target_workers:
                        # Extract numeric values
                        hashrate_60s_val = self.extract_numeric(hashrate_60s)
                        hashrate_3hr_val = self.extract_numeric(hashrate_3hr)
                        earnings_val = self.extract_earnings(earnings)

                        workers_data[nickname] = {
                            'last_share': last_share,
                            'status': status,
                            'hashrate_60s': hashrate_60s_val,
                            'hashrate_3hr': hashrate_3hr_val,
                            'earnings': earnings_val
                        }

                        print(f"COLLECTED: {nickname} -> {workers_data[nickname]}")

            print(f"Total target workers found: {len(workers_data)}")
            return workers_data if workers_data else None

        except Exception as e:
            print(f"ERROR: Exception during workers parsing: {e}")
            print(f"ERROR: Exception type: {type(e).__name__}")
            return None

    def extract_numeric(self, text):
        """Extract numeric value from text and convert to Th/s"""
        match = re.search(r'(\d+\.?\d*)', text)
        if match:
            value = float(match.group(1))
            # Convert based on unit suffix
            if 'Gh/s' in text or 'GH/s' in text:
                return value / 1000  # Convert GH/s to TH/s
            elif 'Th/s' in text or 'TH/s' in text:
                return value
            elif 'Ph/s' in text or 'PH/s' in text:
                return value * 1000  # Convert PH/s to TH/s
            else:
                return value  # Assume TH/s if no unit
        return 0.0

    def extract_earnings(self, text):
        """Extract earnings value and format as decimal"""
        match = re.search(r'(\d+\.?\d*(?:[eE][+-]?\d+)?)', text)
        if match:
            value = float(match.group(1))
            return f"{value:.8f}"  # Format to 8 decimal places
        return "0.00000000"

    def close(self):
        """Close the browser driver"""
        if self.driver:
            self.driver.quit()
            print("Browser driver closed")


class WorkerCSVManager:
    """
    Handles CSV file operations for individual worker hashrate data
    """

    def __init__(self, worker_name):
        """Initialize worker CSV manager"""
        self.worker_name = worker_name
        self.filename = f"{worker_name}_hashrate_data.csv"
        self.columns = ["id", "last_share", "status", "60s", "3hr", "earnings", "min_3hr", "avg_3hr", "max_3hr"]
        print(f"Worker CSV Manager initialized for {worker_name}: {self.filename}")

    def initialize_csv(self):
        """Create CSV file with headers if it doesn't exist"""
        if not os.path.exists(self.filename):
            print(f"Creating new worker CSV file: {self.filename}")
            df = pd.DataFrame(columns=self.columns)
            df.to_csv(self.filename, index=False)
            print(f"Worker CSV created: {self.filename}")
        else:
            print(f"Worker CSV exists: {self.filename}")

    def get_next_id(self):
        """Get the next ID for the worker CSV record"""
        try:
            if os.path.exists(self.filename):
                df = pd.read_csv(self.filename)
                if len(df) > 0:
                    last_id = df['id'].max()
                    return last_id + 1
                else:
                    return 0
            else:
                return 0
        except Exception as e:
            print(f"ERROR: Failed to get next ID for {self.worker_name}: {e}")
            return 0

    def append_data(self, worker_data):
        """Append worker data to CSV"""
        print(f"Appending data for worker {self.worker_name}...")

        # Get next ID
        next_id = self.get_next_id()

        # Calculate statistics from historical data
        stats = self.calculate_statistics()

        # Create row data
        row_data = {
            "id": next_id,
            "last_share": worker_data['last_share'],
            "status": worker_data['status'],
            "60s": worker_data['hashrate_60s'],
            "3hr": worker_data['hashrate_3hr'],
            "earnings": worker_data['earnings'],
            "min_3hr": stats['min_3hr'],
            "avg_3hr": stats['avg_3hr'],
            "max_3hr": stats['max_3hr']
        }

        print(f"Worker {self.worker_name} data: {row_data}")

        try:
            df = pd.DataFrame([row_data])
            df.to_csv(self.filename, mode='a', header=False, index=False)
            print(f"Data appended for worker {self.worker_name}")
        except Exception as e:
            print(f"ERROR: Failed to append data for {self.worker_name}: {e}")

    def calculate_statistics(self):
        """Calculate min, avg, mean, max, std from historical 3hr hashrate data"""
        try:
            if os.path.exists(self.filename):
                df = pd.read_csv(self.filename)
                if len(df) > 0 and '3hr' in df.columns:
                    # Get last 10 records for statistics (or all if less than 10)
                    recent_data = df['3hr'].tail(10).dropna()

                    if len(recent_data) > 0:
                        return {
                            'min_3hr': round(float(recent_data.min()), 2),
                            'avg_3hr': round(float(recent_data.mean()), 2),
                            'max_3hr': round(float(recent_data.max()), 2)
                        }

            # Default values if no historical data
            return {
                'min_3hr': 0.0,
                'avg_3hr': 0.0,
                'max_3hr': 0.0
            }

        except Exception as e:
            print(f"ERROR: Failed to calculate statistics for {self.worker_name}: {e}")
            return {
                'min_3hr': 0.0,
                'avg_3hr': 0.0,
                'max_3hr': 0.0
            }


class WorkersManager:
    """
    Manages multiple individual worker CSV files
    """

    def __init__(self, target_workers):
        """Initialize managers for target workers"""
        self.target_workers = target_workers
        self.worker_managers = {}

        for worker in target_workers:
            self.worker_managers[worker] = WorkerCSVManager(worker)

        print(f"Workers Manager initialized for: {target_workers}")

    def initialize_all_csvs(self):
        """Initialize all worker CSV files"""
        for worker, manager in self.worker_managers.items():
            manager.initialize_csv()

    def append_workers_data(self, workers_data):
        """Append data for all workers that have data"""
        if not workers_data:
            print("No workers data to append")
            return

        for worker_name, worker_data in workers_data.items():
            if worker_name in self.worker_managers:
                self.worker_managers[worker_name].append_data(worker_data)
            else:
                print(f"Skipping unknown worker: {worker_name}")


class CSVManager:
    """
    Handles CSV file operations for storing hashrate data
    """

    def __init__(self, filename="hashrate_data.csv"):
        """Initialize CSV manager with filename"""
        self.filename = filename
        self.columns = ["id", "timestamp", "24hrs", "3hrs", "10min", "5min", "60s", "min_10m", "avg_10m", "max_10m",
                        "min_3hr", "avg_3hr", "max_3hr", "min_24hr", "avg_24hr", "max_24hr"]
        print(f"CSV Manager initialized with file: {filename}")

    def initialize_csv(self):
        """Create CSV file with headers if it doesn't exist"""
        if not os.path.exists(self.filename):
            print("Creating new CSV file with headers...")
            df = pd.DataFrame(columns=self.columns)
            df.to_csv(self.filename, index=False)
            print("CSV file created successfully")
        else:
            print("CSV file exists")

    def get_next_id(self):
        """Get the next ID for the CSV record"""
        try:
            if os.path.exists(self.filename):
                df = pd.read_csv(self.filename)
                if len(df) > 0:
                    last_id = df['id'].max()
                    next_id = last_id + 1
                    print(f"Next ID: {next_id}")
                    return next_id
                else:
                    print("Empty CSV file, starting with ID 0")
                    return 0
            else:
                print("CSV file doesn't exist, starting with ID 0")
                return 0
        except Exception as e:
            print(f"ERROR: Failed to get next ID: {e}")
            return 0

    def append_data(self, hashrate_data):
        """
        Append new hashrate data to CSV file
        hashrate_data: dictionary with time periods as keys
        """
        print("Appending data to CSV...")

        # Get next ID
        next_id = self.get_next_id()

        # Create timestamps
        now = datetime.datetime.now()
        timestamp = now.strftime("%m/%d %H:%M:%S")

        print(f"ID: {next_id}")
        print(f"Timestamp: {timestamp}")

        # Calculate statistics from historical data
        stats = self.calculate_statistics()

        # Create row data with None as default for missing values
        row_data = {
            "id": next_id,
            "timestamp": timestamp,
            "24hrs": hashrate_data.get("24 hrs", None),
            "3hrs": hashrate_data.get("3 hrs", None),
            "10min": hashrate_data.get("10 min", None),
            "5min": hashrate_data.get("5 min", None),
            "60s": hashrate_data.get("60 sec", None),
            "min_10m": stats['min_10m'],
            "avg_10m": stats['avg_10m'],
            "max_10m": stats['max_10m'],
            "min_3hr": stats['min_3hr'],
            "avg_3hr": stats['avg_3hr'],
            "max_3hr": stats['max_3hr'],
            "min_24hr": stats['min_24hr'],
            "avg_24hr": stats['avg_24hr'],
            "max_24hr": stats['max_24hr']
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

    def calculate_statistics(self):
        """Calculate min, avg, max statistics for 10min, 3hrs, and 24hrs from historical data"""
        try:
            if os.path.exists(self.filename):
                df = pd.read_csv(self.filename)
                if len(df) > 0:
                    # Get last 10 records for statistics (or all if less than 10)
                    recent_df = df.tail(10)

                    # Calculate stats for 10min column
                    min_10m = avg_10m = max_10m = 0.0
                    if '10min' in df.columns:
                        data_10m = recent_df['10min'].dropna()
                        if len(data_10m) > 0:
                            min_10m = round(float(data_10m.min()), 2)
                            avg_10m = round(float(data_10m.mean()), 2)
                            max_10m = round(float(data_10m.max()), 2)

                    # Calculate stats for 3hrs column
                    min_3hr = avg_3hr = max_3hr = 0.0
                    if '3hrs' in df.columns:
                        data_3hr = recent_df['3hrs'].dropna()
                        if len(data_3hr) > 0:
                            min_3hr = round(float(data_3hr.min()), 2)
                            avg_3hr = round(float(data_3hr.mean()), 2)
                            max_3hr = round(float(data_3hr.max()), 2)

                    # Calculate stats for 24hrs column
                    min_24hr = avg_24hr = max_24hr = 0.0
                    if '24hrs' in df.columns:
                        data_24hr = recent_df['24hrs'].dropna()
                        if len(data_24hr) > 0:
                            min_24hr = round(float(data_24hr.min()), 2)
                            avg_24hr = round(float(data_24hr.mean()), 2)
                            max_24hr = round(float(data_24hr.max()), 2)

                    # Calculate mean and std from 10min data (or use 3hrs as fallback)
                    mean_data = recent_df['10min'].dropna() if '10min' in df.columns else recent_df[
                        '3hrs'].dropna() if '3hrs' in df.columns else []
                    mean_val = round(float(mean_data.mean()), 2) if len(mean_data) > 0 else 0.0
                    std_val = round(float(mean_data.std()), 2) if len(mean_data) > 1 else 0.0

                    return {
                        'min_10m': min_10m,
                        'avg_10m': avg_10m,
                        'max_10m': max_10m,
                        'min_3hr': min_3hr,
                        'avg_3hr': avg_3hr,
                        'max_3hr': max_3hr,
                        'min_24hr': min_24hr,
                        'avg_24hr': avg_24hr,
                        'max_24hr': max_24hr
                    }

            # Default values if no historical data
            return {
                'min_10m': 0.0,
                'avg_10m': 0.0,
                'max_10m': 0.0,
                'min_3hr': 0.0,
                'avg_3hr': 0.0,
                'max_3hr': 0.0,
                'min_24hr': 0.0,
                'avg_24hr': 0.0,
                'max_24hr': 0.0
            }

        except Exception as e:
            print(f"ERROR: Failed to calculate statistics: {e}")
            return {
                'min_10m': 0.0,
                'avg_10m': 0.0,
                'max_10m': 0.0,
                'min_3hr': 0.0,
                'avg_3hr': 0.0,
                'max_3hr': 0.0,
                'min_24hr': 0.0,
                'avg_24hr': 0.0,
                'max_24hr': 0.0
            }

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

    def __init__(self, url, worker_names, csv_filename="hashrate_data.csv"):
        """Initialize monitor with URL and CSV filename"""
        self.url = url
        self.scraper = HashrateDataScraper(url)
        self.csv_manager = CSVManager(csv_filename)
        self.workers_manager = WorkersManager(worker_names)
        print("Hashrate Monitor initialized")

    def run_once(self):
        """Run a single data collection cycle"""
        print("\n" + "=" * 50)
        print(f"Starting data collection at {datetime.datetime.now()}")
        print("=" * 50)

        # Fetch data from website
        hashrate_data, workers_data = self.scraper.fetch_data(self.workers_manager.target_workers)

        success = False

        if hashrate_data:
            print(f"Successfully collected {len(hashrate_data)} hashrate values")
            # Store hashrate data in CSV
            self.csv_manager.append_data(hashrate_data)
            success = True
        else:
            print("ERROR: Failed to collect hashrate data")

        if workers_data:
            print(f"Successfully collected {len(workers_data)} worker values")
            # Store workers data in individual CSV files
            self.workers_manager.append_workers_data(workers_data)
            success = True
        else:
            print("ERROR: Failed to collect workers data")

        if success:
            print("Data collection cycle completed successfully")
        else:
            print("ERROR: Data collection cycle failed - no data retrieved")

        return success

    def run_continuous(self, interval_minutes=1):
        """
        Run continuous monitoring with specified interval
        interval_minutes: minutes between data collections
        """
        print(f"Starting continuous monitoring (every {interval_minutes} minute(s))")
        print("Press Ctrl+C to stop...")

        # Initialize CSV files
        self.csv_manager.initialize_csv()
        self.workers_manager.initialize_all_csvs()

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
    print(f"Hashrate Data Scraper {VERSION}")
    print("=" * 50)

    # Load configuration
    bitcoin_address, worker_names, refresh_interval = load_config()

    if not bitcoin_address:
        print("ERROR: No bitcoin address in config.txt")
        sys.exit(1)

    # Build URL
    url = f"https://ocean.xyz/stats/{bitcoin_address}"

    print(f"Worker names: {worker_names}")
    print(f"Refresh interval: {refresh_interval} minutes")

    # Create monitor instance
    monitor = HashrateMonitor(url, worker_names)

    print("\nStarting continuous monitoring mode...")
    print("This will append data to CSV files with timestamp format: mm/dd hr:m:s")
    print("=" * 50)

    try:
        monitor.run_continuous(interval_minutes=refresh_interval)
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        monitor.cleanup()


if __name__ == "__main__":
    main()