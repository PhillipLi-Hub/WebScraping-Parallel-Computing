import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import warnings

# Suppress all warnings
warnings.filterwarnings("ignore")
original_stdout = sys.stdout
original_stderr = sys.stderr

start = datetime.now()
chrome_options = Options()
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")  # Run in headless mode
chrome_options.add_argument("--log-level=3")  # Set log level to only show fatal errors (3)
chrome_options.add_argument("--silent")  # Suppress ChromeDriver output
chrome_options.add_argument("--no-sandbox")  # If running in a Linux environment, this helps avoid some errors
chrome_options.add_argument("--disable-dev-shm-usage")  # Useful in some cases to avoid memory issues
chrome_options.add_argument("--remote-debugging-port=0")  # Disable DevTools listening output
# Redirect output streams to avoid logging to the console
f = open(os.devnull, 'w')
sys.stdout = f
sys.stderr = f

# Get the current folder path
current_folder_path = os.getcwd()
corrected_path = current_folder_path.replace('\\', '/')  # Windows uses backslashes
# Path to your chromedriver.exe
driver_path = corrected_path+'/chromedriver.exe'

# Path to your chromedriver.exe

# Function to scrape a specific webpage and extract data
def scrape_twse_page(url, etf_etp_label):
    # Set up Chrome service
    service = Service(executable_path=driver_path)
    service.log_path = 'NUL'  # This prevents the logs from being displayed in the console

    # Initialize the Chrome WebDriver
    driver = webdriver.Chrome(service=service, options=chrome_options)
    sys.stdout = original_stdout
    sys.stderr = original_stderr

    # Load the main webpage with Selenium
    driver.get(url)

    # Wait for dynamic content to load (use WebDriverWait instead of fixed sleep)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, 'table'))
        )
    except Exception as e:
        print(f"Error: {e}")
        driver.quit()
        return []

    # Get the page source after the content has loaded
    page_source = driver.page_source

    # Parse the page source with BeautifulSoup
    soup = BeautifulSoup(page_source, 'html.parser')

    # Close the browser once the ETF/ETP codes have been extracted
    driver.quit()

    # Find the table with ETF/ETP data
    table = soup.find('table')

    if table is None:
        print(f"No table found on {url}")
        return []

    # Initialize a list to hold data for the DataFrame
    data = []

    # Loop through each row in the table to extract ETF/ETP data
    for row in table.find_all('tr')[1:]:  # Skipping the header row
        cells = row.find_all('td')

        # Extract the listing date, security code, ETF/ETP name, and issuer
        if len(cells) >= 4:
            listing_date = cells[0].text.strip()
            security_code = cells[1].text.strip()
            etf_name = cells[2].text.strip()
            issuer = cells[3].text.strip()

            # Split the listing date and security code into multiple parts if necessary
            listing_date_parts = [part for part in listing_date.split(')') if part.strip()]
            security_code_parts = [part for part in security_code.split(')') if part.strip()]

            # Process each part of the listing date and security code
            for i in range(len(listing_date_parts)):
                # Retain the parentheses for the listing date if the original part had it
                ld = listing_date_parts[i].strip()
                if "TWD" in ld or "RMB" in ld or "USD" in ld:
                    ld += ')'

                # Remove parentheses and content inside for the security code
                sc = security_code_parts[i].split('(')[0].strip() + ' TT'

                # Append the row to the data list for DataFrame
                data.append([ld, sc, etf_name, issuer, etf_etp_label, "TWSE"])

        elif len(cells) == 2:
            # Handle cases where there are additional entries for the same ETF/ETP (additional security code and issuer)
            security_code = cells[0].text.strip()
            issuer = cells[1].text.strip()

            # Clean the security code (remove brackets and currency designations inside)
            sc_clean = security_code.split('(')[0].strip() + ' TT'

            # Append the row to the data list for DataFrame, leaving the listing date and ETF/ETP name empty (since it's implied from the previous row)
            data.append(['', sc_clean, '', issuer, etf_etp_label, "TWSE"])

    return data

# Scrape the ETF page
etf_data = scrape_twse_page("https://www.twse.com.tw/en/products/securities/etf/products/list.html", "ETF")
print('twse etf completed.')
# Scrape the ETP page using the correct link
etp_data = scrape_twse_page("https://www.twse.com.tw/en/products/securities/etn/products/list.html", "ETP")
print('twse etp completed.')
# Combine both datasets
combined_data = etf_data + etp_data

# Create a DataFrame from the combined data
df_twse = pd.DataFrame(combined_data, columns=['Listing Date', 'Security Code', 'ETF/ETP Name', 'Issuer', 'ETF/ETP', 'Exchange'])


# Set up Chrome service
service = Service(executable_path=driver_path)

# Function to clean security code by removing any content inside parentheses and appending " TT"
def clean_security_code(security_code):
    cleaned_code = security_code.split('(')[0].strip()  # Remove parentheses and content within them
    if not cleaned_code.endswith('TT'):
        cleaned_code += ' TT'
    return cleaned_code
# Function to clean security code by removing any content inside parentheses and appending " TT"

# Everything else remains unchanged

# Function to fetch issuer information from the detail link (to be run in parallel)
def fetch_issuer_from_link(link):
    try:
        detail_response = requests.get(link)
        detail_soup = BeautifulSoup(detail_response.text, 'html.parser')

        # Find the issuer/manager info
        issuer_label = detail_soup.find('td', text=lambda x: x and 'Issuer/Manager' in x)
        if issuer_label:
            issuer_info = issuer_label.find_next('td').text.strip()
            return issuer_info
        return 'Unknown'
    except Exception as e:
        return f'Error fetching issuer: {e}'

# Function to extract data from a specific ETF or ETP page
def extract_data_from_page(url, etf_etp_label, listing_date_col):
    # Initialize the Chrome WebDriver
    service.log_path = "NUL"  # This prevents the logs from being displayed in the console
    driver = webdriver.Chrome(service=service, options=chrome_options)
    sys.stdout = original_stdout
    sys.stderr = original_stderr

    try:
        # Load the main webpage with Selenium
        driver.get(url)
        time.sleep(5)  # Wait for the page to load

        # Get the page source after the content has loaded
        page_source = driver.page_source

        # Parse the page source with BeautifulSoup
        soup = BeautifulSoup(page_source, 'html.parser')

        # Find the table with ETF/ETP data
        table = soup.find('table')

        # Initialize a list to hold data for the DataFrame
        data = []

        # Loop through each row in the table to extract ETF/ETP data
        for row in table.find_all('tr')[1:]:  # Skipping the header row
            cells = row.find_all('td')

            if len(cells) > listing_date_col:  # Ensure the correct number of columns exists
                code = clean_security_code(cells[0].text.strip()) + " TT"  # Security code + " TT"
                name = cells[1].text.strip()  # ETF/ETP name
                listing_date = cells[listing_date_col].text.strip()  # Listing date based on index

                # Get issuer link from the "Information" column (usually the last column)
                link_cell = cells[-1].find('a', href=True)
                if link_cell:
                    link_url = 'https://www.tpex.org.tw' + link_cell['href']  # Build the full link
                else:
                    link_url = ''  # If no link is found

                # Append the row (without issuer, only the link) to the data list
                data.append([listing_date, code, name, link_url, etf_etp_label, "TPE"])

    finally:
        # Close the browser after data extraction
        driver.quit()

    return data

# List of ETF and ETP URLs and their corresponding detail page prefixes for TPEX
etf_etp_sources = [
    {
        "url": "https://www.tpex.org.tw/web/etf/etf_specification_domestic.php?l=en-us",
        "label": "ETF",
        "listing_date_col": 2  # Listing Date is in the third column (index 2)
    },
    {
        "url": "https://www.tpex.org.tw/web/etf/etf_specification_foreign.php?l=en-us",
        "label": "ETF",
        "listing_date_col": 2  # Listing Date is in the third column (index 2)
    },
    {
        "url": "https://www.tpex.org.tw/web/etf/etf_bond.php?l=en-us",
        "label": "ETF",
        "listing_date_col": 2  # Listing Date is in the third column (index 2)
    },
    {
        "url": "https://www.tpex.org.tw/web/etn/etn_listed.php?l=en-us",
        "label": "ETP",  # ETP specific label
        "listing_date_col": 4  # Listing Date is in the fifth column (index 4)
    }
]

# Step 1: Loop through each source and extract data without issuers
tpex_data = []
for source in etf_etp_sources:
    tpex_data.extend(extract_data_from_page(source["url"], source["label"], source["listing_date_col"]))

# Step 2: Use ThreadPoolExecutor to fetch issuers in parallel
issuer_links = [row[3] for row in tpex_data if row[3]]  # Only take rows where there's a valid link
with ThreadPoolExecutor(max_workers=10) as executor:
    fetched_issuers = list(executor.map(fetch_issuer_from_link, issuer_links))

# Step 3: Update the issuer info back into tpex_data
issuer_index = 0
for row in tpex_data:
    if row[3]:  # Only update if there's a valid link
        row[3] = fetched_issuers[issuer_index]
        issuer_index += 1
print('tpse etf/etp completed.')
# Convert the TPEX data into a Pandas DataFrame
df_tpex = pd.DataFrame(tpex_data, columns=['Listing Date', 'Security Code', 'ETF/ETP Name', 'Issuer', 'ETF/ETP', 'Exchange'])
df_tpex['Security Code'] = df_tpex['Security Code'].apply(lambda x: x[:-3] if len(x) > 3 else x)
# Display the DataFrame
df_twse['Listing Date Temp'] = df_twse['Listing Date'].str.split('(').str[0]
# Next, replace dots with dashes for consistency if needed
df_twse['Listing Date Temp'] = df_twse['Listing Date Temp'].str.replace('.', '-')
# Now, convert the column to datetime format
df_twse['Listing Date Temp'] = pd.to_datetime(df_twse['Listing Date Temp'], format='%Y-%m-%d', errors='coerce')
df_tpex['Listing Date Temp'] = pd.to_datetime(df_tpex['Listing Date'], format='%Y/%m/%d')


df = df_twse.append(df_tpex)
current_month_start = pd.to_datetime(datetime.now().strftime('%Y-%m-01'))

# Filter the dataframe to select rows where 'Listing Date' is before the current month
df_filtered = df[df['Listing Date Temp'] < current_month_start]
# Get the current date
current_date = datetime.now()

# Subtract one month to get the previous month
last_month = current_date - relativedelta(months=1)

# Format the result as a string in the format "YYYY-MM"
last_month_string = last_month.strftime('%Y_%m')
df_filtered = df_filtered.drop( 'Listing Date Temp',axis = 1)
df_filtered.to_excel(corrected_path+'/'+last_month_string+'_scraped_comparison.xlsx')
end = datetime.now()
execution_time = (end - start).total_seconds()

# Round the execution time to 1 decimal place
print("Total time to execute is "+str(round(execution_time, 1))+" s.")