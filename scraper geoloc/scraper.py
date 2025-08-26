import json
import random
import time
import logging
import argparse
from dataclasses import dataclass, asdict, field
from itertools import product
from typing import List, Tuple, Optional

import pandas as pd
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Page
from filelock import FileLock

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants (updated for better configurability)
SEARCH_BOX_XPATH = '//input[@id="searchboxinput"]'  # Still valid as of 2025 checks
LISTING_XPATH = '//a[contains(@href, "https://www.google.com/maps/place")]'  # Common pattern, reliable
DETAILS_TIMEOUT = 10000
SCROLL_DELAY_MIN = 0.2
SCROLL_DELAY_MAX = 1.2
SCROLL_COUNT = 10
SCROLL_VALUE_MIN = 300
SCROLL_VALUE_MAX = 800
PAGE_LOAD_TIMEOUT = 60000
PAGE_WAIT_TIMEOUT = 5000
RANDOM_DELAY_MIN = 2
RANDOM_DELAY_MAX = 5
PROCESSED_LOG_FILE = 'processed_combinations.json'
LOCK_FILE = 'file.lock'

# Updated selectors based on recent UI checks (minor tweaks for stability)
NAME_XPATH = '//h1[contains(@class, "DUwDvf") and contains(@class, "lfPIob")]'
ADDRESS_XPATH = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
WEBSITE_XPATH = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
PHONE_XPATH = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
REVIEWS_XPATH = '//span[@role="img" and contains(@class, "F7nice")]'

@dataclass
class Business:
    """Holds business data with additional fields for completeness."""
    name: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    phone_number: Optional[str] = None
    reviews_count: Optional[int] = None
    reviews_average: Optional[float] = None
    latitude: Optional[float] = None  # New field
    longitude: Optional[float] = None  # New field

@dataclass
class BusinessList:
    """Holds list of Business objects and saves to various formats."""
    business_list: List[Business] = field(default_factory=list)

    def dataframe(self) -> pd.DataFrame:
        """Transforms business_list to pandas dataframe."""
        return pd.json_normalize((asdict(business) for business in self.business_list), sep="_")

    def save_to_excel(self, filename: str) -> None:
        """Saves pandas dataframe to excel (xlsx) file."""
        self.dataframe().to_excel(f"{filename}.xlsx", index=False)

    def save_to_csv(self, filename: str) -> None:
        """Saves pandas dataframe to csv file."""
        self.dataframe().to_csv(f"{filename}.csv", index=False)

    def save_to_json(self, filename: str) -> None:
        """Appends list of Business objects to an existing or new JSON file, avoiding duplicates."""
        lock = FileLock(LOCK_FILE)
        with lock:
            try:
                with open(f"{filename}.json", "r") as f:
                    existing_data = json.load(f)
            except FileNotFoundError:
                existing_data = []

            existing_identifiers = {(business['name'], business['address']) for business in existing_data if business['name'] and business['address']}
            new_data = [asdict(business) for business in self.business_list if (business.name, business.address) not in existing_identifiers]

            if new_data:
                existing_data.extend(new_data)
                with open(f"{filename}.json", "w") as f:
                    json.dump(existing_data, f, indent=4)

def read_keywords(filename: str) -> List[str]:
    with open(filename, 'r') as f:
        return [line.strip() for line in f.readlines() if line.strip()]

def read_zip_codes(filename: str) -> List[str]:
    df = pd.read_csv(filename, dtype={'zip': str})
    return df['zip'].dropna().tolist()

async def random_delay(min_seconds: float = RANDOM_DELAY_MIN, max_seconds: float = RANDOM_DELAY_MAX) -> None:
    delay = random.uniform(min_seconds, max_seconds)
    logger.debug(f"Random delay: {delay:.2f} seconds")
    await asyncio.sleep(delay)

async def clear_search_field(page: Page) -> None:
    """Clears the search field on the Google Maps page."""
    search_box = page.locator(SEARCH_BOX_XPATH)
    await search_box.click()
    await page.keyboard.press("Control+A")
    await page.keyboard.press("Backspace")

async def extract_business_details(page: Page) -> Business:
    """Extracts business details from the page with improved error handling."""
    business = Business()

    async def safe_extract_text(xpath: str) -> Optional[str]:
        locator = page.locator(xpath)
        try:
            if await locator.count() > 0:
                text = await locator.inner_text(timeout=DETAILS_TIMEOUT)
                return text.strip()
        except Exception as e:
            logger.warning(f"Failed to extract text from {xpath}: {e}")
        return None

    business.name = await safe_extract_text(NAME_XPATH)
    business.address = await safe_extract_text(ADDRESS_XPATH)
    business.website = await safe_extract_text(WEBSITE_XPATH)
    business.phone_number = await safe_extract_text(PHONE_XPATH)

    # Extract reviews
    try:
        reviews_locator = page.locator(REVIEWS_XPATH)
        if await reviews_locator.count() > 0:
            aria_label = await reviews_locator.get_attribute("aria-label", timeout=DETAILS_TIMEOUT)
            if aria_label:
                reviews_split = aria_label.split()
                business.reviews_average = float(reviews_split[0].replace(",", ".").strip())
                business.reviews_count = int(reviews_split[2].strip())
    except Exception as e:
        logger.warning(f"Failed to get reviews: {e}")

    # Attempt to extract lat/long from URL (simple method)
    try:
        current_url = page.url
        if '@' in current_url:
            coords_part = current_url.split('@')[1].split(',')[0:2]
            business.latitude = float(coords_part[0])
            business.longitude = float(coords_part[1])
    except Exception as e:
        logger.debug(f"Failed to extract lat/long: {e}")

    return business

def read_processed_combinations(filename: str) -> List[Tuple[str, str]]:
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
            return [tuple(item) for item in data if isinstance(item, list) and len(item) == 2]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def log_processed_combination(filename: str, combination: Tuple[str, str]) -> None:
    lock = FileLock(LOCK_FILE)
    with lock:
        processed_combinations = read_processed_combinations(filename)
        if combination not in processed_combinations:
            processed_combinations.append(combination)
            with open(filename, 'w') as f:
                json.dump(processed_combinations, f, indent=4)
            logger.info(f"Logged processed combination: {combination}")

async def process_combination(keyword: str, zip_code: str, page: Page, total: int, max_retries: int) -> None:
    try:
        business_list = BusinessList()
        await clear_search_field(page)
        await random_delay()

        search_for = f"{keyword} in {zip_code}"
        logger.info(f"Searching for: {search_for}")
        search_box = page.locator(SEARCH_BOX_XPATH)
        await search_box.fill(search_for)  # Use fill for faster input
        await page.keyboard.press("Enter")
        await random_delay(5, 11)
        await page.wait_for_timeout(PAGE_WAIT_TIMEOUT)

        retries = 0
        while retries < max_retries:
            try:
                await page.wait_for_selector(LISTING_XPATH, timeout=PAGE_WAIT_TIMEOUT)
                break
            except PlaywrightTimeoutError:
                retries += 1
                logger.warning(f"Timeout waiting for listings. Retry {retries}/{max_retries} for {keyword} in {zip_code}.")
                if retries == max_retries:
                    logger.error(f"Max retries reached for {keyword} in {zip_code}. Skipping.")
                    return

        previously_counted = 0
        while True:
            for _ in range(SCROLL_COUNT):
                scroll_value = random.randint(SCROLL_VALUE_MIN, SCROLL_VALUE_MAX)
                await page.mouse.wheel(0, scroll_value)
                await random_delay(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX)
            await page.wait_for_timeout(PAGE_WAIT_TIMEOUT)

            current_count = await page.locator(LISTING_XPATH).count()
            logger.debug(f"Current listings count: {current_count}")
            if current_count >= total:
                listings = await page.locator(LISTING_XPATH).all()[:total]
                logger.info(f"Reached target. Total listings to process: {len(listings)}")
                break
            elif current_count == previously_counted:
                listings = await page.locator(LISTING_XPATH).all()
                logger.info(f"No more listings. Total scraped: {len(listings)}")
                break
            else:
                previously_counted = current_count

        for idx, listing in enumerate(listings):
            try:
                await listing.scroll_into_view_if_needed()
                await listing.wait_for(state='visible', timeout=DETAILS_TIMEOUT)
                await listing.click()
                await page.wait_for_timeout(PAGE_WAIT_TIMEOUT)
                business = await extract_business_details(page)
                if business.name:  # Skip if no name (invalid)
                    business_list.business_list.append(business)
                    logger.info(f"Extracted business {idx+1}: {business.name}")
            except PlaywrightTimeoutError:
                logger.warning("Timeout processing listing. Skipping.")
            except Exception as e:
                logger.error(f"Error processing listing {idx+1}: {e}")

        if business_list.business_list:
            output_file = f"google_maps_data_{keyword.replace(' ', '_')}"
            business_list.save_to_json(output_file)
            business_list.save_to_csv(output_file)  # Added: Save to CSV by default
            logger.info(f"Saved data for {keyword} in {zip_code} to {output_file}")
        else:
            logger.warning(f"No businesses found for {keyword} in {zip_code}")

        log_processed_combination(PROCESSED_LOG_FILE, (keyword, zip_code))

    except Exception as e:
        logger.error(f"Unexpected error processing {keyword} in {zip_code}: {e}")

async def main(args):
    keywords = read_keywords(args.keywords_file)
    zip_codes = read_zip_codes(args.zip_file)
    if not keywords or not zip_codes:
        logger.error("No keywords or zip codes found. Exiting.")
        return

    combinations = list(product(keywords, zip_codes))
    random.shuffle(combinations)

    processed_combinations = read_processed_combinations(PROCESSED_LOG_FILE)
    combinations_to_process = [comb for comb in combinations if comb not in processed_combinations]
    logger.info(f"Total combinations to process: {len(combinations_to_process)}")

    if not combinations_to_process:
        logger.info("All combinations already processed.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=args.headless, args=['--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'])  # Updated UA
        tasks = []

        for i in range(args.concurrent):
            context = await browser.new_context(viewport={'width': 1920, 'height': 1080})
            page = await context.new_page()
            await page.goto("https://www.google.com/maps", timeout=PAGE_LOAD_TIMEOUT)
            await page.wait_for_timeout(PAGE_WAIT_TIMEOUT)
            tasks.append(page)

        async def worker(page: Page):
            while combinations_to_process:
                keyword, zip_code = combinations_to_process.pop()
                logger.info(f"Processing: {keyword} in {zip_code}")
                await process_combination(keyword, zip_code, page, args.total, args.max_retries)
                await random_delay(10, 20)  # Longer delay between searches to avoid detection

        await asyncio.gather(*[worker(page) for page in tasks])

        await browser.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Google Maps Business Scraper")
    parser.add_argument('--keywords_file', default='keywords.txt', help='File containing keywords')
    parser.add_argument('--zip_file', default='all_zip.csv', help='CSV file containing zip codes')
    parser.add_argument('--total', type=int, default=100, help='Maximum businesses to scrape per search')
    parser.add_argument('--max_retries', type=int, default=3, help='Max retries for loading listings')
    parser.add_argument('--concurrent', type=int, default=4, help='Number of concurrent browser pages (reduced for stability)')
    parser.add_argument('--headless', action='store_true', default=False, help='Run in headless mode')
    args = parser.parse_args()

    asyncio.run(main(args))