# scripts/scrape_best_sellers_all_category_safe.py
"""
Amazon Best Sellers Scraper (Selenium) - Template
-------------------------------------------------
You must provide your own selectors / paths.

Author: niko prabowo
Date: 2025-10-25
"""

import time
import json
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# --- Browser Setup ---
def setup_driver():
    """Setup Chrome WebDriver without automation flags."""
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-geolocation")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option(
        "prefs", {"profile.default_content_setting_values.geolocation": 2}
    )
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/141.0.7390.123 Safari/537.36"
    )
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    return driver


# --- Price Conversion ---
IDR_TO_USD = 0.000060  # Example conversion


def convert_price(price_text: str) -> str:
    """Convert price from IDR to USD if needed."""
    if not price_text:
        return price_text

    clean = price_text.strip().replace(",", "").replace(" ", "")
    if clean.startswith("$"):
        return clean

    if clean.lower().startswith("idr") or "rp" in clean.lower():
        nums = re.findall(r"[\d.]+", clean)
        if not nums:
            return price_text
        try:
            amount = float(nums[0])
            usd = amount * IDR_TO_USD if amount > 10000 else amount
            return f"${usd:.2f} (converted)"
        except Exception:
            return price_text
    return price_text


# --- Scraping Logic ---
def scrape_amazon_best_sellers(
    driver, url: str, page_number: int, global_offset: int = 0
):
    """Scrape Amazon Best Sellers page.
    NOTE: User must provide their own XPaths / CSS selectors for each element.
    """

    print(f"\n Opening {url} ...")
    driver.get(url)
    wait = WebDriverWait(driver, 15)

    # --- TODO: Replace this with the actual list container in your page ---
    ol_element = wait.until(
        EC.presence_of_element_located((By.XPATH, "YOUR_OL_XPATH_HERE"))
    )

    # --- TODO: Replace this with the actual category element ---
    try:
        category_elem = driver.find_element(By.XPATH, "YOUR_CATEGORY_XPATH_HERE")
        category = category_elem.text.strip()
    except:
        category = None

    # Scroll to load items
    for i in range(50):
        driver.execute_script(f"window.scrollTo(0, {i*300});")
        time.sleep(0.5)

    # --- TODO: Replace with actual product container XPath ---
    li_elements = ol_element.find_elements(By.XPATH, ".//li[YOUR_ITEM_XPATH_HERE]")
    print(f"🛒 Products detected: {len(li_elements)}")

    data = []
    for idx, li in enumerate(li_elements, start=1):
        try:
            # --- TODO: Replace with actual product card/container XPath ---
            container = li.find_element(By.XPATH, "YOUR_CONTAINER_XPATH_HERE")
        except:
            continue

        # --- TODO: Replace with XPath for title ---
        try:
            title = container.find_element(By.XPATH, "YOUR_TITLE_XPATH_HERE").text
        except:
            title = None

        # --- TODO: Replace with XPath for link ---
        try:
            href = container.find_element(
                By.XPATH, "YOUR_LINK_XPATH_HERE"
            ).get_attribute("href")
        except:
            href = None

        # --- TODO: Replace with XPath for rating and review_count ---
        try:
            rating_elem = container.find_element(By.XPATH, "YOUR_RATING_XPATH_HERE")
            rating = rating_elem.get_attribute("aria-label").split(",")[0].strip()
            review_count = rating_elem.find_element(
                By.XPATH, "YOUR_REVIEW_COUNT_XPATH_HERE"
            ).text
        except:
            rating, review_count = None, None

        # --- TODO: Replace with XPath for price ---
        try:
            price_elem = container.find_element(By.XPATH, "YOUR_PRICE_XPATH_HERE")
            price_attr = price_elem.get_attribute("data-a-price")
            if price_attr:
                price = f"${json.loads(price_attr).get('amount')}"
            else:
                price = price_elem.text
        except:
            price = None

        price = convert_price(price)
        rank = global_offset + idx

        data.append(
            {
                "rank": rank,
                "page": page_number,
                "category": category,
                "title": title,
                "link": href,
                "rating": rating,
                "review_count": review_count,
                "price": price,
            }
        )
        print(f"#{rank:03d} | {title[:50] if title else 'No title'} | {price}")

    return data


# --- Main Execution ---
if __name__ == "__main__":
    # --- TODO: Provide your own CSV of category URLs ---
    input_csv = r"data/transf_url_best_seller_all_category_20251025.csv"
    df_urls = pd.read_csv(input_csv)
    category_urls = df_urls["transformed_url"].dropna().tolist()

    driver = setup_driver()
    all_results = []

    try:
        for cat_idx, base_url in enumerate(category_urls, start=1):
            print(f"\n=== Scraping category {cat_idx}/{len(category_urls)} ===")
            for page in range(1, 3):  # Adjust number of pages
                url = re.sub(r"pg_\d+", f"pg_{page}", base_url)
                url = re.sub(r"pg=\d+", f"pg={page}", url)
                results = scrape_amazon_best_sellers(driver, url, page)
                all_results.extend(results)
                time.sleep(2)
    finally:
        driver.quit()

    # --- Save results ---
    output_path = "data/scrape_best_sellers_all_category_safe.csv"
    pd.DataFrame(all_results).to_csv(output_path, sep=";", index=False)
    print(f"\nAll done. Saved: {output_path}")
