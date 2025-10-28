# Amazon Scraped Data Analysis v1

This project contains **scraped data from Amazon Best Sellers & Movers & Shakers**, along with analysis scripts and insights.  
The goal is to explore product performance, price segmentation, review density impact, and category momentum.

---

## Project Overview

- **Data Source:** Amazon Best Sellers & Movers & Shakers pages
- **Tech Stack:** Python, Selenium, Pandas, Matplotlib
- **Scripts:** Automate data collection and cleaning
- **Analysis:** Explore pricing strategy, review impact, and fast movers

---

## Folder Structure

amazon-scraped-data-analysis-v1/
├─ data/
│ ├─ clean_best_sellers_all_category_YYYYMMDD.csv
│ ├─ clean_movers_shakers_all_category_YYYYMMDD.csv
│ ├─ scrape_best_sellers_all_category_YYYYMMDD.csv
│ └─ scrape_movers_shakers_all_category_YYYYMMDD.csv
├─ outputs/ # outputs from analysis_best_sellers.ipynb
├─ scripts/
│ ├─ scrape_best_sellers_all_category.py
│ ├─ scrape_movers_shakers_all_category.py
│ ├─ transform_best_sellers.py
│ ├─ transform_movers_shakers.py
│ └─ analysis_best_sellers.ipynb
├─ reports/Report Amazon Best Sellers & Movers and Shakers.pptx
└─ README.md

---

## Usage

### 1. Scraping Data

> ⚠️ For ethical scraping and GitHub safety, the script **does not include exact XPaths** for Amazon elements. You must locate them yourself by inspecting the Amazon page.

Example:

```python
from scripts.scrape_best_sellers_all_category import setup_driver, scrape_amazon_best_sellers

driver = setup_driver()
url = "your-amazon-category-url"
results = scrape_amazon_best_sellers(driver, url, page_number=1)
driver.quit()
```

### 2. Data Analysis

After scraping and cleaning, the CSV files can be analyzed using Python or notebooks.

Example analyses included:

- Price Segmentation: Low / Mid / High per category
- Review Density vs Rank: Identify how social proof affects best-seller rank
- Category Momentum: Track fast movers and sudden spikes
- Insights & Recommendations: Pricing strategy, launch planning, and review campaigns

---

## Strategic Recommendations (Summary)

- Price Smartly: Mid-price band generally works; use discounts for visibility.
- Review Strategy: Start campaigns immediately; maintain steady review flow.
- Momentum Tracking: Flag categories with sudden rank jumps for insights.
- Launch Planning: Early engagement bursts (CTR, purchases, reviews) matter more than total reviews.

---

## License

This project is for learning and analysis purposes. Please follow ethical scraping practices.
Do not share scraped data publicly if it violates Amazon's Terms of Service.

---

## Contact

If you need data scraping or analysis support, feel free to reach out.
Email: nikoberwibowo@gmail.com
Let’s connect and turn insights into action! Thanks!
