# NEPSE EOD Scraper

This repository contains a Python scraper designed to fetch End-of-Day (EOD) stock data for the Nepal Stock Exchange (NEPSE) from nepsealpha.com. The scraped data is stored locally in both CSV and DuckDB formats, which are then committed to the repository.

## Project Structure
.
├── .github/
│   └── workflows/
│       └── daily-scrape.yml  # GitHub Actions workflow for scheduling
├── data/
│   └── eod/
│       ├── nepse.csv         # Stores the main CSV data
│       └── nepse.duckdb      # Stores the main DuckDB database
├── scripts/
│   ├── scraper.py            # The core scraping and data processing logic
│   └── company.json          # Your company symbols configuration
├── .gitignore                # Tells Git what files to ignore
├── README.md                 # Project description and instructions
└── requirements.txt          # Python dependencies

## Setup

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/your-username/nepse-eod-scraper.git](https://github.com/your-username/nepse-eod-scraper.git)
    cd nepse-eod-scraper
    ```
2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: `venv\Scripts\activate`
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    playwright install chromium
    ```
4.  **Populate `scripts/company.json`:**
    Ensure this file contains the symbols of active equity companies you wish to track.

## Running the Scraper

You can run the scraper manually for testing:

```bash
python scripts/scraper.py