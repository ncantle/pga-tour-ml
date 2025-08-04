import pandas as pd
from datetime import datetime
from config.db_config import get_engine
from src.utils.selenium_helper import get_chrome_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

STAT_CATEGORIES = {
    "sg_total": "02674",
    "sg_t2g": "02567",
    "sg_app": "02564",
    "sg_arg": "02566",
    "sg_putt": "02569",
}

# BASE_URL = "https://www.pgatour.com/stats/stat.{}.html"
BASE_URL = "https://www.pgatour.com/stats/detail/{}"

def scrape_stat_column(driver, url: str, stat_name: str):
    driver.get(url)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
    except Exception as e:
        print(f"❌ Timeout or error waiting for table at {url}: {e}")
        return pd.DataFrame()

    try:
        table = driver.find_element(By.TAG_NAME, "table")
        rows = table.find_elements(By.TAG_NAME, "tr")

        data = []
        for row in rows[1:]:  # skip header row
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) >= 5:  # based on PGA layout: Rank | Player | Events | Rounds | AVG
                name = cells[1].text.strip()
                avg = cells[4].text.strip()
                if name and avg:
                    try:
                        data.append({"name": name, stat_name: float(avg)})
                    except ValueError:
                        continue  # skip non-numeric averages

        return pd.DataFrame(data)
    except Exception as e:
        print(f"❌ Failed to parse table at {url}: {e}")
        return pd.DataFrame()

def ingest_player_data(table_name="players"):
    driver = get_chrome_driver()
    dfs = []

    for stat_name, stat_code in STAT_CATEGORIES.items():
        url = BASE_URL.format(stat_code)
        print(f"🔍 Scraping {stat_name} from {url}")
        df = scrape_stat_column(driver, url, stat_name)
        if not df.empty:
            dfs.append(df)
        else:
            print(f"⚠️ No data found for {stat_name}")

    driver.quit()

    if not dfs:
        print("❌ No data was ingested from any stat pages.")
        return None

    df_merged = dfs[0]
    for df in dfs[1:]:
        df_merged = pd.merge(df_merged, df, on="name", how="outer")

    df_merged["player_id"] = df_merged["name"].apply(lambda x: abs(hash(x)) % (10**8))

    engine = get_engine()
    with engine.begin() as conn:
        df_merged.to_sql(table_name, con=conn, schema="raw", if_exists="replace", index=False)

    print(f"[{datetime.now()}] ✅ Ingested {len(df_merged)} players into raw.{table_name}")
    return df_merged

if __name__ == "__main__":
    ingest_player_data()
    print("Player data ingestion completed successfully.")