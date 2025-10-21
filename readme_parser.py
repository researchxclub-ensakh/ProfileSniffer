import pandas as pd
import requests
import json
import time
from typing import Optional


def fetch_github_readme(username: str, delay: float = 2.0, retries: int = 3) -> dict:
    """
    Fetch README.md from a user's GitHub profile repo.
    Retries on connection failures, with a delay between attempts.
    """
    url = f"https://raw.githubusercontent.com/{username}/{username}/main/README.md"
    for attempt in range(1, retries + 1):
        try:
            time.sleep(delay)
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                print(f"✅ {username} - README fetched successfully")
                return {"Readme file": response.text, "Readme Error": None}
            elif response.status_code == 404:
                print(f"🚫 {username} - No README found (404)")
                return {"Readme file": None, "Readme Error": "No README found (404)"}
            else:
                print(f"⚠️ {username} - HTTP {response.status_code}")
        except requests.RequestException as e:
            print(f"❌ {username} - Connection failed (attempt {attempt}/{retries}): {e}")
            last_error = str(e)
    return {"Readme file": None, "Readme Error": last_error or "Unknown error"}


def enrich_table_with_readmes(df: pd.DataFrame, name_col: str = "GitHub Username", limit: Optional[int] = None, delay: float = 2.0) -> pd.DataFrame:
    """
    For each GitHub user, fetch README with delay between requests and attach results to the DataFrame.
    """
    if limit:
        df = df.head(limit)

    readmes, errors = [], []

    for username in df[name_col]:
        if isinstance(username, str) and username.strip():
            result = fetch_github_readme(username.strip(), delay=delay)
            readmes.append(result["Readme file"])
            errors.append(result["Readme Error"])
        else:
            readmes.append(None)
            errors.append("Invalid or missing username")

    df["Readme file"] = readmes
    df["Readme Error"] = errors
    return df


# Example usage
if __name__ == "__main__":
    # Load your new clean JSON
    with open("extracted_users_preserve.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data["extracted_table"])

    # ⚙️ Ensure we have a valid GitHub username column
    df["GitHub Username"] = df["Name"].apply(
        lambda x: str(x).split()[0].strip().replace(" ", "") if isinstance(x, str) else None
    )

    # Scrape all users (or set limit=10 for testing)
    enriched_df = enrich_table_with_readmes(df, name_col="GitHub Username", limit=None, delay=2.0)

    # Save final output
    enriched_json = enriched_df.to_dict(orient="records")
    final_output = {"extracted_table": enriched_json}

    with open("extracted_last_table_followerst.json", "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)

    print("✅ Completed! Output saved to extracted_last_table_followerst.json")
