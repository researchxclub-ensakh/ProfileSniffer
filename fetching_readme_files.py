import pandas as pd
import requests
import json
import time
from bs4 import BeautifulSoup
from typing import Optional

def extract_github_usernames(df: pd.DataFrame, column: str = "Name") -> list:
    """
    Extract GitHub usernames from the <a href="https://github.com/<username>"> tag.
    """
    usernames = []
    for cell in df[column]:
        soup = BeautifulSoup(cell, "html.parser")
        link = soup.find("a", href=True)
        if link and "github.com" in link["href"]:
            username = link["href"].rstrip("/").split("/")[-1]
            usernames.append(username)
        else:
            usernames.append(None)
    return usernames


def fetch_github_readme(username: str, delay: float = 5.0) -> dict:
    """
    Fetch README.md from user's GitHub profile repo.
    Wait `delay` seconds after each request to respect rate limits.
    """
    url = f"https://raw.githubusercontent.com/{username}/{username}/main/README.md"
    time.sleep(delay)  # delay before each request
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            print(f" {username} - README fetched successfully")
            return {"Readme file": response.text, "Readme Error": None}
        else:
            print(f"Ooups hehe {username} - README not found (HTTP {response.status_code})")
            return {"Readme file": None, "Readme Error": f"HTTP {response.status_code}: Not found"}
    except requests.RequestException as e:
        print(f" {username} - Request failed: {e}")
        return {"Readme file": None, "Readme Error": str(e)}


def enrich_table_with_readmes(df: pd.DataFrame, name_col: str = "Name", limit: Optional[int] = None, delay: float = 5.0) -> pd.DataFrame:
    """
    For each GitHub user, fetch README with delay between requests and attach results to the DataFrame.
    """
    usernames = extract_github_usernames(df, name_col)
    df["GitHub Username"] = usernames

    if limit:
        df = df.head(limit)

    readmes, errors = [], []

    for username in df["GitHub Username"]:
        if username:
            result = fetch_github_readme(username, delay=delay)
            readmes.append(result["Readme file"])
            errors.append(result["Readme Error"])
        else:
            readmes.append(None)
            errors.append("No valid GitHub username found")

    df["Readme file"] = readmes
    df["Readme Error"] = errors
    return df.drop(columns=["GitHub Username"])


# Example usage
if __name__ == "__main__":
    # Load your extracted JSON
    with open("morocco_extracted_table.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data["extracted_table"])

    # Scrape ALL users (or limit if testing)
    enriched_df = enrich_table_with_readmes(df, limit=None, delay=5.0)

    # Save final output
    enriched_json = enriched_df.to_dict(orient="records")
    final_output = {"extracted_table": enriched_json}

    with open("morocco_with_readmes_slow.json", "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)

    print("✅ Completed! Output saved to 'morocco_with_readmes_slow.json'")
