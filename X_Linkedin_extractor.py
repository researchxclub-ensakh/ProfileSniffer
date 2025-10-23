import json
import re


def extract_social_links(input_path: str):
    """Extract Twitter and LinkedIn links for each user (with Twitter priority logic)."""
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("extracted_table", data)

    twitter_links = []
    linkedin_links = []

    for user in records:
        username = user.get("GitHub Username") or user.get("Name")
        readme = user.get("Readme file") or ""
        twitter_field = user.get("Twitter Username")

        twitter_url = None

        # --- 1️⃣ First priority: Twitter Username field ---
        if isinstance(twitter_field, str) and twitter_field.strip() and twitter_field.strip().lower() != "no twitter username":
            # Clean up common bad formats (like '@username')
            twitter_handle = twitter_field.strip().lstrip("@")
            twitter_url = f"https://twitter.com/{twitter_handle}"

        # --- 2️⃣ Fallback: Extract from README ---
        elif isinstance(readme, str):
            matches = re.findall(r"https?://(?:www\.)?twitter\.com/[A-Za-z0-9_]+", readme)
            if matches:
                twitter_url = matches[0]  # take the first one found

        # --- LinkedIn always from README ---
        linkedin_matches = re.findall(r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/[A-Za-z0-9\-_]+", readme)

        # --- Append results ---
        if twitter_url:
            twitter_links.append({
                "GitHub Username": username,
                "Link": twitter_url
            })

        if linkedin_matches:
            for link in set(linkedin_matches):
                linkedin_links.append({
                    "GitHub Username": username,
                    "Link": link
                })

    # --- Save results ---
    with open("twitter.json", "w", encoding="utf-8") as f:
        json.dump(twitter_links, f, indent=2, ensure_ascii=False)

    with open("linkedin.json", "w", encoding="utf-8") as f:
        json.dump(linkedin_links, f, indent=2, ensure_ascii=False)

    print(f"✅ Extracted {len(twitter_links)} Twitter links → twitter.json")
    print(f"✅ Extracted {len(linkedin_links)} LinkedIn links → linkedin.json")


if __name__ == "__main__":
    extract_social_links("unified_github_users_with_sources.json")
