import os
import json
from apify_client import ApifyClient
from dotenv import load_dotenv


class NoTokenAvailable(Exception):
    """Raised when no token with remaining uses is available."""
    pass


class LinkedinApiWrapper:
    def __init__(self, tokens: dict[str, int], actor_id: str):
        self.tokens = tokens
        self.api_token = None
        self.actor_id = actor_id
        self.client = None

    # --- Internal helpers ---
    def _fetch_token(self) -> str | None:
        """Return a token with remaining uses > 0, otherwise None."""
        for token, remaining in self.tokens.items():
            if remaining > 0:
                return token
        return None

    def _update_token_usage(self, token: str):
        """Decrement token use count or remove it if it reaches 0."""
        if token not in self.tokens:
            return
        if self.tokens[token] > 1:
            self.tokens[token] -= 1
        else:
            del self.tokens[token]

    def enrich_profiles(self, urls, max_profiles):
        """Send URLs to Apify actor and fetch enriched results."""

        # ✅ Fetch a valid token
        token = self._fetch_token()
        if not token:
            raise NoTokenAvailable("No valid SerpAPI token available.")
        self.api_token = token

        limited_urls = urls[:max_profiles]

        print(f"🔍 Enriching {len(limited_urls)} LinkedIn profiles...")

        run_input = {"profileUrls": limited_urls}

        client = ApifyClient(self.api_token)

        # Run the actor
        run = client.actor(self.actor_id).call(run_input=run_input)

        # ✅ Update or remove token after use
        self._update_token_usage(self.api_token)

        # Fetch enriched data
        enriched_data = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            enriched_data.append(item)

        print(f"✅ Retrieved {len(enriched_data)} enriched profiles.")
        return enriched_data

    # --- Save results ---
    @classmethod
    def save_results(cls, results: list[dict], output_filename: str):
        existing = []
        if os.path.exists(output_filename):
            with open(output_filename, "r", encoding="utf-8") as f:
                existing = json.load(f)
        combined = existing + results
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(combined, f, ensure_ascii=False, indent=4)
        print(f"💾 Results appended to {output_filename}")


def load_linkedin_urls_from_json(json_file):
    """Load LinkedIn profile URLs from the 'link' field in a JSON file."""
    urls = []
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("❌ The JSON file must contain a list of objects.")

    for item in data:
        link = item.get("link", "").strip()
        if link and "linkedin.com/in/" in link:
            urls.append(link)

    return urls


if __name__ == "__main__":
    load_dotenv()

    tokens_json = os.getenv("APIFY_TOKENS", "{}")
    apify_tokens = json.loads(tokens_json)
    ACTOR_ID = os.getenv("APIFY_ACTOR_ID")

    JSON_FILE = "input.json"  # Input JSON file
    OUTPUT_FILE = "linkedin_profiles.json"

    scrap_engine = LinkedinApiWrapper(apify_tokens, ACTOR_ID)

    urls = load_linkedin_urls_from_json(JSON_FILE)
    results = scrap_engine.enrich_profiles(urls, max_profiles=100)

    scrap_engine.save_results(results, OUTPUT_FILE)
    print(results)
