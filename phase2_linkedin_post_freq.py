"""
Phase 2: Scrape LinkedIn profiles via Apify to get original post frequency.
- Actor: datadoping/linkedin-profile-posts-scraper (RE0MriXnFhR3IgVnJ) — 17K runs, 5★
- Targets all coaches in coaches_all_us.csv with a LinkedIn URL
- Counts ORIGINAL posts only (reshared_post == None) in the last 90 days
- Adds columns: LinkedIn Posts (90d), LinkedIn Last Post Date, LinkedIn Post Freq/Month
- Writes results back atomically (safe to interrupt & resume)

Run: python3 phase2_linkedin_post_freq.py
"""

import csv, json, time, re, shutil
from datetime import datetime, timedelta, timezone
import requests

APIFY_TOKEN   = os.environ.get('APIFY_TOKEN', '')  # set via: export APIFY_TOKEN=your_key
ACTOR_ID      = 'RE0MriXnFhR3IgVnJ'   # datadoping/linkedin-profile-posts-scraper
CSV_FILE      = 'coaches_all_us.csv'
BATCH_SIZE    = 50     # LinkedIn profiles per Apify run
POLL_INTERVAL = 15     # seconds between status checks
LOOKBACK_DAYS = 90     # count posts within this window
MAX_POSTS     = 50     # max posts to fetch per profile (actor 'count' param)

NEW_COLS = [
    "LinkedIn Posts (90d)",
    "LinkedIn Last Post Date",
    "LinkedIn Post Freq/Month"
]


def normalise_linkedin(url: str) -> str:
    """Normalise to https://www.linkedin.com/in/handle"""
    url = url.strip().rstrip("/")
    m = re.search(r'linkedin\.com/in/([\w\-]+)', url, re.IGNORECASE)
    return f"https://www.linkedin.com/in/{m.group(1)}" if m else url


def run_apify_batch(linkedin_urls: list) -> dict:
    """Submit LinkedIn URLs to Apify, wait for completion, return {profile_url: stats}."""
    payload = {
        "profiles": linkedin_urls,
        "count": MAX_POSTS,
    }

    r = requests.post(
        f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs",
        params={"token": APIFY_TOKEN},
        json=payload,
        timeout=30
    )
    r.raise_for_status()
    run_data = r.json()["data"]
    run_id   = run_data["id"]
    print(f"  Run started: {run_id}")

    # Poll until finished
    while True:
        time.sleep(POLL_INTERVAL)
        status_r = requests.get(
            f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs/{run_id}",
            params={"token": APIFY_TOKEN},
            timeout=15
        )
        status_r.raise_for_status()
        run_data = status_r.json()["data"]
        status   = run_data["status"]
        print(f"  Status: {status}")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break

    if status != "SUCCEEDED":
        print(f"  WARNING: Run {run_id} ended with status {status}")
        return {}

    # Fetch dataset items
    dataset_id = run_data["defaultDatasetId"]
    items_r = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": APIFY_TOKEN, "format": "json"},
        timeout=30
    )
    items_r.raise_for_status()
    items = items_r.json()

    # Aggregate per profile — original posts only
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    profile_stats = {}   # normalised_url → {"count": int, "latest": datetime|None}

    for item in items:
        # Map post back to the submitted profile URL
        profile_url = normalise_linkedin(item.get("input", ""))
        if not profile_url:
            continue

        # Original post = reshared_post is None
        if item.get("reshared_post") is not None:
            continue

        # Parse post date from posted_at.timestamp (milliseconds)
        posted_at = item.get("posted_at", {})
        ts_ms = posted_at.get("timestamp")
        if ts_ms:
            post_date = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        else:
            # Fallback: parse posted_at.date string
            date_str = posted_at.get("date", "")
            try:
                post_date = datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                post_date = None

        if not post_date or post_date < cutoff:
            continue

        if profile_url not in profile_stats:
            profile_stats[profile_url] = {"count": 0, "latest": None}

        profile_stats[profile_url]["count"] += 1
        if not profile_stats[profile_url]["latest"] or post_date > profile_stats[profile_url]["latest"]:
            profile_stats[profile_url]["latest"] = post_date

    # Format for CSV
    result = {}
    for url, stats in profile_stats.items():
        count  = stats["count"]
        latest = stats["latest"]
        freq   = round(count / (LOOKBACK_DAYS / 30), 2)
        result[url] = {
            "LinkedIn Posts (90d)":      str(count),
            "LinkedIn Last Post Date":   latest.strftime("%Y-%m-%d") if latest else "",
            "LinkedIn Post Freq/Month":  str(freq),
        }
    return result


def main():
    # Load CSV
    rows = []
    fieldnames = []
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        for row in reader:
            rows.append(row)

    # Add new columns if missing
    for col in NEW_COLS:
        if col not in fieldnames:
            fieldnames.append(col)
            for row in rows:
                row[col] = ""

    # Targets: has LinkedIn URL, not yet enriched
    targets = [
        (i, normalise_linkedin(r["LinkedIn"]))
        for i, r in enumerate(rows)
        if r.get("LinkedIn", "").strip()
        and not r.get("LinkedIn Posts (90d)", "").strip()
    ]
    print(f"Coaches to enrich: {len(targets)}")

    enriched_total = 0
    for batch_start in range(0, len(targets), BATCH_SIZE):
        batch = targets[batch_start: batch_start + BATCH_SIZE]
        urls  = [url for _, url in batch]
        print(f"\nBatch {batch_start // BATCH_SIZE + 1}/{-(-len(targets)//BATCH_SIZE)} — {len(urls)} profiles")

        stats_map = run_apify_batch(urls)
        enriched_in_batch = 0

        for idx, url in batch:
            stats = stats_map.get(url)
            if stats:
                for col, val in stats.items():
                    rows[idx][col] = val
                enriched_in_batch += 1
                enriched_total += 1
            else:
                # No original posts in window — mark 0 so we don't retry
                rows[idx]["LinkedIn Posts (90d)"]     = "0"
                rows[idx]["LinkedIn Last Post Date"]  = ""
                rows[idx]["LinkedIn Post Freq/Month"] = "0"

        print(f"  Profiles with original posts: {enriched_in_batch}")

        # Atomic write after every batch
        tmp = CSV_FILE + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        shutil.move(tmp, CSV_FILE)
        print(f"  CSV saved. Total enriched so far: {enriched_total}")

    print(f"\nDone. Total profiles enriched: {enriched_total}")


if __name__ == "__main__":
    main()
