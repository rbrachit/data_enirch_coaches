"""
Phase 1: Scrape coach websites via Apify cheerio-scraper to extract LinkedIn URLs.
- Targets coaches with a website but no LinkedIn URL in coaches_all_us.csv
- Processes in batches of 100 URLs per Apify run
- Writes results back atomically (safe to interrupt & resume)
"""

import csv, json, time, re, os, shutil, tempfile
import requests

APIFY_TOKEN  = os.environ.get('APIFY_TOKEN', '')  # set via: export APIFY_TOKEN=your_key
ACTOR_ID     = 'apify~web-scraper'   # Puppeteer-based — handles JS-rendered social links
CSV_FILE     = 'coaches_all_us.csv'
BATCH_SIZE   = 50    # URLs per Apify run (web-scraper is heavier than cheerio)
POLL_INTERVAL = 15   # seconds between status checks

# pageFunction runs in the BROWSER context — use document directly (not context.page)
PAGE_FUNCTION = r"""
async function pageFunction(context) {
    const html = document.documentElement.outerHTML;
    const found = new Set();

    // Regex scan full rendered HTML (catches footer icons, JS-injected links, JSON-LD etc.)
    const regex = /linkedin\.com\/in\/([\w\-]+)/gi;
    let m;
    while ((m = regex.exec(html)) !== null) {
        found.add('https://www.linkedin.com/in/' + m[1]);
    }

    const results = [...found];
    return {
        sourceUrl: window.location.href,
        linkedinUrl: results.length > 0 ? results[0] : null,
        allLinkedinUrls: results
    };
}
"""

def run_apify_batch(urls: list[str]) -> dict[str, str]:
    """Submit a batch of URLs to Apify, wait for completion, return {url: linkedin_url}."""
    start_urls = [{"url": u} for u in urls]
    payload = {
        "startUrls": start_urls,
        "pageFunction": PAGE_FUNCTION,
        "maxCrawlingDepth": 0,       # stay on the landing page only
        "maxPagesPerCrawl": len(urls),
        "maxConcurrency": 10,
        "ignoreSslErrors": True,
        "waitUntil": ["networkidle2"],
        "pageLoadTimeoutSecs": 30,
    }
    headers = {"Content-Type": "application/json"}
    # Start run
    r = requests.post(
        f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs",
        params={"token": APIFY_TOKEN},
        headers=headers,
        json=payload,
        timeout=30
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
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
        status = status_r.json()["data"]["status"]
        print(f"  Status: {status}")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break

    if status != "SUCCEEDED":
        print(f"  WARNING: Run {run_id} ended with status {status}")
        return {}

    # Fetch dataset items
    dataset_id = status_r.json()["data"]["defaultDatasetId"]
    items_r = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": APIFY_TOKEN, "format": "json"},
        timeout=30
    )
    items_r.raise_for_status()
    items = items_r.json()

    # Build map: website_url → linkedin_url
    result = {}
    for item in items:
        src = item.get("sourceUrl") or item.get("url", "")
        li  = item.get("linkedinUrl")
        if src and li:
            result[src.rstrip("/")] = li
    return result


def normalise(url: str) -> str:
    return url.strip().rstrip("/")


def main():
    # Load CSV
    rows = []
    fieldnames = []
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)

    # Identify targets: has website, no LinkedIn
    targets = [
        (i, normalise(r["Their Website"]))
        for i, r in enumerate(rows)
        if r.get("Their Website", "").strip() and not r.get("LinkedIn", "").strip()
    ]
    print(f"Coaches to enrich: {len(targets)}")

    found_total = 0
    for batch_start in range(0, len(targets), BATCH_SIZE):
        batch = targets[batch_start: batch_start + BATCH_SIZE]
        urls  = [url for _, url in batch]
        print(f"\nBatch {batch_start // BATCH_SIZE + 1} — {len(urls)} URLs")

        linkedin_map = run_apify_batch(urls)
        found_in_batch = 0

        for idx, url in batch:
            li = linkedin_map.get(normalise(url))
            if li:
                rows[idx]["LinkedIn"] = li
                found_in_batch += 1
                found_total += 1

        print(f"  LinkedIn URLs found in batch: {found_in_batch}")

        # Atomic write after every batch
        tmp = CSV_FILE + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        shutil.move(tmp, CSV_FILE)
        print(f"  CSV saved. Total found so far: {found_total}")

    print(f"\nDone. Total LinkedIn URLs added: {found_total}")


if __name__ == "__main__":
    main()
