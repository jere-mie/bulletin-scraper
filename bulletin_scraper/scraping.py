from __future__ import annotations

import logging
import time
from urllib.parse import urljoin

import cloudscraper
import requests
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)

PREFERRED_DOMAINS = ["parishbulletins.com", "files.ecatholic.com"]
MAX_RETRIES = 10
RETRY_DELAYS = [1, 2, 4, 8, 16, 16, 16, 16, 16, 16]


def scrape_bulletin_with_retry(church_name: str, bulletin_website: str) -> str | None:
    for attempt in range(MAX_RETRIES):
        try:
            pdf_link = scrape_bulletin(bulletin_website)
            if pdf_link:
                return pdf_link
            if attempt == MAX_RETRIES - 1:
                return None
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                logger.warning(
                    "%s (attempt %s/%s): %s... Retrying in %ss",
                    church_name,
                    attempt + 1,
                    MAX_RETRIES,
                    str(exc)[:50],
                    delay,
                )
                time.sleep(delay)
            else:
                logger.error("%s: Failed after %s attempts", church_name, MAX_RETRIES)
                return None
    return None


def scrape_bulletin(bulletin_website: str) -> str | None:
    scraper = cloudscraper.create_scraper()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    response = scraper.get(bulletin_website, headers=headers, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    preferred_pdfs: list[str] = []
    other_pdfs: list[str] = []
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if ".pdf" not in href.lower():
            continue
        absolute_url = urljoin(bulletin_website, href)
        if any(domain in absolute_url for domain in PREFERRED_DOMAINS):
            preferred_pdfs.append(absolute_url)
        else:
            other_pdfs.append(absolute_url)
    all_pdfs = preferred_pdfs + other_pdfs
    return all_pdfs[0] if all_pdfs else None


def download_pdf(pdf_url: str, output_path: str) -> bool:
    try:
        logger.debug("Downloading %s...", pdf_url[:60])
        response = requests.get(pdf_url, timeout=20)
        response.raise_for_status()
        with open(output_path, "wb") as handle:
            handle.write(response.content)
        logger.debug("Saved to %s", output_path)
        return True
    except Exception as exc:
        logger.error("Failed to download %s...: %s", pdf_url[:60], str(exc)[:50])
        return False