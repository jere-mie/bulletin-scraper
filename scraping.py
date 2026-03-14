"""
Scraping utilities for fetching bulletin PDF links and downloading PDFs.
"""

import json
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import cloudscraper
import requests
import time
import logging

logger = logging.getLogger(__name__)

PREFERRED_DOMAINS = ['parishbulletins.com', 'files.ecatholic.com']
MAX_RETRIES = 10
RETRY_DELAYS = [1, 2, 4, 8, 16, 16, 16, 16, 16, 16]  # Exponential backoff in seconds


def load_churches_json(churches_path):
    """Load churches data from JSON file"""
    logger.info(f"Loading churches from {churches_path}")
    with open(churches_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_bulletin_links(churches):
    """
    Scrape all bulletin websites and extract PDF links.
    Returns a dict mapping bulletin_website -> pdf_link
    """
    logger.info(f"Scraping bulletin links for {len(churches)} churches")
    
    website_cache = {}
    scraped_count = 0
    failed_count = 0
    
    for church in churches:
        church_name = church.get('name', 'Unknown')
        bulletin_website = church.get('bulletin_website', '')
        
        # Skip if no bulletin website
        if not bulletin_website or bulletin_website == 'N/A':
            logger.debug(f"Skipping {church_name}: No bulletin website")
            continue
        
        # Skip if already scraped this website
        if bulletin_website in website_cache:
            logger.debug(f"Using cached result for {bulletin_website}")
            continue
        
        # Try to scrape with retries
        pdf_link = scrape_bulletin_with_retry(church_name, bulletin_website)
        
        if pdf_link:
            website_cache[bulletin_website] = pdf_link
            scraped_count += 1
            logger.info(f"✓ {church_name}: {pdf_link[:60]}...")
        else:
            website_cache[bulletin_website] = None
            failed_count += 1
            logger.warning(f"✗ {church_name}: No PDF found after retries")
        
        # Add delay between requests to reduce 403 errors and rate limiting
        time.sleep(1)
    
    logger.info(f"Scraping complete: {scraped_count} found, {failed_count} failed")
    return website_cache


def scrape_bulletin_with_retry(church_name, bulletin_website):
    """
    Scrape a bulletin website with retry logic.
    Returns PDF link or None if all retries fail.
    """
    for attempt in range(MAX_RETRIES):
        try:
            pdf_link = scrape_bulletin(bulletin_website)
            if pdf_link:
                return pdf_link
            
            # If no PDF found but no error, return None
            if attempt == MAX_RETRIES - 1:
                return None
                
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                logger.warning(
                    f"{church_name} (attempt {attempt + 1}/{MAX_RETRIES}): "
                    f"{str(e)[:50]}... Retrying in {delay}s"
                )
                time.sleep(delay)
            else:
                logger.error(f"{church_name}: Failed after {MAX_RETRIES} attempts")
                return None
    
    return None


def scrape_bulletin(bulletin_website):
    """
    Scrape a single bulletin website and extract PDF link.
    Uses cloudscraper to bypass Cloudflare.
    Returns the preferred PDF link or None.
    """
    # Create scraper with explicit browser headers to mimic legitimate traffic
    scraper = cloudscraper.create_scraper()
    
    # Add custom headers to further reduce 403 errors
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'https://www.google.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    response = scraper.get(bulletin_website, headers=headers, timeout=15)
    response.raise_for_status()
    
    # Parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all PDF links
    preferred_pdfs = []
    other_pdfs = []
    
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        if '.pdf' in href.lower():
            absolute_url = urljoin(bulletin_website, href)
            
            # Categorize by domain
            is_preferred = any(domain in absolute_url for domain in PREFERRED_DOMAINS)
            if is_preferred:
                preferred_pdfs.append(absolute_url)
            else:
                other_pdfs.append(absolute_url)
    
    # Prioritize preferred domains
    all_pdfs = preferred_pdfs + other_pdfs
    return all_pdfs[0] if all_pdfs else None


def download_pdf(pdf_url, output_path):
    """
    Download a PDF from a URL and save to disk.
    Returns True if successful, False otherwise.
    """
    try:
        logger.debug(f"Downloading {pdf_url[:60]}...")
        response = requests.get(pdf_url, timeout=20)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        logger.debug(f"Saved to {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to download {pdf_url[:60]}...: {str(e)[:50]}")
        return False


def download_all_pdfs(website_cache, output_dir):
    """
    Download all PDFs from the website cache.
    Returns a list of (website, pdf_link, pdf_path) tuples for successful downloads.
    """
    logger.info("Downloading bulletins...")
    
    os.makedirs(output_dir, exist_ok=True)
    downloaded = []
    
    for idx, (website, pdf_link) in enumerate(website_cache.items(), 1):
        if not pdf_link:
            logger.debug(f"Skipping {website}: No PDF link")
            continue
        
        output_path = os.path.join(output_dir, f'bulletin_{idx}.pdf')
        if download_pdf(pdf_link, output_path):
            downloaded.append((website, pdf_link, output_path))
    
    logger.info(f"Downloaded {len(downloaded)} bulletins")
    return downloaded