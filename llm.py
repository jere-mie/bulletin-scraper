"""
LLM interaction utilities for analyzing church bulletins.
Compares bulletin PDFs to existing church JSON data and returns markdown.
Now supports image-based analysis for better accuracy.
"""

import base64
import json
import os
import io
import time
import logging
import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv()

OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
OPENROUTER_API_URL = 'https://openrouter.ai/api/v1/chat/completions'

# Validate API key early so the user gets a clear error when it's missing
if not OPENROUTER_API_KEY:
    logger.error("OPENROUTER_API_KEY is not set. Set it in .env or the environment.")
    raise RuntimeError(
        "OPENROUTER_API_KEY is not set. Please set it in your environment or .env file "
        "before running the bulletin analyzer."
    )
# Model options: prefer faster models for bulk processing
PREFERRED_MODEL = 'google/gemini-3.1-flash-lite-preview'
FALLBACK_MODEL = 'google/gemini-2.5-flash-lite'

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = [2, 5, 10]  # Seconds to wait between retries

# Import PDF to images conversion
from .pdf_to_images import convert_pdf_to_images


def _make_api_request(url, headers, payload, timeout, context=""):
    """
    Make API request with retry logic for 502/503 errors.
    
    Args:
        url: API endpoint URL
        headers: Request headers
        payload: Request payload
        timeout: Request timeout in seconds
        context: Context string for logging (e.g., church names)
    
    Returns:
        Response JSON dict or None on failure
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            # Retry on 502/503 errors (server issues)
            if response.status_code in [502, 503]:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY[attempt]
                    logger.warning(f"⚠ {response.status_code} error for {context}, retrying in {delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"API request failed after {MAX_RETRIES} retries for {context}: {response.status_code} {e}")
                    return None
            else:
                # Don't retry other HTTP errors (401, 429, etc.)
                logger.error(f"API request failed for {context}: {response.status_code} {e}")
                return None
                
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY[attempt]
                logger.warning(f"⚠ Timeout for {context}, retrying in {delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(delay)
                continue
            else:
                logger.error(f"API request timed out after {MAX_RETRIES} retries for {context}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {context}: {str(e)[:100]}")
            return None
    
    return None


def _encode_image_to_base64(image):
    """
    Encode an image (PIL Image or file path) to base64 data URL.
    """
    from PIL import Image
    
    if isinstance(image, str):
        # It's a file path
        with open(image, 'rb') as f:
            img_bytes = f.read()
    else:
        # It's a PIL Image
        buffer = io.BytesIO()
        image.save(buffer, format='PNG')
        img_bytes = buffer.getvalue()
    
    img_base64 = base64.b64encode(img_bytes).decode('utf-8')
    return f"data:image/png;base64,{img_base64}"


def _build_image_content(images, prompt_text):
    """
    Build the content array for multi-image LLM request.
    """
    content = [{'type': 'text', 'text': prompt_text}]
    
    for i, img in enumerate(images):
        data_url = _encode_image_to_base64(img)
        content.append({
            'type': 'image_url',
            'image_url': {
                'url': data_url,
                'detail': 'high'
            }
        })
    
    return content


def analyze_bulletin(pdf_path, churches_data, model=None, use_images=True):
    """
    Send a bulletin PDF to the LLM and ask it to compare against existing church data.
    Returns markdown with any differences found, or empty string if no differences.
    
    Args:
        pdf_path: Path to the bulletin PDF file
        churches_data: Either a single church dict or a list of church dicts that share this bulletin
        model: Optional model to use (overrides PREFERRED_MODEL)
        use_images: If True, convert PDF to images for analysis (recommended for accuracy)
    
    Returns:
        Markdown string with differences (empty if no differences), or None on error
    """
    if not os.path.exists(pdf_path):
        logger.error(f"PDF not found: {pdf_path}")
        return None
    
    # Use provided model or fall back to preferred model
    model_to_use = model if model else PREFERRED_MODEL
    
    # Ensure we have a list
    if isinstance(churches_data, dict):
        churches_list = [churches_data]
    else:
        churches_list = churches_data
    
    church_names = ', '.join([c.get('name', 'Unknown') for c in churches_list])
    
    try:
        # Prepare the comparison prompt
        prompt = f"""ONLY OUTPUT FINAL TABLES. NO EXPLANATIONS. NO PREAMBLE.

Database:
{json.dumps(churches_list, indent=2)}

Compare the bulletin images to database. Output ONLY:

"NO DIFFERENCES" if all times match exactly.

OR table format with ONLY actual differences:

### Church Name

| Field | Bulletin | Database | Page | Note |
|-------|----------|----------|------|------|
| Saturday Mass | 1700 | 1730 | 1 | Optional |

RULES:
- Only include rows where Bulletin value DIFFERS from Database value
- Do NOT include rows where both are empty/missing
- Do NOT include rows where both match
- ALWAYS convert all times to 4-digit 24-hour format (e.g., 5:30 PM -> 1730, 9:45 AM -> 0945)
- Be concise in Note field
- Page number required (based on image order, starting from 1)
- Ignore any non-mass/adoration/confession events
- Ignore any special events/holidays
- Ignore 'memorial masses' as they are not regular schedule
- No text before or after tables"""

        if use_images:
            # Convert PDF to images
            images = convert_pdf_to_images(pdf_path, max_pages=6)  # Limit to first 6 pages
            
            if not images:
                logger.warning(f"Failed to convert PDF to images, falling back to PDF mode: {pdf_path}")
                return _analyze_bulletin_pdf(pdf_path, churches_list, prompt, model_to_use, church_names)
            
            logger.info(f"Analyzing {len(images)} page images for: {church_names}")
            
            # Build multi-image content
            content = _build_image_content(images, prompt)
            
            payload = {
                'model': model_to_use,
                'messages': [{'role': 'user', 'content': content}],
                'reasoning': {
                    'max_tokens': 3000,
                    'exclude': True,
                    'enabled': True
                }
            }
        else:
            # Fall back to PDF mode
            return _analyze_bulletin_pdf(pdf_path, churches_list, prompt, model_to_use, church_names)
        
        # Call OpenRouter API
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        result = _make_api_request(OPENROUTER_API_URL, headers, payload, 180, church_names)
        
        if result is None:
            return None
        
        # Extract response content
        if 'choices' in result and len(result['choices']) > 0:
            content = result['choices'][0]['message']['content'].strip()
            
            logger.debug(f"LLM Raw Response:\n{content}")
            
            # Check if no differences
            if content == "NO DIFFERENCES":
                logger.info(f"✓ No differences found for: {church_names}")
                return ""  # Return empty string for no differences
            
            # Return the markdown response
            logger.info(f"✓ Found differences for: {church_names}")
            return content
        else:
            logger.error(f"No response from LLM for: {church_names}")
            return None
    
    except Exception as e:
        logger.error(f"Error analyzing bulletin for {church_names}: {str(e)[:100]}")
        return None


def _analyze_bulletin_pdf(pdf_path, churches_list, prompt, model_to_use, church_names):
    """
    Legacy PDF-based analysis (fallback when image conversion fails).
    """
    try:
        # Encode PDF as base64
        with open(pdf_path, 'rb') as f:
            pdf_base64 = base64.b64encode(f.read()).decode('utf-8')
        
        data_url = f"data:application/pdf;base64,{pdf_base64}"
        
        logger.info(f"Analyzing PDF (fallback mode) for: {church_names}")
        
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Build payload for PDF upload fallback
        payload = {
            'model': model_to_use,
            'messages': [
                {
                    'role': 'user',
                    'content': [
                        {'type': 'text', 'text': prompt},
                        {
                            'type': 'file',
                            'file': {
                                'filename': 'bulletin.pdf',
                                'file_data': data_url
                            }
                        }
                    ]
                }
            ],
            'reasoning': {
                'max_tokens': 3000,
                'exclude': True,
                'enabled': True
            }
        }

        result = _make_api_request(OPENROUTER_API_URL, headers, payload, 120, church_names)
        
        if result is None:
            return None
        
        if 'choices' in result and len(result['choices']) > 0:
            content = result['choices'][0]['message']['content'].strip()
            
            if content == "NO DIFFERENCES":
                logger.info(f"✓ No differences found for: {church_names}")
                return ""
            
            logger.info(f"✓ Found differences for: {church_names}")
            return content
        else:
            logger.error(f"No response from LLM for: {church_names}")
            return None
            
    except Exception as e:
        logger.error(f"PDF analysis failed for {church_names}: {str(e)[:100]}")
        return None


def update_churches_from_markdown(churches_data, markdown_report, markdown_results, model=None):
    """
    Use LLM to analyze the markdown report and update churches.json accordingly.
    
    Args:
        churches_data: List of church dictionaries (the current churches.json)
        markdown_report: The full markdown report content generated by analysis
        markdown_results: Dict of bulletin websites and their analysis results
        model: Optional model to use (overrides PREFERRED_MODEL)
    
    Returns:
        Updated list of church dictionaries with modifications applied
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Sending churches.json and analysis report to LLM for updates...")
        
        # Use provided model or fall back to preferred model
        model_to_use = model if model else PREFERRED_MODEL
        
        # Prepare the update prompt
        prompt = f"""You are a JSON data manager for a Catholic church database. 

CURRENT churches.json:
{json.dumps(churches_data, indent=2)}

ANALYSIS REPORT (differences found between bulletins and database):
{markdown_report}

TASK: Review the analysis report and update the churches.json to correct any discrepancies found in the bulletins.

RULES:
1. Only modify fields that have CONFIRMED differences in the analysis report
2. Use exact times from the bulletin (in HHMM 24-hour format)
3. Maintain all existing fields and structure
4. Do NOT invent or assume data not in the report
5. Return ONLY valid JSON, no explanations

EXAMPLE CHANGE:
If report shows "Saturday Mass | 1700 | 1730 | Page 1", change the Saturday mass from 1730 to 1700 in the church's masses array.

OUTPUT: Return ONLY the complete updated churches.json as valid JSON array. No markdown, no explanations."""

        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'model': model_to_use,
            'messages': [
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'timeout': 120
        }
        
        result = _make_api_request(OPENROUTER_API_URL, headers, payload, 120, "JSON update")
        
        if result is None:
            return churches_data
        
        if 'choices' in result and len(result['choices']) > 0:
            content = result['choices'][0]['message']['content'].strip()
            
            # Extract JSON from response (in case there's any extra text)
            import re
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(0)
                updated_churches = json.loads(json_str)
                logger.info(f"✓ Successfully generated updated churches data")
                return updated_churches
            else:
                logger.error("LLM response did not contain valid JSON array")
                return churches_data
        else:
            logger.error("No response from LLM for JSON update")
            return churches_data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for JSON update: {str(e)[:100]}")
        return churches_data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {str(e)[:100]}")
        return churches_data
    except Exception as e:
        logger.error(f"Error updating churches from markdown: {str(e)[:100]}")
        return churches_data


def extract_events_from_bulletin(pdf_path, churches_data, existing_events, model=None, use_images=True):
    """
    Extract upcoming events from a bulletin PDF using LLM.
    
    Args:
        pdf_path: Path to the bulletin PDF file
        churches_data: List of church dicts that share this bulletin (simplified, with id, name, familyOfParishes)
        existing_events: List of existing events for this family (simplified, for deduplication)
        model: Optional model to use (overrides PREFERRED_MODEL)
        use_images: If True, convert PDF to images for analysis (recommended for accuracy)
    
    Returns:
        List of event dicts extracted from the bulletin, or None on error.
        Events that match existing ones will have the same 'id'.
        New events will have 'id': None.
    """
    from datetime import datetime
    
    if not os.path.exists(pdf_path):
        logger.error(f"PDF not found: {pdf_path}")
        return None
    
    # Use provided model or fall back to preferred model
    model_to_use = model if model else PREFERRED_MODEL
    
    church_names = ', '.join([c.get('name', 'Unknown') for c in churches_data])
    family_of_parishes = None
    for c in churches_data:
        if c.get('familyOfParishes'):
            family_of_parishes = c.get('familyOfParishes')
            break
    
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Prepare the events extraction prompt
        prompt = f"""You are extracting UPCOMING SPECIAL EVENTS from a Catholic parish bulletin.

CHURCHES IN THIS BULLETIN:
{json.dumps(churches_data, indent=2)}

EXISTING EVENTS (from previous extractions - use for deduplication):
{json.dumps(existing_events, indent=2) if existing_events else "[]"}

CURRENT DATE: {current_date}

TASK: Extract ONLY special one-time or limited-time events. Look for:
- Parish social events (dinners, breakfasts, potlucks, picnics)
- Fundraisers and sales (bake sales, bazaars, raffles)
- Educational programs (Bible studies, RCIA sessions, workshops, retreats)
- Community outreach (food drives, volunteer events)
- Meetings (parish council, ministry meetings, info sessions)
- Seasonal celebrations (Christmas concerts, Advent programs, Lenten missions)
- Special liturgies with a SPECIFIC DATE (e.g., "Advent Penance Service on Dec 15")

DO NOT INCLUDE (these are handled separately):
- Regular weekly Mass schedules (Sunday Mass, Saturday Vigil, weekday Masses)
- Regular weekly Confession/Reconciliation times
- Regular weekly Adoration/Holy Hour schedules
- Daily Mass schedules
- Any recurring weekly activity without a specific one-time date
- General statements like "Confession available before Mass"

OUTPUT FORMAT (JSON array):
[
  {{
    "id": "existing_id_if_match_or_null",
    "title": "Event Name",
    "description": "Brief description",
    "church_id": "church-slug-or-null",
    "church_name": "Church Name or null",
    "family_of_parishes": "{family_of_parishes or 'Unknown'}",
    "date": "YYYY-MM-DD",
    "start_time": "HHMM or null",
    "end_time": "HHMM or null",
    "location": "Specific location or null",
    "tags": ["category1", "category2"]
  }}
]

PREDEFINED CATEGORIES (select one or more per event):
1. community - Community outreach, food drives, volunteer events
2. education - Bible studies, RCIA sessions, workshops, seminars
3. fundraiser - Bake sales, bazaars, raffles, fundraising events
4. liturgy - Special liturgies with a specific date (Advent service, Lenten mission, Stations of the Cross, etc.)
5. meeting - Parish council, ministry meetings, info sessions
6. retreat - Parish retreats, spiritual retreats, day of recollection
7. sacramental - Baptism preparation, marriage preparation, confirmation prep
8. seasonal - Christmas concerts, Easter programs, holiday celebrations
9. social - Dinners, breakfasts, potlucks, picnics, social gatherings
10. volunteer - Volunteer opportunities, volunteer recruitment events
11. other - Events that don't fit other categories

RULES:
- ONLY include events with a SPECIFIC DATE (not recurring weekly schedules)
- Convert all dates to YYYY-MM-DD format
- Convert times to 24-hour HHMM format (e.g., 5:30 PM -> 1730, 9:00 AM -> 0900)
- If date year is not specified, assume current year. If the date has already passed this year, assume next year.
- If an event is for a specific church, set church_id and church_name from the CHURCHES list above
- If an event is for the whole family of parishes (or church not specified), set church_id and church_name to null
- DEDUPLICATION: If an event matches one in EXISTING EVENTS (same or very similar title, same date, same church), return the SAME id from existing. For new events, set id to null.
- TAGS: Select one or more categories from the list above. Use lowercase category names (e.g., "community", "social"). An event can have multiple applicable tags (e.g., a fundraising dinner could be ["fundraiser", "social"]).
- Return empty array [] if no special events found
- Return ONLY valid JSON array, no explanations or markdown"""

        if use_images:
            # Convert PDF to images
            images = convert_pdf_to_images(pdf_path, max_pages=8)  # More pages for events
            
            if not images:
                logger.warning(f"Failed to convert PDF to images, falling back to PDF mode: {pdf_path}")
                return _extract_events_pdf(pdf_path, prompt, model_to_use, church_names)
            
            logger.info(f"Extracting events from {len(images)} page images for: {church_names}")
            
            # Build multi-image content
            content = _build_image_content(images, prompt)
            
            payload = {
                'model': model_to_use,
                'messages': [{'role': 'user', 'content': content}],
                'reasoning': {
                    'max_tokens': 5000,
                    'exclude': True,
                    'enabled': True
                }
            }
        else:
            return _extract_events_pdf(pdf_path, prompt, model_to_use, church_names)
        
        # Call OpenRouter API
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        result = _make_api_request(OPENROUTER_API_URL, headers, payload, 180, church_names)
        
        if result is None:
            return None
        
        # Extract response content
        if 'choices' in result and len(result['choices']) > 0:
            content = result['choices'][0]['message']['content'].strip()
            
            logger.debug(f"LLM Events Raw Response:\n{content}")
            
            # Try to parse JSON from response
            import re
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(0)
                events = json.loads(json_str)
                logger.info(f"✓ Extracted {len(events)} events for: {church_names}")
                return events
            elif content.strip() == '[]':
                logger.info(f"✓ No events found for: {church_names}")
                return []
            else:
                logger.warning(f"Could not parse events JSON for: {church_names}")
                logger.debug(f"Response content: {content[:500]}")
                return []
        else:
            logger.error(f"No response from LLM for events extraction: {church_names}")
            return None
    
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse events JSON for {church_names}: {str(e)[:100]}")
        return []
    except Exception as e:
        logger.error(f"Error extracting events for {church_names}: {str(e)[:100]}")
        return None


def _extract_events_pdf(pdf_path, prompt, model_to_use, church_names):
    """
    Legacy PDF-based events extraction (fallback when image conversion fails).
    """
    try:
        with open(pdf_path, 'rb') as f:
            pdf_base64 = base64.b64encode(f.read()).decode('utf-8')
        
        data_url = f"data:application/pdf;base64,{pdf_base64}"
        
        logger.info(f"Extracting events from PDF (fallback mode) for: {church_names}")
        
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'model': model_to_use,
            'messages': [
                {
                    'role': 'user',
                    'content': [
                        {'type': 'text', 'text': prompt},
                        {
                            'type': 'file',
                            'file': {
                                'filename': 'bulletin.pdf',
                                'file_data': data_url
                            }
                        }
                    ]
                }
            ],
            'reasoning': {
                'max_tokens': 5000,
                'exclude': True,
                'enabled': True
            }
        }
        
        result = _make_api_request(OPENROUTER_API_URL, headers, payload, 120, church_names)
        
        if result is None:
            return None
        
        if 'choices' in result and len(result['choices']) > 0:
            content = result['choices'][0]['message']['content'].strip()
            
            import re
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(0)
                events = json.loads(json_str)
                logger.info(f"✓ Extracted {len(events)} events for: {church_names}")
                return events
            elif content.strip() == '[]':
                logger.info(f"✓ No events found for: {church_names}")
                return []
            else:
                logger.warning(f"Could not parse events JSON for: {church_names}")
                return []
        else:
            logger.error(f"No response from LLM for events extraction: {church_names}")
            return None
            
    except Exception as e:
        logger.error(f"PDF events extraction failed for {church_names}: {str(e)[:100]}")
        return None