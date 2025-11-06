import os, json, datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError
from redis import Redis
from .credentials import *

load_dotenv()

# ================= REDIS HELPERS =================
def ConnectRedis():
    try:
        return Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            db=int(os.getenv("REDIS_DB"))
        )
    except Exception as e:
        print("Error in connecting redis:", str(e))
        return None

def get_redis_data(key):
    try:
        redis_client = ConnectRedis()
        cached_data = redis_client.get(key)
        return json.loads(cached_data) if cached_data else []
    except Exception as e:
        print(f"[CACHE ERROR] {e}")
        return []

def set_redis_data(key, data):
    try:
        redis_client = ConnectRedis()
        redis_client.set(key, json.dumps(data))
    except Exception as e:
        print(f"[REDIS SET ERROR] {e}")

def remove_duplicates_by_date(existing_data, new_data):
    try:
        existing_dates = {
            item.get("order_date") or item.get("creation_date")
            for item in existing_data
            if item.get("order_date") or item.get("creation_date")
        }
        unique_new_data = [
            item for item in new_data
            if (item.get("order_date") or item.get("creation_date")) not in existing_dates
        ]
        return unique_new_data
    except Exception as e:
        print(f"[DEDUPE ERROR] {e}")
        return new_data

def store_po_data_with_deduplication(new_data):
    try:
        existing_po_data = get_redis_data("indus_po_data")
        filtered_new_data = remove_duplicates_by_date(existing_po_data, new_data)

        set_redis_data("indus_latest_data", filtered_new_data)
        print(f"[✓] Stored {len(filtered_new_data)} new PO records to 'indus_latest_data'")

        updated_po_data = existing_po_data + filtered_new_data
        set_redis_data("indus_po_data", updated_po_data)
        print(f"[✓] Updated 'indus_po_data' with total {len(updated_po_data)} records")

        return filtered_new_data
    except Exception as e:
        print(f"[STORE ERROR] {e}")
        return []

# ================= DATA GROUPING =================
def group_items_by_indus_id(items):
    try:
        grouped = {}
        for item in items:
            site_id = item.get('indus_id', '').strip()
            project_id = item.get('project_id', '').strip()
            if not site_id:
                continue
            key = (site_id, project_id)
            if key not in grouped:
                grouped[key] = {
                    "site_id": site_id,
                    "project_id": project_id,
                    "line_items": []
                }
            grouped[key]["line_items"].append({
                "description": item.get('description', ''),
                "item_job": item.get('item_job', ''),
                "line": item.get('line', ''),
                "price": item.get('price', ''),
                "qty": item.get('qty', '')
            })
        return list(grouped.values())
    except Exception as e:
        print(f"[ERROR] Error grouping items by indus_id: {e}")
        return []

# ================= SCRAPING HELPERS =================
def scrape_po_details(page, po_number, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            if not wait_for_selector_retry(page, "tbody tr", timeout=60000, retries=retries):
                raise TimeoutError(f"Table not loaded for PO {po_number}")

            items, column_mapping = [], {}
            rows = page.query_selector_all("tbody tr")

            # Build column map
            for row in rows:
                headers = row.query_selector_all("th")
                if headers:
                    for idx, header in enumerate(headers):
                        column_mapping[header.inner_text().strip()] = idx
                    break

            if not column_mapping:
                headers = page.query_selector_all("thead th, table th")
                for idx, header in enumerate(headers):
                    column_mapping[header.inner_text().strip()] = idx

            # Parse rows
            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) < 10:
                    continue
                try:
                    line = cells[column_mapping.get("Line", 2)].inner_text().strip()
                    if not line or not line.isdigit():
                        continue
                    item_job = cells[column_mapping.get("Item/Job", 4)].inner_text().strip()
                    description = cells[column_mapping.get("Description", 6)].inner_text().strip()
                    qty = cells[column_mapping.get("Qty", 8)].inner_text().strip()
                    price = cells[column_mapping.get("Price", 9)].inner_text().strip()
                    indus_id = cells[column_mapping.get("Site ID", 25)].inner_text().strip() if 25 < len(cells) else ''
                    project_id = cells[column_mapping.get("Project Name", 27)].inner_text().strip() if 27 < len(cells) else ''
                    items.append({
                        "line": line,
                        "item_job": item_job,
                        "description": description,
                        "qty": qty,
                        "price": price,
                        "project_id": project_id,
                        "indus_id": indus_id
                    })
                except Exception as e:
                    print(f"[WARNING] Error parsing row in PO {po_number}: {e}")
            return items
        except TimeoutError:
            attempt += 1
            print(f"[TIMEOUT] Table not loaded for PO {po_number}. Retry {attempt}/{retries}")
            page.reload()
            page.wait_for_load_state("networkidle", timeout=30000)
    print(f"[ERROR] Failed to load table for PO {po_number} after {retries} retries")
    return []

# ================= SAFE NAVIGATION =================
def safe_click(page, selector, timeout=30000, wait_for_load=True, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            page.wait_for_selector(selector, timeout=timeout)
            page.click(selector)
            if wait_for_load:
                page.wait_for_load_state("networkidle", timeout=timeout)
            return True
        except TimeoutError:
            attempt += 1
            print(f"[WARNING] Timeout while waiting for {selector}. Retry {attempt}/{retries}")
            if attempt >= retries:
                return False
        except Exception as e:
            print(f"[ERROR] Failed to click {selector}: {e}")
            return False

def wait_for_selector_retry(page, selector, timeout=30000, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            page.wait_for_selector(selector, timeout=timeout)
            return True
        except TimeoutError:
            attempt += 1
            print(f"[WARNING] Timeout waiting for {selector}. Retry {attempt}/{retries}")
            if attempt >= retries:
                return False
        except Exception as e:
            print(f"[ERROR] Error waiting for {selector}: {e}")
            return False

def scrape_indus_po_data(max_pages=2):
    """
    Scrapes multiple pages of PO numbers first, then visits each PO to scrape details individually.
    After first 25 POs, uses Advanced Search to fetch remaining POs one by one.
    Ensures Orders tab is reloaded:
      1) Once after PO collection
      2) Before each Advanced Search for POs > 25
    """
    po_numbers = []
    result = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            page.goto(ERP_LOGIN_URL)

            # ---- Login ----
            page.fill("input#usernameField", ERP_USERNAME)
            page.fill("input#passwordField", ERP_PASSWORD)
            safe_click(page, "button:has-text('Log In')")
            page.wait_for_load_state("networkidle", timeout=30000)
            print("[✓] Logged into ERP system")

            # ---- Navigate to Orders ----
            safe_click(page, "img[title='Expand']")
            safe_click(page, "li >> text=Home Page")
            safe_click(page, "a:has-text('Orders')")
            print(f"[INFO] Starting PO number collection (up to {max_pages} pages)...")

            # Step 1: Collect all PO numbers from pages
            current_page = 1
            while current_page <= max_pages:
                page.wait_for_selector("span#ResultRN1 table tbody tr", timeout=30000)
                rows = page.query_selector_all("span#ResultRN1 table tbody tr")

                for row in rows:
                    cells = row.query_selector_all("td")
                    if len(cells) >= 6:
                        po_number = cells[0].inner_text().strip()
                        rev = cells[1].inner_text().strip()
                        order_date = cells[5].inner_text().strip()

                        if not po_number or any(x in po_number.lower() for x in ["next", "previous", "more"]):
                            continue
                        if not po_number[0].isalnum():
                            continue

                        po_numbers.append({
                            "po_number": po_number,
                            "rev": rev,
                            "order_date": order_date,
                            "scraped_at": datetime.datetime.now().isoformat(),
                            "items": []
                        })

                print(f"[INFO] Page {current_page}: Collected {len(rows)} POs")

                # Move to next page if available
                next_button = page.query_selector("a.x49[title='Next 25'], a:has-text('Next 25')")
                if next_button:
                    safe_click(page, "a.x49[title='Next 25'], a:has-text('Next 25')")
                    page.wait_for_timeout(2000)
                    current_page += 1
                else:
                    break

            print(f"[✓] Collected total {len(po_numbers)} PO numbers")

            # ---- Reset Orders tab once before starting detail scraping ----
            safe_click(page, "a:has-text('Orders')")
            print("[INFO] Reset Orders tab before starting detail scraping, waiting 15 seconds...")
            page.wait_for_timeout(15000)

            # Step 2: Visit each PO to scrape details
            for idx, po in enumerate(po_numbers, 1):
                print(f"[INFO] Scraping details for PO {idx}/{len(po_numbers)}: {po['po_number']}")

                if idx <= 25:
                    # ---- First 25 POs: normal navigation ----
                    try:
                        po_link_selector = f"span#ResultRN1 a:has-text('{po['po_number']}')"
                        if safe_click(page, po_link_selector):
                            items = scrape_po_details(page, po['po_number'])
                            po['project'] = group_items_by_indus_id(items)
                            del po['items']

                            # Scrape creation date
                            try:
                                date_elem = page.query_selector("span[id*='PosOrderDateTime']")
                                if date_elem:
                                    po["creation_date"] = date_elem.inner_text().strip()
                            except Exception:
                                pass

                            page.go_back()
                            page.wait_for_load_state("networkidle", timeout=30000)
                            print(f"[✓] Scraped details for PO {po['po_number']}")
                    except Exception as e:
                        print(f"[ERROR] Error scraping PO {po['po_number']}: {e}")
                else:
                    # ---- After 25 POs: use Advanced Search ----
                    # ---- After 25 POs: use Advanced Search ----
                    try:
                        # Reload Orders tab before each Advanced Search
                        print(f"[INFO] Reset Orders tab for Advanced Search for PO {po['po_number']}")
                        safe_click(page, "a:has-text('Orders')")
                        page.wait_for_timeout(15000)

                        # Click Advanced Search button
                        print("[INFO] Clicking Advanced Search button")
                        safe_click(page, "button#SrchBtn[title='Advanced Search']")
                        page.wait_for_timeout(3000)

                        # Enter PO number in search field
                        print(f"[INFO] Entering PO number {po['po_number']} in search field")
                        page.fill("input#Value_0", po['po_number'])

                        # Click Go button
                        print("[INFO] Clicking Go button")
                        safe_click(page, "button#customizeSubmitButton")
                        # Wait for the PO results table
                        page.wait_for_selector("table#ResultRN\\.PosVpoPoList\\:Content tbody tr", timeout=5000)

                        # Click the PO link by inner text (handles dynamic IDs like N3, N5, etc.)
                        po_link_selector = f"a[id*='PosPoNumber']:has-text('{po['po_number']}')"
                        print(f"[INFO] Clicking PO link for {po['po_number']} in search results")

                        if safe_click(page, po_link_selector):
                            page.wait_for_load_state("networkidle", timeout=30000)

                            items = scrape_po_details(page, po['po_number'])
                            po['project'] = group_items_by_indus_id(items)
                            del po['items']

                            # Scrape creation date
                            try:
                                date_elem = page.query_selector("span[id*='PosOrderDateTime']")
                                if date_elem:
                                    po["creation_date"] = date_elem.inner_text().strip()
                            except Exception:
                                pass

                            # Go back to PO summary table
                            page.go_back()
                            page.wait_for_load_state("networkidle", timeout=30000)
                            print(f"[✓] Scraped details for PO {po['po_number']} via Advanced Search")
                        else:
                            print(f"[WARNING] PO {po['po_number']} not found in Advanced Search results")

                    except Exception as e:
                        print(f"[ERROR] Error scraping PO {po['po_number']} via Advanced Search: {e}")


                result.append(po)

            browser.close()
            print(f"[✓] Scraping completed. Total POs: {len(result)}")
            return store_po_data_with_deduplication(result)

    except Exception as e:
        print(f"[SCRAPER ERROR] {e}")
        return store_po_data_with_deduplication(result) if result else []
