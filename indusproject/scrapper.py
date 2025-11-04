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

# ================= MAIN SCRAPER =================
def scrape_indus_po_data(max_pages=1):
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
            print("[INFO] Starting PO scraping...")

            current_page = 1
            seen_first_po = None

            while current_page <= max_pages:
                page.wait_for_selector("span#ResultRN1 table tbody tr", timeout=30000)
                rows = page.query_selector_all("span#ResultRN1 table tbody tr")

                if not rows:
                    print("[INFO] No rows found on this page.")
                    break

                first_po_current = rows[0].query_selector_all("td")[0].inner_text().strip()
                if first_po_current == seen_first_po:
                    print("[INFO] Page repeated, stopping pagination.")
                    break
                seen_first_po = first_po_current

                po_list = []
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

                        po_list.append({
                            "po_number": po_number,
                            "rev": rev,
                            "order_date": order_date,
                            "scraped_at": datetime.datetime.now().isoformat(),
                            "items": []
                        })

                print(f"[INFO] Page {current_page}: Found {len(po_list)} POs.")

                # Visit each PO (with Advanced Search fallback)
                for po in po_list:
                    po_link_selector = f"span#ResultRN1 a:has-text('{po['po_number']}')"
                    if not safe_click(page, po_link_selector):
                        print(f"[INFO] PO {po['po_number']} not visible in table — using Advanced Search.")
                        try:
                            if not page.query_selector("input[name*='PO_NUMBER']"):
                                safe_click(page, "button[title='Advanced Search']")
                                page.wait_for_timeout(2000)
                            po_number_field = page.query_selector("input[name*='PO_NUMBER']")
                            if po_number_field:
                                po_number_field.fill(po["po_number"])
                            else:
                                print("[WARNING] Could not find PO Number field in Advanced Search")
                                continue
                            go_button = page.query_selector("button:has-text('Go')")
                            if go_button:
                                page.evaluate("(btn) => btn.click()", go_button)
                            else:
                                print("[WARNING] Could not find Go button in Advanced Search")
                                continue
                            page.wait_for_selector("span#ResultRN1 table tbody tr", timeout=30000)
                            search_po_link = page.query_selector(f"span#ResultRN1 a:has-text('{po['po_number']}')")
                            if not search_po_link:
                                print(f"[WARNING] No search results found for PO {po['po_number']}")
                                continue
                            page.evaluate("(link) => link.click()", search_po_link)
                            page.wait_for_load_state("networkidle", timeout=30000)
                            items = scrape_po_details(page, po['po_number'])
                            po['project'] = group_items_by_indus_id(items)
                            del po['items']
                            try:
                                date_elem = page.query_selector("span[id*='PosOrderDateTime']")
                                if date_elem:
                                    po["creation_date"] = date_elem.inner_text().strip()
                            except Exception:
                                pass
                            page.go_back()
                            page.wait_for_load_state("networkidle", timeout=30000)
                            result.append(po)
                        except Exception as e:
                            print(f"[ERROR] Failed fallback Advanced Search for {po['po_number']}: {e}")
                        continue

                    # Normal flow (PO visible)
                    items = scrape_po_details(page, po['po_number'])
                    po['project'] = group_items_by_indus_id(items)
                    del po['items']
                    page.go_back()
                    page.wait_for_load_state("networkidle", timeout=30000)
                    result.append(po)

                # Pagination
                next_button = page.query_selector("a[title^='Next'], a:has-text('Next')")
                if next_button and "disabled" not in (next_button.get_attribute("class") or "").lower():
                    page.evaluate("(btn) => btn.click()", next_button)
                    page.wait_for_timeout(4000)
                    current_page += 1
                    continue
                break

            browser.close()
            print(f"[✓] Scraping completed. Total POs: {len(result)}")
            return store_po_data_with_deduplication(result)

    except Exception as e:
        print(f"[SCRAPER ERROR] {e}")
        return store_po_data_with_deduplication(result) if result else []

