# --- THIS IS THE FINAL, CORRECTED SCRIPT ---
import os
import sys
import json
import logging
import asyncio
import re
import time
import io
from sqlite3 import Error
import sqlite3
import fitz  # PyMuPDF
import google.generativeai as genai
from PIL import Image
from dotenv import load_dotenv
from thefuzz import fuzz



# --- 1. Load Environment Variables & Configure API ---
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable not set.")
genai.configure(api_key=GEMINI_API_KEY)

# --- 2. Global Settings & Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Globals ---
DB_NAME = "voter_data.db"
MODEL_NAME = "gemini-1.5-flash"
MAX_CONCURRENT_REQUESTS = 50

# --- 3. Prompts and Mappings (Keep your existing prompts) ---
# --- 3. Prompts and Mappings ---
HEADER_PAGE_PROMPT = """
# PROMPT_VERSION: 2025-07-18_H1 - Header Metadata Prompt;-

Perform OCR on this image. This is a header/metadata page of a voter list PDF. Extract all key information.
Your response MUST be a valid JSON object only, with no additional text, explanations, or conversational elements.

The page contains several sections with electoral roll metadata. Extract the following fields:

- 'type': "header_metadata"
- 'json_schema_version': "2025-07-18_H1"

For the top section with general election information:
- 'roll_main_title': Extract the main title (e.g., "મતદારયાદી 2025 S06 ગુજરાત").
- 'assembly_constituency_number_name_estimated': Extract the full text for the Assembly Constituency Number, Name, and estimate (e.g., "વિધાનસભા મત વિભાગનો નંબર, નામ અને અંદાજિત: 160-સુરત ઉત્તર").
- 'part_number_top_right': Extract the 'ભાગ નંબર' from the top right (e.g., "86").

For '1. સુધારાની વિગત' section:
- 'revision_year': Extract the year (e.g., "2025").
- 'qualification_date': Extract the date (e.g., "01-04-2024").
- 'revision_type': Extract the type of revision (e.g., "સળંગ સુધારણા 2025").
- 'publication_date': Extract the publication date (e.g., "10-04-2025").

For 'મતદારયાદીની ઓળખ' section:
- 'electoral_roll_details': Extract the entire descriptive paragraph about the electoral roll revision. This should be a single string containing all the text from this box.

For '2. ભાગ અને મતદાન ક્ષેત્રની વિગત' section:
- 'part_and_polling_area_details_title': Extract the main title of this subsection (e.g., "ભાગમાં સમાવેશ વિધાનસભાનો નંબર અને નામ").
- 'part_and_polling_area_details_title': Extract the main title of this subsection (e.g., "ભાગમાં સમાવેશ વિધાનસભાનો નંબર અને નામ").
- 'location_1': Extract the full text for the first location line.
- 'location_2': Extract the full text for the second location line.
- 'location_3': Extract the full text for the third location line.
- 'location_4': Extract the full text for the fourth location line.
- 'location_5': Extract the full text for the fifth location line.
- 'location_6': Extract the full text for the sixth location line.
- get all the locations in this manner.



For 'સામાન્ય સર્વેક્ષણ / નામ' section:
- 'district': Extract the district name (e.g., "સુરત").
- 'taluka': Extract the taluka name (e.g., "સુરત (શહેર)").
- 'department': Extract the department name (e.g., "ઉત્તર").
- 'pin_code': Extract the pin code (e.g., "395003").

For '3. મતદાન મથકની વિગત' section:
- 'polling_station_name_number': Extract the full text for the polling station number and name (e.g., "1- સેયદપુરા-૩").
- 'polling_station_type': Extract the type of polling station (e.g., "સામન્ય").
- 'male_voters_in_part_count': Extract the number of male voters in this part (e.g., "0").

For '4. મતદારોનો સંક્ષેપ' section:
- 'total_voters_serial_no': Extract the number under 'અનુક્રમ નંબર' (e.g., "1").
- 'total_voters_number': Extract the number under 'નંબર' (e.g., "1053").
- 'total_voters_male_count': Extract the number under 'પુરુષ' (e.g., "584").
- 'total_voters_female_count': Extract the number under 'સ્ત્રી' (e.g., "459").
- 'total_voters_other_gender_count': Extract the number under 'ત્રીજી જાતિ' (e.g., "0").
- 'total_voters_grand_total_count': Extract the total number under 'કુલ' (e.g., "1043").

Important Instructions:
- Ensure all extracted text is in Gujarati as it appears in the image.
- If a field is not present or OCR confidence low, return an empty string for that key, but maintain the specified JSON structure.
""" 

VOTER_LIST_PAGE_PROMPT = """
# PROMPT_VERSION: 2025-07-19_V3_JSONL - Voter List Page Prompt (Line-Delimited JSON)

You are an expert at extracting structured information from voter list PDF images.
Perform OCR on this image. Then, from the OCR'd text all individual voter objects.

# MANDATORY EXTRACTION PLAN:
You MUST follow this plan precisely.
1. **Analyze Layout:** First, perform a full analysis of the entire image. Acknowledge that the page is structured as a grid of voter boxes, arranged in rows and columns.
2. **Row-by-Row Processing:** You MUST process the data row by row. Start with the top row and extract the voter from the left column, then the middle column, then the right column. Continue this for all rows visible in the image.
3. **Final Check:** Before concluding, perform a final visual sweep of the entire grid to ensure no voter boxes in any row or column have been missed.
4. **Generate Output:** Produce the final output as one valid JSON object per line. Your response must contain ONLY the JSON objects, with no extra text or explanations.

**Overall Page Instruction:**
First, identify the 'વિભાગ નામ' at the top of the page and extract only the name of the area.


For each 'voter' object (one per line):
- 'type': "voter"
- 'json_schema_version': "2025-07-19_V3_JSONL"
- 'SL_NO': Extract the serial number shown next to the voter's details box (e.g., "1").
- 'VOTER_NAME': Locate the 'મતદાનું નામ:' field. Extract **ALL text** that follows 'મતદાનું નામ:' up until the next distinct field label (e.g., 'પિતાનું નામ:', 'પતિનું નામ:', or 'ઘર નંબર:'). **Combine all words and parts, including any surnames appearing on the same or subsequent lines, into a single, complete full name for the voter.** (e.g., "અવધકુમાર શેરવાળી", "મોહમદમુસા હલદર", "મોહમદ શભર હલદર").
- 'RELATIVE_NAME': Locate the 'પિતાનું નામ:' or 'પતિનું નામ:' field. Extract **ALL text** that follows this relation label up until the next distinct field label (e.g., 'ઘર નંબર:', 'ઉમર :'). **Combine all words and parts, including any surnames appearing on the same or subsequent lines, into a single, complete full name for the father/husband/mother.** (e.g., "હિરાલાલ શેરવાળી", "મોહમદઅલીમ હલદર"). If no relation name is found, use an empty string.
- 'HOUSE_NO': Extract the house number (e.g., '7-3327').
- 'AGE': Extract the age as a number (e.g., '47').
- 'GENDER': Extract the gender (e.g., 'પુરૂષ').
- 'IDCARD_NO': Extract the Voter ID (e.g: "SRV2111425","XDA3171667","GJ/21/141/006010",etc.....).**Extracting this field is very important,It should be there in the voter object otherwise the object is of no use**
- 'RLN_TYPE': "Identify if RELATIVE_NAME field is 'પતિનું નામ:'= H, 'પિતાનું નામ:'= F, 'માતાનું નામ'= M, 'અન્ય'= O"
- 'ALL_TXT': Extract all raw text content within this specific voter's detail box.
- 'BOX_NO_ON_PAGE': The sequential number of the voter on this specific page (1-30).
- 'STATUSTYPE': **Analyze the voter's box. If a semi-transparent "DELETED" stamp is visible over the text, set this to "D".** If a "#" symbol appears before the SL_NO, set this to "M". Otherwise, set this to "N".
- 'PAGE_SECTION_NAME': "Use the section name you identified at the start of the process as the value for this field. Use the exact same value for every voter object on this page."


**extraction over speed, take your time but do everything as instructed**
"""

FOOTER_PAGE_PROMPT = """
# PROMPT_VERSION: 2025-07-18_F1 - Footer Summary Prompt

Perform OCR on this image. This is the **final summary page** of a voter list PDF.
**STRICTLY** extract information into a JSON object.
Your response MUST be a valid JSON object ONLY, with ABSOLUTELY no additional text, explanations, or conversational elements.

Return JSON in the following format:
{
  "type": "footer_summary",
  "json_schema_version": "2025-07-18_F1",
  "assembly_constituency_number_name": "<Extract 'વિધાનસભા મત વિભાગનો નંબર અને નામ' like '160-સુરત ઉત્તર'>",
  "part_number": "<Extract 'ભાગ નંબર' like '86'>",
  "summary_voters_section_A": {
    "rows": [
      {
        "description_type": "મૂળ મતદાર યાદી - નવા સીમાંકન પ્રમાણેના મત",
        "male_count": "<Extract number>",
        "female_count": "<Extract number>",
        "other_gender_count": "<Extract number>",
        "total_count": "<Extract number>"
      }
    ]
  }
}
"""

PROMPT_MAPPING = {
    "header_metadata": HEADER_PAGE_PROMPT,
    "footer_summary": FOOTER_PAGE_PROMPT,
    "voter_list_page": VOTER_LIST_PAGE_PROMPT
}

# --- 4. Helper & Processing Functions ---
def normalize_gender(gender_text):
    if gender_text:
        if "પુર" in gender_text: return "પુરુષ"
        if "સ્ત્ર" in gender_text: return "સ્ત્રી"
    return gender_text

def convert_pdf_page_to_image(pdf_path, page_number):
    try:
        doc = fitz.open(pdf_path)
        if page_number >= len(doc): return None
        page = doc.load_page(page_number)
        pix = page.get_pixmap(dpi=300)
        image = Image.open(io.BytesIO(pix.tobytes("png")))
        doc.close()
        return image
    except Exception as e:
        logging.exception(f"Error converting PDF page {page_number} for {pdf_path}.")
        return None

async def _do_nothing():
    return None

async def process_image_with_gemini_async(semaphore, image, prompt, page_identifier):
    model = genai.GenerativeModel(MODEL_NAME)
    retries = 5
    async with semaphore:
        for attempt in range(retries):
            try:
                response = await model.generate_content_async([prompt, image])
                if response and hasattr(response, "text") and response.text:
                    return response.text
                logging.warning(f"Empty response for page {page_identifier}, attempt {attempt+1}")
                if attempt < retries - 1: await asyncio.sleep(2**attempt)
            except Exception as e:
                logging.warning(f"API Error on page {page_identifier}, attempt {attempt+1}: {e}")
                if attempt < retries - 1: await asyncio.sleep(min(60, 2**attempt))
    logging.error(f"Failed to get response for page {page_identifier} after {retries} retries.")
    return None

async def process_voter_page_in_chunks_async(semaphore, page_image, prompt, page_index):
    logging.info(f"Splitting voter page {page_index + 1} for chunked processing.")
    width, height = page_image.size
    midpoint = height // 2
    top_half = page_image.crop((0, 0, width, midpoint))
    bottom_half = page_image.crop((0, midpoint, width, height))
    chunk_tasks = [
        process_image_with_gemini_async(semaphore, top_half, prompt, f"{page_index + 1}-Top"),
        process_image_with_gemini_async(semaphore, bottom_half, prompt, f"{page_index + 1}-Bottom")
    ]
    chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
    combined_response_text = ""
    for i, result in enumerate(chunk_results):
        if isinstance(result, Exception) or not result:
            logging.error(f"Failed to get a valid result for page {page_index + 1}, part {i+1}. Error: {result}")
            continue
        cleaned_chunk = result.strip().removeprefix("```json").removesuffix("```").strip()
        combined_response_text += cleaned_chunk + "\n"
    return combined_response_text.strip()

def parse_gemini_response(text_response, prompt_type, page_index):
    if not text_response:
        logging.warning(f"Cannot parse empty response for page {page_index+1}")
        return None
    if prompt_type in ["header_metadata", "footer_summary"]:
        cleaned_response = text_response.strip().removeprefix("```json").removesuffix("```").strip()
        try:
            match = re.search(r"\{[\s\S]*\}", cleaned_response)
            if match:
                return json.loads(match.group(0))
            logging.error(f"No valid JSON object found for page {page_index+1}")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error for page {page_index+1}: {e}\nResponse:\n{cleaned_response}")
            return None
    else:  # Voter list page (JSONL)
        parsed_data = []
        for i, line in enumerate(text_response.strip().split("\n")):
            if line.strip():
                try:
                    parsed_data.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    logging.warning(f"Page {page_index+1}, Line {i+1}: Could not parse JSONL. RAW LINE: '{line.strip()}'")
        return parsed_data

# --- 5. Database Schema and Functions ---
TABLES = {
    'pdfs': "CREATE TABLE IF NOT EXISTS `pdfs` (`id` INTEGER PRIMARY KEY, `file_name` TEXT NOT NULL UNIQUE, `assembly_constituency` TEXT, `part_number` INTEGER, `publication_date` TEXT, `total_voters_count` INTEGER, `processed_at` TEXT DEFAULT CURRENT_TIMESTAMP)",
    'sections': "CREATE TABLE IF NOT EXISTS `sections` (`id` INTEGER PRIMARY KEY, `pdf_id` INTEGER NOT NULL, `section_name` TEXT, FOREIGN KEY (`pdf_id`) REFERENCES `pdfs`(`id`) ON DELETE CASCADE)",
    'voters': "CREATE TABLE IF NOT EXISTS `voters` (`id` INTEGER PRIMARY KEY, `section_id` INTEGER NOT NULL, `idc_no` TEXT NOT NULL UNIQUE, `VOTER_NAME` TEXT, `RELATIVE_NAME` TEXT, `rln_type` TEXT, `house_no` TEXT, `age` INTEGER, `gender` TEXT, `sl_no_in_pdf` INTEGER, `box_no_on_page` INTEGER, `page_no` INTEGER,`all_text` TEXT, `statustype` TEXT, FOREIGN KEY (`section_id`) REFERENCES `sections`(`id`) ON DELETE CASCADE)",
    'summary_stats': "CREATE TABLE IF NOT EXISTS `summary_stats` (`id` INTEGER PRIMARY KEY, `pdf_id` INTEGER NOT NULL, `description` TEXT, `male_count` INTEGER, `female_count` INTEGER, `other_gender_count` INTEGER, `total_count` INTEGER, FOREIGN KEY (`pdf_id`) REFERENCES `pdfs`(`id`) ON DELETE CASCADE)"
}

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = ON;")
        logging.info(f"Connected to SQLite: {db_file}")
        return conn
    except Error as e:
        logging.error(f"DB connection error: {e}")
        return None

def create_tables(conn):
    try:
        with conn:
            for ddl in TABLES.values():
                conn.execute(ddl)
        logging.info("Database tables verified successfully.")
    except Error as e:
        logging.error(f"Failed to create tables: {e}")

def insert_pdf_data(conn, pdf_data, file_name):
    # This function is fine as is
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM pdfs WHERE file_name = ?", (file_name,))
            if cursor.fetchone():
                logging.warning(f"PDF '{file_name}' already exists. Skipping.")
                return None
            header = pdf_data.get('header_metadata', {})
            footer = pdf_data.get('footer_summary', {})
            assembly_const = header.get("assembly_constituency_number_name_estimated", "")
            part_num = header.get("part_number_top_right")
            pub_date_str = header.get("publication_date", "")
            pub_date = None
            if pub_date_str:
                try:
                    pub_date = time.strftime('%Y-%m-%d', time.strptime(pub_date_str, '%d-%m-%Y'))
                except ValueError:
                    logging.warning(f"Could not parse date '{pub_date_str}'.")
            total_voters = None
            if footer and footer.get("summary_voters_section_A", {}).get("rows"):
                total_voters = footer["summary_voters_section_A"]["rows"][-1].get("total_count")
            values = (file_name, assembly_const, part_num, pub_date, total_voters)
            cursor.execute("INSERT INTO pdfs (file_name, assembly_constituency, part_number, publication_date, total_voters_count) VALUES (?, ?, ?, ?, ?)", values)
            pdf_id = cursor.lastrowid
            logging.info(f"Inserted PDF '{file_name}' with ID: {pdf_id}")
            if footer and footer.get("summary_voters_section_A", {}).get("rows"):
                for row in footer["summary_voters_section_A"]["rows"]:
                    stat_values = (pdf_id, row.get('description_type'), row.get('male_count'), row.get('female_count'), row.get('other_gender_count'), row.get('total_count'))
                    cursor.execute("INSERT INTO summary_stats (pdf_id, description, male_count, female_count, other_gender_count, total_count) VALUES (?, ?, ?, ?, ?, ?)", stat_values)
            return pdf_id
    except Error as e:
        logging.error(f"DB Error in insert_pdf_data: {e}", exc_info=True)
        return None

def insert_sections(conn, pdf_id, header_data):
    if not header_data: return {}
    section_cache = {}
    try:
        with conn:
            cursor = conn.cursor()
            for key, value in header_data.items():
                if key.startswith("location_") and value:
                    section_name = value.strip()
                    cursor.execute("SELECT id FROM sections WHERE pdf_id = ? AND section_name = ?", (pdf_id, section_name))
                    existing = cursor.fetchone()
                    if not existing:
                        cursor.execute("INSERT INTO sections (pdf_id, section_name) VALUES (?, ?)", (pdf_id, section_name))
                        section_cache[section_name] = cursor.lastrowid
                    else:
                        section_cache[section_name] = existing[0]
        return section_cache
    except Error as e:
        logging.error(f"DB Error in insert_sections: {e}", exc_info=True)
        return {}

def insert_voter_data(conn, pdf_id, all_voter_data, section_cache):
    """
    Inserts voter data using ONLY the PAGE_NO provided by the AI for each record.
    This version removes all fallback page number logic.
    """
    try:
        voters_inserted = 0
        sql = "INSERT OR IGNORE INTO voters (section_id, idc_no, VOTER_NAME, RELATIVE_NAME, rln_type, house_no, age, gender, sl_no_in_pdf, box_no_on_page, page_no, statustype, all_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        # This initial log runs once and is correct
        pages_to_process = [page[0].get('PAGE_NO', f"Unknown_Index_{i}") for i, page in enumerate(all_voter_data) if page and isinstance(page, list) and page[0]]
        logging.info(f"Database insertion: Attempting to process data from pages: {pages_to_process}")

        with conn:
            cursor = conn.cursor()
            
            for page_index, page_data in enumerate(all_voter_data):
                
                # These page-level checks are correct
                if not page_data or not isinstance(page_data, list) or 'voter' not in page_data[0].get('type', ''):
                    logging.warning(f"SKIPPING non-voter data at page index {page_index + 1}.")
                    continue
                
                # We get the page number here ONLY for the page-level log messages. It is NOT used for insertion.
                page_level_log_no = page_data[0].get('PAGE_NO', f'Unknown_Index_{page_index + 1}')
                page_section_name = page_data[0].get('PAGE_SECTION_NAME')

                if not page_section_name:
                    logging.warning(f"SKIPPING PAGE {page_level_log_no}: The 'PAGE_SECTION_NAME' field is missing. Discarding {len(page_data)} records.")
                    continue

                best_match = max(section_cache.items(), key=lambda item: fuzz.partial_ratio(page_section_name, item[0]), default=(None, None))
                section_id = best_match[1]

                if not section_id:
                    logging.warning(f"SKIPPING PAGE {page_level_log_no}: Failed to find a matching section_id for '{page_section_name}'. Discarding {len(page_data)} records.")
                    continue
                
                # This loop now uses the record's own page number for all its logging
                for record in page_data:
                    if record.get('IDCARD_NO'):
                        
                        # --- THIS IS THE FINAL GUARANTEE ---
                        # The page number being saved to the database comes ONLY from the record itself.
                        # The old `current_page_no` variable has been completely removed.
                        values = (
                            section_id, 
                            record.get('IDCARD_NO'), 
                            record.get('VOTER_NAME'), 
                            record.get('RELATIVE_NAME'), 
                            record.get('RLN_TYPE', 'O'), 
                            record.get('HOUSE_NO'), 
                            record.get('AGE'), 
                            normalize_gender(record.get('GENDER')), 
                            record.get('SL_NO'), 
                            record.get('BOX_NO_ON_PAGE'), 
                            record.get('PAGE_NO'),  # <-- DATA FROM AI RECORD
                            record.get('STATUSTYPE', 'N'), 
                            record.get('ALL_TXT')
                        )
                        cursor.execute(sql, values)
                        
                        if cursor.rowcount == 0:
                            logging.warning(f"DUPLICATE IGNORED on page {record.get('PAGE_NO')}: ID '{record.get('IDCARD_NO')}'")
                        
                        voters_inserted += cursor.rowcount
                    else:
                        logging.warning(f"SKIPPING RECORD on page {record.get('PAGE_NO')}: Missing IDCARD_NO. (Name: '{record.get('VOTER_NAME')}')")

            logging.info(f"Committed {voters_inserted} new voter records.")

    except Exception:
        logging.error("!!!!!! A CRITICAL UNEXPECTED ERROR OCCURRED IN insert_voter_data !!!!!!", exc_info=True)
# --- 6. Main Pipeline Function ---
async def process_single_pdf_and_store_data_async(pdf_path, status_callback, db_connection):
    """
    Processes a single PDF file, and MANUALLY adds the correct page number
    to each voter record after parsing.
    """
    pdf_file_name = os.path.basename(pdf_path)
    logging.info(f"Starting processing for {pdf_file_name}...")
    status_callback("processing", f"Opening {pdf_file_name}...")

    try:
        with fitz.open(pdf_path) as doc: num_pages = len(doc)
    except Exception as e:
        logging.error(f"Could not open PDF {pdf_path}: {e}")
        status_callback("error", f"Could not open PDF: {e}")
        return

    page_prompts = ["header_metadata" if i == 0 else "footer_summary" if i == num_pages - 1 else "voter_list_page" for i in range(num_pages)]
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = []
    
    for i in range(num_pages):
        if i == 1: # Skip page 2
            tasks.append(_do_nothing())
            continue
        page_image = convert_pdf_page_to_image(pdf_path, i)
        if not page_image: continue
        
        prompt_type = page_prompts[i]
        task = process_voter_page_in_chunks_async(semaphore, page_image, VOTER_LIST_PAGE_PROMPT, i) if prompt_type == "voter_list_page" else process_image_with_gemini_async(semaphore, page_image, PROMPT_MAPPING.get(prompt_type), i + 1)
        tasks.append(task)

    status_callback("processing", f"Extracting data from {num_pages} pages...")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    all_parsed_data = {}
    for i, res in enumerate(results):
        if isinstance(res, Exception) or not res:
            logging.error(f"Failed to get result for page {i + 1}: {res}")
            continue
            
        prompt_type = page_prompts[i]
        parsed = parse_gemini_response(res, prompt_type, i)
        
        if parsed:
            # --- THIS IS THE FIX ---
            # If this is a voter page, loop through every record and add the correct page number.
            if prompt_type == "voter_list_page" and isinstance(parsed, list):
                correct_page_no = i + 1
                for record in parsed:
                    if isinstance(record, dict):
                        record['PAGE_NO'] = correct_page_no
                logging.info(f"Page {correct_page_no}: Parsed {len(parsed)} voter records.")
            
            all_parsed_data[i] = parsed
            
    for i, data in all_parsed_data.items():
        if page_prompts[i] == "voter_list_page" and isinstance(data, list):
            for j, record in enumerate(data):
                if isinstance(record, dict): record['BOX_NO_ON_PAGE'] = j + 1
                    
    status_callback("processing", f"Saving extracted data for {pdf_file_name}...")
    header_data = all_parsed_data.get(0)
    footer_data = all_parsed_data.get(num_pages - 1)
    voter_pages_data = [data for i, data in all_parsed_data.items() if page_prompts[i] == "voter_list_page" and data]
    
    if not header_data:
        logging.error(f"Missing header data for '{pdf_file_name}'. Cannot proceed.")
        status_callback("error", f"Missing header data for {pdf_file_name}")
        return
        
    pdf_id = insert_pdf_data(db_connection, {'header_metadata': header_data, 'footer_summary': footer_data}, pdf_file_name)
    if pdf_id is None:
        logging.warning(f"PDF record for {pdf_file_name} not inserted (may already exist).")
        status_callback("processing", f"{pdf_file_name} already exists in database. Skipped.")
        return
        
    logging.info(f"VERIFICATION: Passing a total of {sum(len(page) for page in voter_pages_data)} records to the database function.")
    section_cache = insert_sections(db_connection, pdf_id, header_data)
    insert_voter_data(db_connection, pdf_id, voter_pages_data, section_cache)
        
    logging.info(f"--- Finished processing {pdf_file_name} ---")