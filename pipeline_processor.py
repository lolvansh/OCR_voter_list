import os
import sys
import json
import logging
import asyncio
import re
import time
import io
from glob import glob
import sqlite3
from sqlite3 import Error
from thefuzz import fuzz  # You must run: pip install thefuzz python-Levenshtein

import fitz  # PyMuPDF
import google.generativeai as genai
from PIL import Image
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# --- Secure API Key Handling ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable not set. Please set it in a .env file.")
genai.configure(api_key=GEMINI_API_KEY)

# --- Global Token Counters & Lock ---
daily_input_tokens_count = 0
daily_output_tokens_count = 0
token_counter_lock = asyncio.Lock()

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Configuration ---
# This will be the directory where the .db file and potentially intermediate JSONs are stored.
# For a standalone app, it might be the same as the input PDF directory, or a specified output.
# We'll make it configurable via CLI, defaulting to a subdirectory for processed JSONs.
DEFAULT_PROCESSED_DATA_DIR = os.path.join(os.getcwd(), "processed_data")
DB_NAME = "voter_data.db"
MODEL_NAME = "gemini-1.5-flash"
MAX_CONCURRENT_REQUESTS = 50

# --- Prompts (from final_ocr.py - unchanged) ---
# PROMPT FOR THE FIRST HEADER/METADATA PAGE (page index 0)
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

# PROMPT FOR THE MAIN VOTER LIST PAGES (all pages between header and footer)
VOTER_LIST_PAGE_PROMPT = """
# PROMPT_VERSION: 2025-07-19_V3_JSONL - Voter List Page Prompt (Line-Delimited JSON)

You are an expert at extracting structured information from voter list PDF images.
Perform OCR on this image. Then, from the OCR'd text all individual voter objects.

# MANDATORY EXTRACTION PLAN:
You MUST follow this plan precisely.
1. **Analyze Layout:** First, perform a full analysis of the entire image. Acknowledge that the page is structured as a grid of voter boxes, arranged in rows and columns (usually 10 rows and 3 columns).
2. **Row-by-Row Processing:** You MUST process the data row by row. Start with the top row and extract the voter from the left column, then the middle column, then the right column. Once the first row is complete, move down to the second row and repeat the left-to-right process. Continue this for all 10 rows until the page is finished.
3. **Final Check:** Before concluding, perform a final visual sweep of the entire grid to ensure no voter boxes in any row or column have been missed.
4. **Generate Output:** Produce the final output as one valid JSON object per line. Your response must contain ONLY the JSON objects, with no extra text or explanations.

**Overall Page Instruction:**
First, identify the 'વિભાગ નામ' at the top of the page and extract only the name of the area.
**identify the 'PAGE_NO' by looking for the number next to the word 'પૃષ્ઠ' (page), which is usually at the bottom of the image. **Do NOT confuse this with the 'ભાગ નંબર' (Part Number).**
Remember both the section name and page number.
If either is not present, consider it to be an empty string.

For each 'voter' object (one per line):
- 'type': "voter"
- 'json_schema_version': "2025-07-19_V3_JSONL"
- 'SL_NO': Extract the serial number shown next to the voter's details box (e.g., "1").
- 'VOTER_NAME': Locate the 'મતદાનું નામ:' field. Extract **ALL text** that follows 'મતદાનું નામ:' up until the next distinct field label (e.g., 'પિતાનું નામ:', 'પતિનું નામ:', or 'ઘર નંબર:'). **Combine all words and parts, including any surnames appearing on the same or subsequent lines, into a single, complete full name for the voter.** (e.g., "અવધકુમાર શેરવાળી", "મોહમદમુસા હલદર", "મોહમદ શભર હલદર").
- 'RELATIVE_NAME': Locate the 'પિતાનું નામ:' or 'પતિનું નામ:' field. Extract **ALL text** that follows this relation label up until the next distinct field label (e.g., 'ઘર નંબર:', 'ઉમર :'). **Combine all words and parts, including any surnames appearing on the same or subsequent lines, into a single, complete full name for the father/husband/mother.** (e.g., "હિરાલાલ શેરવાળી", "મોહમદઅલીમ હલદર"). If no relation name is found, use an empty string.
- 'HOUSE_NO': Extract the house number (e.g., '7-3327').
- 'AGE': Extract the age as a number (e.g., '47').
- 'GENDER': Extract the gender (e.g., 'પુરૂષ').
- 'IDCARD_NO': Extract the Voter ID (e.g: "SRV2111425").
- 'RLN_TYPE': "Identify if RELATIVE_NAME field is 'પતિનું નામ:'= H, 'પિતાનું નામ:'= F, 'માતાનું નામ'= M, 'અન્ય'= O"
- 'ALL_TXT': Extract all raw text content within this specific voter's detail box.
- 'BOX_NO_ON_PAGE': The sequential number of the voter on this specific page (1-30).
- 'STATUSTYPE': **Analyze the voter's box. If a semi-transparent "DELETED" stamp is visible over the text, set this to "D".** If a "#" symbol appears before the SL_NO, set this to "M". Otherwise, set this to "N".
- 'PAGE_SECTION_NAME': "Use the section name you identified at the start of the process as the value for this field. Use the exact same value for every voter object on this page."
- 'PAGE_NO': "Use the page number you identified at the start of the process as the value for this field. Use the exact same value for every voter object on this page."

**extraction over speed, take your time but do everything as instructed**
"""


# PROMPT FOR THE LAST FOOTER PAGE (Last page of the PDF)
FOOTER_PAGE_PROMPT = """
# PROMPT_VERSION: 2025-07-18_F1 - Footer Summary Prompt

Perform OCR on this image. This is the **final summary page** of a voter list PDF.
**STRICTLY** extract information into a JSON object.
**DO NOT include the 'page_index', 'pdf_page_number', 'total_pdf_pages', or 'pdf_file_name' keys in your JSON output; these will be added by the script.**
Your response MUST be a valid JSON object ONLY, with ABSOLUTELY no additional text, explanations, or conversational elements.

Return JSON in the following format. Ensure all specified keys are present, even if their value is an empty string if data is not found.

{
  "type": "footer_summary",
  "json_schema_version": "2025-07-18_F1",
  "assembly_constituency_number_name": "<Extract 'વિધાનસભા મત વિભાગનો નંબર અને નામ' like '160-સુરત ઉત્તર'>",
  "part_number": "<Extract 'ભાગ નંબર' like '86'>",
  "summary_voters_section_A": {
    "title": "મતદારોનું સંક્ષિપ્ત વિવરણ",
    "sub_title": "A) મતદારોની સંખ્યા",
    "table_header": {
      "type_of_voter_list": "મતદાર યાદીનો પ્રકાર",
      "identity_of_voter_list": "મતદારયાદીની ઓળખ",
      "male_count_col": "પુરુષ",
      "female_count_col": "સ્ત્રી",
      "other_gender_count_col": "ત્રીજી જાતિ",
      "total_count_col": "કુલ"
    },
    "rows": [
      {
        "description_type": "મૂળ મતદાર યાદી - નવા સીમાંકન પ્રમાણેના મત",
        "male_count": "<Extract number>",
        "female_count": "<Extract number>",
        "other_gender_count": "<Extract number>",
        "total_count": "<Extract number>"
      },
      {
        "description_type": "ખાસ સંક્ષિપ્ત સુધારણા 2025 પુરવણી 1 ઘટક - 1 : વધારા યાદી",
        "male_count": "<Extract number>",
        "female_count": "<Extract number>",
        "other_gender_count": "<Extract number>",
        "total_count": "<Extract number>"
      },
      {
        "description_type": "પુરવણી 2 સતત સુધારણા 2025",
        "male_count": "<Extract number>",
        "female_count": "<Extract number>",
        "other_gender_count": "<Extract number>",
        "total_count": "<Extract number>"
      },
      {
        "description_type": "ખાસ સંક્ષિપ્ત સુધારણા 2025 પુરવણી 1 ઘટક - 2 : કમી યાદી",
        "male_count": "<Extract number>",
        "female_count": "<Extract number>",
        "other_gender_count": "<Extract number>",
        "total_count": "<Extract number>"
      },
      {
        "description_type": "પુરવણી 2 સતત સુધારણા 2025 (કમી યાદી)",
        "male_count": "<Extract number>",
        "female_count": "<Extract number>",
        "other_gender_count": "<Extract number>",
        "total_count": "<Extract number>"
      },
      {
        "description_type": "જાતિના કોલમમાં સુધારો કરવાથી થતો ફેરફાર",
        "male_count": "<Extract number (can be negative)>",
        "female_count": "<Extract number (can be negative)>",
        "other_gender_count": "<Extract number (can be negative)>",
        "total_count": "<Extract number (can be negative)>"
      },
      {
        "description_type": "આ સુધારણા પછી મતદાર યાદીમાં કુલ મતદારો (1+II-III+IV)",
        "male_count": "<Extract number>",
        "female_count": "<Extract number>",
        "other_gender_count": "<Extract number>",
        "total_count": "<Extract number>"
      }
    ]
  },
  "summary_revisions_section_B": {
    "title": "B) સુધારાઓની સંખ્યા",
    "table_header": {
      "type_of_roll": "મતદાર યાદીનો",
      "identity_of_roll": "મતદારયાદીની ઓળખ",
      "number_of_revisions": "સુધારાઓની સંખ્યા"
    },
    "rows": [
      {
        "type": "પુરવણી 1",
        "identity": "ખાસ સંક્ષિપ્ત સુધારણા 2025",
        "count": "<Extract number>"
      },
      {
        "type": "પુરવણી 2",
        "identity": "સતત સુધારણા 2025",
        "count": "<Extract number>"
      }
    ]
  },
  "signatory_details": "<Extract 'મતદાર નોંધણી અધિકારીની સહી'>",
  "disclaimer_text": "<Extract 'E2-Expired, S2-Shifted/ Change of Residence, R2-Duplicate, M2-Missing, Q2-Disqualified' and 'તા. 01-04-2025 ના રોજ ઉમર'>",
  "publication_note": "<Extract '# પુરવણીમાં સુધાર્યા મુજબ. પ્રસિદ્ધિની તારીખ :- :- 10-04-2025'>",
  "total_pages_in_pdf_extracted": "<Extract 'કુલ પૃષ્ઠો 43'> (extract only the number)",
  "current_page_number_extracted": "<Extract 'પૃષ્ઠ 43'> (extract only the number)"
}

Important Instructions:
- Ensure all extracted text is in Gujarati as it appears in the image, except for fixed string values for 'type', 'title', 'sub_title', and column headers.
- If a field is not present or cannot be confidently extracted, return an empty string for its value, but always include the key, including all keys within nested objects and arrays.
- For numerical values, extract them as plain integers. For values like '-1', extract them as integers.
- The output MUST be ONLY a JSON object.
- **ABSOLUTELY NO INTRODUCTORY OR CONCLUDING REMARKS, OR ANY ANY TEXT OUTSIDE THE JSON BLOCK.**
"""


# --- Helper Functions (from final_ocr.py - mostly unchanged, adjusted for unified flow) ---


def normalize_gender(gender_text):
    """Standardizes gender text to a single value."""
    if gender_text:
        if "પુર" in gender_text:  # Checks for the root 'Pur' in Purush
            return "પુરુષ"
        if "સ્ત્ર" in gender_text:  # Checks for the root 'Str' in Stree
            return "સ્ત્રી"
    return gender_text # Return original if no match

def update_status(status_file, status, message):
    """Writes the current job status to a JSON file."""
    try:
        with open(status_file, "w", encoding="utf-8") as f:
            json.dump({"status": status, "message": message}, f, ensure_ascii=False)
        logging.info(f"Status Updated: {status} - {message}")
    except Exception as e:
        logging.error(f"Could not write to status file {status_file}: {e}")


def convert_pdf_page_to_image(pdf_path, page_number):
    """Converts a single PDF page to a high-resolution PNG image."""
    try:
        doc = fitz.open(pdf_path)
        if page_number >= len(doc):
            return None
        page = doc.load_page(page_number)
        pix = page.get_pixmap(dpi=300)
        img_bytes = pix.tobytes("png")
        image = Image.open(io.BytesIO(img_bytes))
        doc.close()
        return image
    except Exception as e:
        logging.exception(
            f"Error converting PDF page {page_number} to image for {pdf_path}."
        )
        return None


async def process_image_with_gemini_async(semaphore, image, prompt, page_index):
    """Sends an image and prompt to the Gemini API asynchronously with retries."""
    model = genai.GenerativeModel(MODEL_NAME)
    retries = 5
    global daily_input_tokens_count, daily_output_tokens_count

    async with semaphore:
        for attempt in range(retries):
            try:
                img_byte_arr = io.BytesIO()
                image.save(img_byte_arr, format="PNG")
                gemini_image = {
                    "mime_type": "image/png",
                    "data": img_byte_arr.getvalue(),
                }

                response = await model.generate_content_async([prompt, gemini_image])

                if not response or not hasattr(response, "text") or not response.text:
                    logging.warning(
                        f"Empty response for page {page_index+1}, attempt {attempt+1}"
                    )
                    if attempt < retries - 1:
                        await asyncio.sleep(2**attempt)
                    continue

                if response.usage_metadata:
                    async with token_counter_lock:
                        daily_input_tokens_count += (
                            response.usage_metadata.prompt_token_count
                        )
                        daily_output_tokens_count += (
                            response.usage_metadata.candidates_token_count
                        )

                return response.text

            except Exception as e:
                logging.warning(f"API Error on page {page_index+1}, attempt {attempt+1}: {e}")
                logging.exception(f"Specific error during API call:", exc_info=True) # Log the full exception
                if attempt < retries - 1:
                    await asyncio.sleep(min(60, 2**attempt))  # Exponential backoff with a cap
                else:
                    return None  # Failed after all retries

    logging.error(f"Failed to get response for page {page_index+1} after {retries} retries.")
    return None


def parse_gemini_response(text_response, prompt_type, page_index):
    """Parses Gemini's text response into a Python object (dict or list of dicts)."""
    if not text_response:
        logging.warning(f"Cannot parse empty response for page {page_index+1}")
        return None

    cleaned_response = text_response.strip()

    if prompt_type in ["header_metadata", "footer_summary"]:
        try:
            if cleaned_response.startswith("```json"):
                cleaned_response = cleaned_response[len("```json") : -len("```")].strip()

            match = re.search(r"\{[\s\S]*\}", cleaned_response)
            if match:
                return json.loads(match.group(0))
            else:
                logging.error(f"No valid JSON object found in response for page {page_index+1}")
                return None
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error for page {page_index+1}: {e}\nResponse was:\n{cleaned_response}")
            return None
    else:  # Voter list page (JSONL)
        parsed_data = []
        for i, line in enumerate(cleaned_response.split("\n")):
            if not line.strip():
                continue
            try:
                parsed_data.append(json.loads(line.strip()))
            except json.JSONDecodeError:
                logging.warning(f"Page {page_index+1}, Line {i+1}: Could not parse JSONL line. Skipping.")
        return parsed_data


# --- SQLite Database Schema and Functions (Adapted from final_sql.py) ---

# SQLite doesn't have AUTO_INCREMENT directly, INTEGER PRIMARY KEY provides that.
# ENUMs are typically stored as TEXT in SQLite.
TABLES = {}
TABLES['pdfs'] = (
    "CREATE TABLE IF NOT EXISTS `pdfs` ("
    "   `id` INTEGER PRIMARY KEY AUTOINCREMENT,"
    "   `file_name` TEXT NOT NULL UNIQUE,"
    "   `assembly_constituency` TEXT,"
    "   `part_number` INTEGER,"
    "   `publication_date` TEXT,"  # Stored as TEXT (YYYY-MM-DD)
    "   `total_voters_count` INTEGER,"
    "   `processed_at` TEXT DEFAULT CURRENT_TIMESTAMP" # Stored as TEXT
    ")"
)
TABLES['sections'] = (
    "CREATE TABLE IF NOT EXISTS `sections` ("
    "   `id` INTEGER PRIMARY KEY AUTOINCREMENT,"
    "   `pdf_id` INTEGER NOT NULL,"
    "   `section_name` TEXT,"
    "   FOREIGN KEY (`pdf_id`) REFERENCES `pdfs`(`id`) ON DELETE CASCADE"
    ")"
)
TABLES['voters'] = (
    "CREATE TABLE IF NOT EXISTS `voters` ("
    "   `id` INTEGER PRIMARY KEY AUTOINCREMENT,"
    "   `section_id` INTEGER NOT NULL,"
    "   `idc_no` TEXT NOT NULL UNIQUE," # UNIQUE constraint for ID card number
    "   `VOTER_NAME` TEXT,"
    "   `RELATIVE_NAME` TEXT,"
    "   `rln_type` TEXT CHECK( rln_type IN ('F', 'H', 'M', 'O') )," # ENUM handled with CHECK constraint
    "   `house_no` TEXT,"
    "   `age` INTEGER,"
    "   `gender` TEXT,"
    "   `sl_no_in_pdf` INTEGER,"
    "   `box_no_on_page` INTEGER,"
    "   `page_no` INTEGER,"
    "  `all_text` TEXT,"
    "   `statustype` TEXT CHECK( statustype IN ('N', 'D', 'M') ) DEFAULT 'N'," # ENUM with CHECK
    "   FOREIGN KEY (`section_id`) REFERENCES `sections`(`id`) ON DELETE CASCADE"
    ")"
)
TABLES['summary_stats'] = (
    "CREATE TABLE IF NOT EXISTS `summary_stats` ("
    "   `id` INTEGER PRIMARY KEY AUTOINCREMENT,"
    "   `pdf_id` INTEGER NOT NULL,"
    "   `description` TEXT,"
    "   `male_count` INTEGER,"
    "   `female_count` INTEGER,"
    "   `other_gender_count` INTEGER,"
    "   `total_count` INTEGER,"
    "   FOREIGN KEY (`pdf_id`) REFERENCES `pdfs`(`id`) ON DELETE CASCADE"
    ")"
)

def create_connection(db_file):
    """Create a database connection to a SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = ON;") # Enable foreign key enforcement
        logging.info(f"Connected to SQLite database: {db_file}")
        return conn
    except Error as e:
        logging.error(f"Error connecting to SQLite database {db_file}: {e}")
    return conn

def create_tables(conn):
    """Create tables in the SQLite database."""
    cursor = conn.cursor()
    for table_name, table_description in TABLES.items():
        try:
            logging.info(f"Creating/Verifying table '{table_name}'...")
            cursor.execute(table_description)
            conn.commit()
            logging.info("OK")
        except Error as err:
            logging.error(f"Failed to create table '{table_name}': {err}")
    cursor.close()

def insert_pdf_data(conn, pdf_data, file_name):
    """Inserts PDF header and footer summary data into the database."""
    cursor = conn.cursor()
    pdf_id = None
    try:
        # Check if PDF already exists
        cursor.execute("SELECT id FROM pdfs WHERE file_name = ?", (file_name,))
        existing_pdf = cursor.fetchone()
        if existing_pdf:
            logging.warning(f"PDF '{file_name}' already exists in database. Skipping insertion.")
            return existing_pdf[0] # Return existing ID

        header_data = pdf_data.get('header_metadata', {})
        footer_data = pdf_data.get('footer_summary', {})

        assembly_const = header_data.get("assembly_constituency_number_name_estimated", "")
        part_num = header_data.get("part_number_top_right")
        pub_date_str = header_data.get("publication_date", "").replace('-', '/')
        # Convert DD-MM-YYYY or DD/MM/YYYY to YYYY-MM-DD for SQLite DATE compatibility
        pub_date = None
        if pub_date_str:
            try:
                # Attempt DD-MM-YYYY first
                pub_date = time.strftime('%Y-%m-%d', time.strptime(pub_date_str, '%d-%m-%Y'))
            except ValueError:
                try:
                    # Attempt DD/MM/YYYY
                    pub_date = time.strftime('%Y-%m-%d', time.strptime(pub_date_str, '%d/%m/%Y'))
                except ValueError:
                    logging.warning(f"Could not parse publication_date '{pub_date_str}' for {file_name}. Storing as NULL.")
                    pub_date = None

        total_voters = None
        if footer_data and footer_data.get("summary_voters_section_A"):
            rows = footer_data["summary_voters_section_A"].get("rows", [])
            if rows:
                total_voters = rows[-1].get("total_count")

        pdf_insert_query = ("INSERT INTO pdfs (file_name, assembly_constituency, part_number, publication_date, total_voters_count) VALUES (?, ?, ?, ?, ?)")
        pdf_values = (file_name, assembly_const, part_num, pub_date, total_voters)
        cursor.execute(pdf_insert_query, pdf_values)
        pdf_id = cursor.lastrowid
        logging.info(f"Added '{file_name}' to pdfs table with ID: {pdf_id}")

        if footer_data:
            summary_rows = footer_data.get("summary_voters_section_A", {}).get("rows", [])
            for row in summary_rows:
                stat_query = ("INSERT INTO summary_stats (pdf_id, description, male_count, female_count, other_gender_count, total_count) VALUES (?, ?, ?, ?, ?, ?)")
                stat_values = (pdf_id, row.get('description_type'), row.get('male_count'), row.get('female_count'), row.get('other_gender_count'), row.get('total_count'))
                cursor.execute(stat_query, stat_values)
            logging.info(f"Added {len(summary_rows)} rows to summary_stats for PDF ID {pdf_id}.")

        conn.commit()
        return pdf_id

    except Error as e:
        logging.error(f"Error inserting PDF data for {file_name}: {e}", exc_info=True)
        conn.rollback()
        return None
    finally:
        cursor.close()

def insert_sections(conn, pdf_id, header_data):
    """Inserts section data into the database and returns a cache of section_name to section_id."""
    cursor = conn.cursor()
    section_cache = {}
    try:
        if not header_data:
            logging.warning(f"No header data provided for PDF ID {pdf_id}. Cannot create sections.")
            return section_cache

        logging.info("Populating sections from header 'location_' keys...")
        for key, value in header_data.items():
            if key.startswith("location_") and value:
                section_name = value.strip()
                # Check if section already exists for this PDF
                cursor.execute("SELECT id FROM sections WHERE pdf_id = ? AND section_name = ?", (pdf_id, section_name))
                existing_section = cursor.fetchone()
                if not existing_section:
                    cursor.execute("INSERT INTO sections (pdf_id, section_name) VALUES (?, ?)", (pdf_id, section_name))
                    section_id = cursor.lastrowid
                    section_cache[section_name] = section_id
                else:
                    section_cache[section_name] = existing_section[0]

        conn.commit()
        logging.info(f"Created/updated {len(section_cache)} sections from header for PDF ID {pdf_id}.")
        return section_cache
    except Error as e:
        logging.error(f"Error inserting sections for PDF ID {pdf_id}: {e}", exc_info=True)
        conn.rollback()
        return {}
    finally:
        cursor.close()

import logging
from sqlite3 import Error
from thefuzz import fuzz

# In pipeline_processor.py

async def _do_nothing():
    """A placeholder coroutine that does nothing."""
    return None

def insert_voter_data(conn, pdf_id, all_voter_data, section_cache):
    """Inserts voter data into the database."""
    cursor = conn.cursor()
    voters_inserted_count = 0
    # Corrected the VALUES clause from ",?,?)" to ", ?, ?)"
    voter_query = ("INSERT OR IGNORE INTO voters (section_id, idc_no, voter_name, relative_name, rln_type, house_no, age, gender, sl_no_in_pdf, box_no_on_page, page_no, statustype, all_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    
    try:
        for page_data in all_voter_data:
            if not page_data or not page_data[0].get('PAGE_SECTION_NAME'):
                continue

            page_section_name = page_data[0]['PAGE_SECTION_NAME']

            best_match_id = None
            highest_score = -1
            
            if not section_cache:
                logging.warning(f"No sections available in cache for PDF ID {pdf_id}. Cannot match voters for section '{page_section_name}'.")
                continue

            # Fuzzy match the section name from the page to the ones from the header
            for header_section_name, header_section_id in section_cache.items():
                score = fuzz.partial_ratio(page_section_name, header_section_name)
                if score > highest_score:
                    highest_score = score
                    best_match_id = header_section_id

            section_id_to_use = best_match_id
            if section_id_to_use is None:
                logging.warning(f"Could not find a matching section for '{page_section_name}' from PDF ID {pdf_id}. Skipping voters for this section.")
                continue

            for record in page_data:
                if record.get('type') == 'voter' and record.get('IDCARD_NO'):
                    try:
                        age = int(record.get('AGE')) if record.get('AGE') is not None else None
                    except (ValueError, TypeError):
                        age = None
                        
                    cleaned_gender = normalize_gender(record.get('GENDER'))

                    # Corrected the order of values to match the SQL query
                    voter_values = (
                        section_id_to_use,
                        record.get('IDCARD_NO'),
                        record.get('VOTER_NAME'),
                        record.get('RELATIVE_NAME'),
                        record.get('RLN_TYPE', 'O'),
                        record.get('HOUSE_NO'),
                        age,
                        cleaned_gender,
                        record.get('SL_NO'),
                        record.get('BOX_NO_ON_PAGE'),
                        record.get('PAGE_NO'),
                        record.get('STATUSTYPE', 'N'), # statustype is 12th
                        record.get('ALL_TXT')          # all_text is 13th
                    )
                    cursor.execute(voter_query, voter_values)
                    voters_inserted_count += cursor.rowcount

        conn.commit()
        logging.info(f"Inserted {voters_inserted_count} new voter records into SQLite.")

    except Error as e:
        logging.error(f"Error inserting voter data for PDF ID {pdf_id}: {e}", exc_info=True)
        conn.rollback()
    finally:
        cursor.close()


import os
import asyncio
import logging
import fitz # PyMuPDF

# Assuming all other necessary functions (prompts, db functions, etc.) are defined elsewhere in the file.

async def process_single_pdf_and_store_data_async(pdf_path, status_callback, db_connection):
    """
    Processes a single PDF file (OCR) and then stores data into the SQLite DB,
    reporting progress via the status_callback function.
    """
    pdf_file_name = os.path.basename(pdf_path)
    logging.info(f"Starting data extraction for {pdf_file_name}...")
    status_callback("processing", f"Opening {pdf_file_name}...")

    # --- 1. Open PDF and get page count (done only once) ---
    try:
        doc = fitz.open(pdf_path)
        num_pages = len(doc)
        doc.close()
    except Exception as e:
        logging.error(f"Could not open or read PDF file {pdf_path}: {e}")
        status_callback("error", f"Could not open PDF {pdf_file_name}: {e}")
        return

    # --- 2. Asynchronously process all pages ---
    all_parsed_data_for_pdf = {}
    page_prompts = ["header_metadata" if i == 0 else ("footer_summary" if i == num_pages - 1 else "voter_list_page") for i in range(num_pages)]
    
    # Assuming MAX_CONCURRENT_REQUESTS is defined globally
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = []

    for i in range(num_pages):
         # --- ADD THIS CHECK TO IGNORE PAGE 2 ---
        if i == 1:  # Page index 1 is the second page.
            logging.info(f"Intentionally skipping page 2 (index 1) for {pdf_file_name}.")
            tasks.append(_do_nothing())
            continue
        
        page_image = convert_pdf_page_to_image(pdf_path, i)
        if not page_image:
            logging.error(f"Failed to convert page {i+1} to image for {pdf_path}")
            continue

        prompt_type = page_prompts[i]
        current_prompt = ""
        if prompt_type == "header_metadata":
            current_prompt = HEADER_PAGE_PROMPT
        elif prompt_type == "footer_summary":
            current_prompt = FOOTER_PAGE_PROMPT
        else:
            current_prompt = VOTER_LIST_PAGE_PROMPT
        
        tasks.append(process_image_with_gemini_async(semaphore, page_image, current_prompt, i))

    status_callback("processing", f"Extracting data from pages(skipping page 2)...")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    logging.info(f"All API calls for {pdf_file_name} completed.")

    # --- 3. Parse results ---
    for task_idx, result in enumerate(results):
        if isinstance(result, Exception) or result is None:
            logging.error(f"Failed to get a valid result for {pdf_file_name}, page {task_idx + 1}. Error: {result}")
            continue
        
        prompt_type = page_prompts[task_idx]
        parsed_data = parse_gemini_response(result, prompt_type, task_idx)

        if parsed_data:
            common_metadata = {
                "pdf_page_index_script": task_idx,
                "pdf_total_pages_script": num_pages,
                "pdf_file_name_script": pdf_file_name,
            }
            if isinstance(parsed_data, dict):
                parsed_data.update(common_metadata)
            elif isinstance(parsed_data, list):
                for item in parsed_data:
                    item.update(common_metadata)
            all_parsed_data_for_pdf[task_idx] = parsed_data

    # --- 4. Insert data into the database ---
    status_callback("processing", f"Saving extracted data for {pdf_file_name} to database...")
    header_data = all_parsed_data_for_pdf.get(0)
    footer_data = all_parsed_data_for_pdf.get(num_pages - 1)
    voter_list_pages_data = [data for i, data in all_parsed_data_for_pdf.items() if i != 0 and i != num_pages - 1]

    if not header_data:
        logging.error(f"Missing header data for '{pdf_file_name}'. Cannot proceed.")
        status_callback("error", f"Missing header data for {pdf_file_name}")
        return

    # 1. Insert PDF record
    pdf_id = insert_pdf_data(db_connection, {'header_metadata': header_data, 'footer_summary': footer_data}, pdf_file_name)
    if pdf_id is None:
        # This can happen if the PDF already exists, which isn't a hard error.
        # We'll log it and stop processing this file.
        logging.warning(f"PDF record for {pdf_file_name} not inserted (may already exist). Skipping further DB operations for this file.")
        status_callback("processing", f"{pdf_file_name} already exists in database. Skipped.")
        return

    # 2. Insert sections
    section_cache = insert_sections(db_connection, pdf_id, header_data)

    # 3. Insert voter data
    insert_voter_data(db_connection, pdf_id, voter_list_pages_data, section_cache)

    logging.info(f"--- Finished processing and importing data for {pdf_file_name} ---")


