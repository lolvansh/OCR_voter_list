import os
import sys
import json
import logging
import asyncio
import io
from glob import glob
import sqlite3
from sqlite3 import Error
from thefuzz import fuzz

import fitz  # PyMuPDF
import google.generativeai as genai
from PIL import Image
from dotenv import load_dotenv

# --- Basic Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- SETUP ---
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY not found in .env file.")
genai.configure(api_key=api_key)

# --- CONFIG: PLEASE CHANGE THESE TWO VALUES ---
# IMPORTANT: Update this path to your PDF file.
# For the example file you provided, you would use "2_pdf.pdf".
PDF_PATH = r"C:\Users\Vansh\Desktop\2_pdf.pdf"
# Page 9 in the document is index 8, page 10 is index 9, and so on.
PAGE_TO_TEST = 0 # This corresponds to 'પૃષ્ઠ 10' in your PDF [cite: 194]

# --- MODIFIED PROMPT (More robust for image chunks) ---
VOTER_LIST_PAGE_PROMPT = """
# PROMPT_VERSION: 2025-07-19_V3_JSONL - Voter List Page Prompt (Single JSON)

You are an expert at extracting structured information from voter list PDF images.
Perform OCR on this image. Then, from the OCR'd text all individual voter objects.

# MANDATORY EXTRACTION PLAN:
You MUST follow this plan precisely.
1. **Analyze Layout:** First, perform a full analysis of the entire image. Acknowledge that the page is structured as a grid of voter boxes, arranged in rows and columns.
2. **Row-by-Row Processing:** You MUST process the data row by row. Start with the top row and extract the voter from the left column, then the middle column, then the right column. Once the first row is complete, move down to the second row and repeat the left-to-right process. Continue this for all rows **visible in the image** until the image is finished.
3. **Final Check:** Before concluding, perform a final visual sweep of the entire grid to ensure no voter boxes in any row or column have been missed.
4. **Generate Output:** Produce the final output as a SINGLE, valid JSON object. The root of this object MUST be a key named "voters" which contains a list of individual voter JSON objects. Your response must contain ONLY this single JSON object.

**Overall Page Instruction:**
First, identify the 'વિભાગ નામ' at the top of the page and extract only the name of the area.
**identify the 'PAGE_NO' by looking for the number next to the word 'પૃષ્ઠ' (page), which is usually at the bottom of the image. **Do NOT confuse this with the 'ભાગ નંબર' (Part Number).**
Remember both the section name and page number.
If either is not present, consider it to be an empty string.

For each 'voter' object (one per line):
- 'type': "voter"
- 'json_schema_version': "2025-07-19_V3_JSONL"

- 'STATUSTYPE': "**CRITICAL STATUS FIELD:** Based on your Priority 1 Status Check, set this value.
    - If the 'DELETED' watermark is present, you MUST set this to 'D'.
    - If a '#' symbol appears before the voter's serial number (SL_NO), you MUST set this to 'M'.
    - If neither of the above conditions is met, you MUST set this to 'N'.
    This field cannot be left empty."
- 'SL_NO': Extract the serial number shown next to the voter's details box (e.g., "1").
- 'VOTER_NAME': Locate the 'મતદાનું નામ:' field. Extract **ALL text** that follows 'મતદાનું નામ:' up until the next distinct field label.
- 'RELATIVE_NAME': Locate the 'પિતાનું નામ:' or 'પતિનું નામ:' field. Extract **ALL text** that follows this relation label up until the next distinct field label.
- 'HOUSE_NO': Extract the house number.
- 'AGE': Extract the age as a number.
- 'GENDER': Extract the gender.
- 'IDCARD_NO': Extract the Voter ID.
- 'RLN_TYPE': "Identify if RELATIVE_NAME field is 'પતિનું નામ:'= H, 'પિતાનું નામ:'= F, 'માતાનું નામ'= M, 'અન્ય'= O"
- 'ALL_TXT': Extract all raw text content within this specific voter's detail box.
- 'BOX_NO_ON_PAGE': The sequential number of the voter on this specific page (1-30).
- 'PAGE_SECTION_NAME': "Use the section name you identified at the start of the process."
- 'PAGE_NO': "Use the page number you identified at the start of the process."
"""

async def process_image_chunk(image_chunk, model, prompt, part_name):
    """Sends a single image chunk to the API and returns the parsed voter list."""
    logging.info(f"Sending {part_name} to Gemini API...")
    generation_config = genai.types.GenerationConfig(max_output_tokens=8192)

    try:
        response = await model.generate_content_async(
            [prompt, image_chunk],
            generation_config=generation_config
        )
        logging.info(f"Received response from API for {part_name}.")

        # Clean the response text to ensure it's valid JSON
        response_text = response.text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]

        data = json.loads(response_text)
        voters = data.get('voters', [])
        logging.info(f"SUCCESS: Parsed {len(voters)} voters from {part_name}.")
        return voters
    except json.JSONDecodeError as e:
        logging.error(f"FAILURE: Could not parse JSON for {part_name}. Error: {e}")
        logging.error(f"--- RAW RESPONSE for {part_name} ---\n{response.text}\n--- END RAW RESPONSE ---")
        return []
    except Exception as e:
        logging.error(f"An unexpected API error occurred for {part_name}: {e}", exc_info=True)
        return []


async def main():
    logging.info(f"--- Starting Split-Page Test for {PDF_PATH}, Page Index {PAGE_TO_TEST} ---")
    try:
        # 1. Convert page to a full image
        doc = fitz.open(PDF_PATH)
        if PAGE_TO_TEST >= len(doc):
            logging.error(f"Page index {PAGE_TO_TEST} is out of bounds. PDF has {len(doc)} pages.")
            return
        page = doc.load_page(PAGE_TO_TEST)
        pix = page.get_pixmap(dpi=300)
        full_image = Image.open(io.BytesIO(pix.tobytes("png")))
        doc.close()
        logging.info("Step 1: Successfully converted PDF page to a full image.")

        # 2. Split the image into top and bottom halves
        width, height = full_image.size
        midpoint = height // 2
        top_half = full_image.crop((0, 0, width, midpoint))
        bottom_half = full_image.crop((0, midpoint, width, height))

        image_chunks = [
            {"image": top_half, "name": "top half"},
            {"image": bottom_half, "name": "bottom half"}
        ]
        logging.info("Step 2: Successfully split the image into two halves.")

        # 3. Process each half and collect results
        model = genai.GenerativeModel("gemini-1.5-flash")
        all_voters = []

        for chunk in image_chunks:
            voters_from_chunk = await process_image_chunk(
                chunk["image"], model, VOTER_LIST_PAGE_PROMPT, chunk["name"]
            )
            all_voters.extend(voters_from_chunk)

        # 4. Post-processing: Merge, re-number, and standardize results
        if not all_voters:
            logging.warning("No voters were extracted from either half of the page.")
            final_json_output = {"voters": []}
        else:
            logging.info("Step 4: Merging results and correcting data...")
            page_no = ""
            section_name = ""
            
            # Find the first available page number and section name to use as a standard
            for voter in all_voters:
                if not page_no and voter.get('PAGE_NO'):
                    page_no = voter.get('PAGE_NO')
                if not section_name and voter.get('PAGE_SECTION_NAME'):
                    section_name = voter.get('PAGE_SECTION_NAME')
                if page_no and section_name:
                    break

            # Renumber and standardize all entries
            for i, voter in enumerate(all_voters):
                voter['BOX_NO_ON_PAGE'] = i + 1
                voter['PAGE_NO'] = page_no
                voter['PAGE_SECTION_NAME'] = section_name

            final_json_output = {"voters": all_voters}
            logging.info(f"SUCCESS: Final processing complete. Total voters found: {len(all_voters)}.")

        # 5. Print the final combined and corrected JSON
        print("\n" + "="*25 + " FINAL COMBINED JSON " + "="*25)
        print(json.dumps(final_json_output, indent=2, ensure_ascii=False))
        print("="*23 + " END OF FINAL JSON " + "="*23 + "\n")

    except FileNotFoundError:
        logging.error(f"CRITICAL ERROR: The file was not found at '{PDF_PATH}'. Please check the path.")
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())