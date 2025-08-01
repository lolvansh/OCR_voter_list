import os
import uuid
import threading
import webbrowser
import logging
import asyncio
import io
import zipfile
import sqlite3
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
from waitress import serve
import sys

# Import your entire processing logic from the other file
import pipeline_processor

# --- Windows Event Loop Policy (Crucial for stability) ---
# +++ ADD THIS BLOCK +++
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    
# --- Basic Configuration & Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Flask App Initialization ---
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploaded_pdfs'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# --- Global Job Store (Thread-Safe) ---
JOBS = {}
JOBS_LOCK = threading.Lock()

def background_task_runner(job_id, pdf_paths):
    """The wrapper function that runs the processing in a separate thread."""
    logging.info(f"Job {job_id}: Starting background processing for {len(pdf_paths)} files.")

    def update_status_for_job(status, message):
        """A helper to update the global JOBS dictionary safely."""
        with JOBS_LOCK:
            JOBS[job_id]['status'] = status
            JOBS[job_id]['message'] = message
        logging.info(f"Job {job_id} status: {status} - {message}")

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)


        db_path = "voter_data.db"
        
        conn = pipeline_processor.create_connection(db_path)
        if conn is None:
            raise Exception("Failed to create database connection.")
        pipeline_processor.create_tables(conn)

        total_pdfs = len(pdf_paths)
        for i, pdf_path in enumerate(pdf_paths):
            pdf_name = os.path.basename(pdf_path)
            update_status_for_job("processing", f"Processing PDF {i+1}/{total_pdfs}: {pdf_name}")
            
            # Call the processing function from the imported module
            loop.run_until_complete(
                pipeline_processor.process_single_pdf_and_store_data_async(pdf_path, update_status_for_job, conn)
            )
        
        conn.close()
        update_status_for_job("complete", f"Successfully processed {total_pdfs} files.")

    except Exception as e:
        logging.error(f"Job {job_id}: Error in background task", exc_info=True)
        update_status_for_job("error", f"An error occurred: {e}")
    finally:
        for pdf_path in pdf_paths:
            try:
                os.remove(pdf_path)
            except OSError as e:
                logging.warning(f"Could not remove temp file {pdf_path}: {e}")

# --- Flask Web Routes ---
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    files = request.files.getlist('file')
    if not files or files[0].filename == '':
        return jsonify({"error": "No files selected"}), 400

    saved_paths = []
    # A small correction to ensure only PDFs are processed and saved
    for f in files:
        if f and f.filename.lower().endswith('.pdf'):
            path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(f.filename))
            f.save(path)
            saved_paths.append(path)

    if not saved_paths:
        return jsonify({"error": "No valid PDF files uploaded"}), 400

    job_id = str(uuid.uuid4())
    with JOBS_LOCK:
        JOBS[job_id] = {"status": "queued", "message": "Files queued for processing..."}

    thread = threading.Thread(target=background_task_runner, args=(job_id, saved_paths))
    thread.start()
    return jsonify({"job_id": job_id})

@app.route('/status/<job_id>')
def get_status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id, {"status": "error", "message": "Job ID not found."})
        return jsonify(job)

@app.route('/download_csv')
def download_csv():
    """Reads data from the SQLite DB, creates CSVs for each table, zips them, and sends for download."""
    db_dir = os.path.join(os.path.expanduser("~"), "Documents", "VoterAppData")
    db_path = os.path.join(db_dir, "voter_data.db")
    
    if not os.path.exists(db_path):
        return jsonify({"error": "Database file not found. Please process at least one PDF first."}), 404

    try:
        conn = sqlite3.connect(db_path)
        tables = ['pdfs', 'sections', 'voters', 'summary_stats']
        
        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            for table_name in tables:
                logging.info(f"Exporting table '{table_name}' to CSV.")
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                csv_data = df.to_csv(index=False)
                zf.writestr(f"{table_name}.csv", csv_data)
        
        conn.close()
        memory_file.seek(0)
        
        return send_file(
            memory_file,
            download_name='voter_data_export.zip',
            as_attachment=True,
            mimetype='application/zip'
        )

    except Exception as e:
        logging.error(f"Error creating CSV export: {e}", exc_info=True)
        return jsonify({"error": f"Failed to generate CSV export: {e}"}), 500


# --- ADD THIS NEW ROUTE FOR ANALYTICS ---

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

# In webapp.py, add these two new routes


@app.route('/api/pdfs')
def get_all_pdfs():
    """Returns a list of all processed PDFs with their IDs and names."""
    db_path = "voter_data.db"
    if not os.path.exists(db_path):
        return jsonify({"error": "Database not found."}), 404
        
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT id, file_name FROM pdfs ORDER BY id", conn)
        conn.close()
        
        # Convert the DataFrame to a list of dictionaries (which becomes JSON)
        return jsonify(df.to_dict(orient='records'))

    except Exception as e:
        logging.error(f"Error fetching PDF list: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    
    

@app.route('/api/sections/<int:pdf_id>')
def get_sections_for_pdf(pdf_id):
    """Returns a list of sections for a given PDF ID."""
    db_path = "voter_data.db"
    if not os.path.exists(db_path):
        return jsonify({"error": "Database not found."}), 404
        
    try:
        conn = sqlite3.connect(db_path)
        query = "SELECT id, section_name FROM sections WHERE pdf_id = ? ORDER BY section_name"
        df = pd.read_sql_query(query, conn, params=(pdf_id,))
        conn.close()
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        logging.error(f"Error fetching sections for pdf_id {pdf_id}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/analytics/section/<int:section_id>')
def get_analytics_for_section(section_id):
    """Returns analytics data (gender, age) for a specific section ID."""
    db_path = "voter_data.db"
    if not os.path.exists(db_path):
        return jsonify({"error": "Database not found."}), 404
        
    try:
        conn = sqlite3.connect(db_path)
        
        # Gender data query
        gender_query = "SELECT gender, COUNT(*) as count FROM voters WHERE section_id = ? GROUP BY gender"
        gender_df = pd.read_sql_query(gender_query, conn, params=(section_id,))
        
        # Age distribution query
        age_query = """
        SELECT
            CASE
                WHEN age BETWEEN 18 AND 29 THEN '18-29'
                WHEN age BETWEEN 30 AND 39 THEN '30-39'
                WHEN age BETWEEN 40 AND 49 THEN '40-49'
                WHEN age BETWEEN 50 AND 59 THEN '50-59'
                ELSE '60+'
            END as age_group,
            COUNT(*) as count
        FROM voters
        WHERE section_id = ? AND age IS NOT NULL
        GROUP BY age_group
        ORDER BY age_group
        """
        age_df = pd.read_sql_query(age_query, conn, params=(section_id,))
        
        conn.close()

        # Combine all analytics into one JSON response
        response_data = {
            "gender_data": {
                "labels": gender_df['gender'].tolist(),
                "data": gender_df['count'].tolist()
            },
            "age_data": {
                "labels": age_df['age_group'].tolist(),
                "data": age_df['count'].tolist()
            }
        }
        return jsonify(response_data)
        
    except Exception as e:
        logging.error(f"Error fetching analytics for section_id {section_id}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/analytics/pdf/<int:pdf_id>')
def get_analytics_for_pdf(pdf_id):
    """Returns analytics data (gender, age) for an entire PDF ID."""
    db_path = "voter_data.db"
    if not os.path.exists(db_path):
        return jsonify({"error": "Database not found."}), 404
        
    try:
        conn = sqlite3.connect(db_path)
        
        # Gender data query for the whole PDF
        gender_query = """
        SELECT v.gender, COUNT(*) as count
        FROM voters v JOIN sections s ON v.section_id = s.id
        WHERE s.pdf_id = ?
        GROUP BY v.gender
        """
        gender_df = pd.read_sql_query(gender_query, conn, params=(pdf_id,))
        
        # Age distribution query for the whole PDF
        age_query = """
        SELECT
            CASE
                WHEN age BETWEEN 18 AND 29 THEN '18-29'
                WHEN age BETWEEN 30 AND 39 THEN '30-39'
                WHEN age BETWEEN 40 AND 49 THEN '40-49'
                WHEN age BETWEEN 50 AND 59 THEN '50-59'
                ELSE '60+'
            END as age_group,
            COUNT(*) as count
        FROM voters v JOIN sections s ON v.section_id = s.id
        WHERE s.pdf_id = ? AND age IS NOT NULL
        GROUP BY age_group ORDER BY age_group
        """
        age_df = pd.read_sql_query(age_query, conn, params=(pdf_id,))
        
        conn.close()

        response_data = {
            "gender_data": { "labels": gender_df['gender'].tolist(), "data": gender_df['count'].tolist() },
            "age_data": { "labels": age_df['age_group'].tolist(), "data": age_df['count'].tolist() }
        }
        return jsonify(response_data)
        
    except Exception as e:
        logging.error(f"Error fetching analytics for pdf_id {pdf_id}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# --- Main Entry Point ---
if __name__ == '__main__':
        # --- ADD THIS INITIALIZATION BLOCK ---
    # This will run once when the server starts.
    db_path = "voter_data.db"
    logging.info("Initializing database...")
    conn = pipeline_processor.create_connection(db_path)
    if conn is not None:
        pipeline_processor.create_tables(conn)
        conn.close()
        logging.info("Database is ready.")
    else:
        logging.error("FATAL: Could not connect to or create the database. Exiting.")
        # We should exit if the database can't be set up.
        exit()
    webbrowser.open_new("http://127.0.0.1:8080")
    serve(app, host="127.0.0.1", port=8080)