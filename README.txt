========================================
  VoterApp - Source Code Documentation
========================================

This document provides a detailed technical overview of the VoterApp source code. For user-facing instructions, please see `readme.txt`.


1. ARCHITECTURAL OVERVIEW
---------------------------
VoterApp is a multi-threaded web application packaged as a single desktop executable. It is designed to perform long-running AI-based OCR tasks without freezing the user interface.

- CORE TECHNOLOGIES:
  - WEB FRAMEWORK: FLASK is used as a lightweight backend to serve the UI and handle requests.
  - WEB SERVER: WAITRESS is used as a production-ready WSGI server, which is more robust than Flask's default development server.
  - CONCURRENCY:
    - THREADING: The main web server runs on one thread, and each file processing job is spawned in a new background thread. This ensures the UI remains responsive.
    - ASYNCIO: Within the background thread, ASYNCIO is used to process the pages of a single PDF concurrently, making many parallel calls to the Gemini API to speed up extraction.
  - PDF & AI:
    - PYMUPDF (fitz): Used to render PDF pages into high-resolution images.
    - GOOGLE GEMINI API: The core AI service used for OCR and structured data extraction from the page images.
  - DATABASE: SQLITE provides a simple, file-based, serverless database, perfect for a portable desktop application.
  - PACKAGING: PYINSTALLER bundles the Python interpreter, all code, and dependencies into a single standalone executable.


2. FILE-BY-FILE BREAKDOWN
---------------------------

webapp.py - The Web Server & Controller
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PURPOSE: This is the main entry point of the application. It handles all web requests, manages the lifecycle of background processing jobs, and serves the frontend UI.

KEY COMPONENTS:
  - GLOBAL VARIABLES:
    - JOBS = {}: A dictionary that acts as a simple, in-memory job store. The keys are unique `job_id`s, and the values are dictionaries containing the job's `status` and `message`.
    - JOBS_LOCK = threading.Lock(): A lock to ensure that the `JOBS` dictionary is accessed in a thread-safe manner, preventing race conditions.

  - background_task_runner(job_id, pdf_paths):
    - This function is the target for the background `threading.Thread`. It acts as the bridge between the synchronous web world and the asynchronous processing world.
    - It creates a new ASYNCIO event loop for the thread.
    - It defines the `update_status_for_job` callback function, which safely updates the shared `JOBS` dictionary.
    - It iterates through the list of PDF file paths and calls the main processing function from the other module for each one.

  - FLASK ROUTES (@app.route(...)):
    - GET /: Serves the `index.html` file to the user.
    - POST /upload: Receives the uploaded PDF files, saves them, creates a unique `job_id`, starts the background thread, and immediately returns the `job_id`.
    - GET /status/<job_id>: This endpoint is polled by the frontend. It safely reads the current status for the given `job_id` from the `JOBS` dictionary and returns it as JSON.


pipeline_processor.py - The Core Processing Engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PURPOSE: This module contains all the business logic for OCR and database interaction. It is completely decoupled from the web server; it has no knowledge of Flask, threads, or web requests.

KEY COMPONENTS:
  - CONSTANTS: Contains global constants like the Gemini API `PROMPTS`, the `TABLES` dictionary defining the database schema, and other configuration values.

  - DATABASE FUNCTIONS (create_connection, create_tables, insert_*):
    - A collection of functions responsible for all `sqlite3` operations.
    - The schema uses `ON DELETE CASCADE` to ensure that when a PDF record is deleted, all its associated data is also removed.

  - process_image_with_gemini_async(...):
    - The sole function responsible for communicating with the Google Gemini API. It handles the asynchronous API call and includes a retry mechanism.

  - process_single_pdf_and_store_data_async(...):
    - The main orchestrator for processing a single PDF file.
    - It is an `async` function that uses `asyncio.gather` to process all pages of the PDF concurrently.
    - It accepts a `status_callback` function as an argument to report its progress, making the module reusable and independent of the UI.


delete_pdf.py - Database Management Utility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PURPOSE: A standalone command-line script for database maintenance. It is not part of the main web application.

FUNCTIONALITY: It allows a user to safely delete a PDF and all its associated data from the database. It connects to the database, lists the currently processed PDFs, and prompts the user to enter the exact filename of the one to delete.


3. DATA FLOW FOR A SINGLE JOB
-------------------------------
1. A user selects files in the browser and clicks "Upload" in `index.html`.
2. `fetch` sends the files to the `/upload` endpoint in `webapp.py`.
3. `webapp.py` saves the files, creates a `job_id`, and starts the `background_task_runner` function in a new thread. It immediately returns the `job_id`.
4. The browser receives the `job_id` and begins polling the `/status/<job_id>` endpoint every 2 seconds.
5. The `background_task_runner` thread calls `pipeline_processor.process_single_pdf_and_store_data_async`, passing it a PDF path and a `status_callback` function.
6. `pipeline_processor` uses `asyncio.gather` to concurrently send page images to the Gemini API.
7. Throughout the process, `pipeline_processor` calls the `status_callback` with progress messages (e.g., "Processing PDF 1/5...").
8. The `status_callback` in `webapp.py` updates the shared `JOBS` dictionary with the new message.
9. The browser's next poll to `/status/<job_id>` reads the updated message from the `JOBS` dictionary and displays it.
10. `pipeline_processor` finishes processing and inserts the final data into the SQLite database.
11. The background thread finishes, setting the final status to "complete", which the browser picks up on its next poll.


4. KEY DESIGN DECISIONS
-------------------------
- WHY THREADING + ASYNCIO?: THREADING is used to prevent the long OCR task from blocking the web server (so the UI remains responsive). ASYNCIO is used *within* the thread to perform network-bound API calls for each page concurrently, which speeds up the processing of each individual PDF.

- WHY A STATUS_CALLBACK?: This design decouples the processing logic from the web server. The processing module can report its status without needing to know *how* that status is being displayed or stored.

- WHY SQLITE?: It provides a portable, serverless, single-file database that requires no setup, which is ideal for a standalone desktop application.

- WHY WAITRESS?: It's a production-ready WSGI server that is more robust and performant than Flask's built-in development server and is easy to bundle with PyInstaller.