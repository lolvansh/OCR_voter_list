Voter List Data Extraction and Analytics Web App
================================================

This project is a web-based application designed to process Gujarati voter list PDFs, extract detailed voter information using the Google Gemini 1.5 Flash model, and store the structured data in a SQLite database. The application features a web interface for uploading files, monitoring processing status, and a dashboard for visualizing the extracted data.

Features
--------

-   **PDF Upload:** Upload one or more voter list PDF files through a simple web interface.

-   **AI-Powered Data Extraction:** Utilizes the Google Gemini 1.5 Flash vision model to perform OCR and extract structured data from each page.

-   **Asynchronous Processing:** Handles PDF processing in the background, allowing for a non-blocking user experience.

-   **Structured Database:** Stores all extracted information (PDF metadata, polling sections, voter details, and summary stats) in a local SQLite database.

-   **Data Export:** Download the entire database as a `.zip` file containing `.csv` files for each table.

-   **Analytics Dashboard:** An interactive web dashboard to visualize voter demographics (gender, age groups) for each processed PDF and polling section.

Technology Stack
----------------

-   **Backend:** Python, Flask, Waitress (as a production-ready server)

-   **AI Model:** Google Gemini 1.5 Flash

-   **Database:** SQLite

-   **Libraries:** PyMuPDF (for PDF handling), Pandas (for data manipulation), TheFuzz (for string matching)

-   **Frontend:** HTML, CSS, JavaScript (with Chart.js for visualizations)

Prerequisites
-------------

Before you begin, ensure you have the following installed on your system:

-   Python 3.8 or higher

-   Git

Setup and Installation
----------------------

Follow these steps to get your local development environment running.

**1\. Clone the Repository**

```
git clone <your-repository-url>
cd <your-repository-folder>

```

**2\. Create and Activate a Virtual Environment** It's highly recommended to use a virtual environment to manage project dependencies.

-   **Windows:**

    ```
    python -m venv venv
    .\venv\Scripts\activate

    ```

-   **macOS / Linux:**

    ```
    python3 -m venv venv
    source venv/bin/activate

    ```

**3\. Install Dependencies** Install all the required Python libraries from the `requirements.txt` file.

```
pip install -r requirements.txt

```

**4\. Create the Environment File** This project requires an API key for the Google Gemini model.

-   Create a file named `.env` in the root of your project directory.

-   Add your API key to this file as follows:

    ```
    GEMINI_API_KEY="YOUR_GOOGLE_AI_API_KEY_HERE"

    ```

    **Note:** The `.env` file is listed in `.gitignore` and should never be committed to your repository.

Running the Application
-----------------------

Once the setup is complete, you can start the web application.

1.  **Run the Web App:**

    ```
    python webapp.py

    ```

2.  **Access the Application:** The terminal will show that the server is running, typically on `http://127.0.0.1:8080`. Your web browser should automatically open to this address.

How to Use
----------

1.  **Upload PDFs:** On the main page, click the "Choose Files" button and select the voter list PDFs you want to process.

2.  **Start Processing:** Click the "Upload and Process" button. You will be redirected to a status page with a unique Job ID.

3.  **Monitor Status:** The status page will automatically update, showing the progress of the data extraction.

4.  **View Dashboard:** Once the job is complete, navigate to the "Dashboard" to view analytics for the processed files.

5.  **Download Data:** Use the "Download Full Database as CSV" button to get a `.zip` file of the extracted data.