# Voter List Data Extraction and Analytics Web App

This project is a web-based application designed to process **Gujarati voter list PDFs**, extract detailed voter information using the **Google Gemini 1.5 Flash** model, and store the structured data in a **SQLite** database.  
The application features a web interface for uploading files, monitoring processing status, and a dashboard for visualizing the extracted data.

---

## 🚀 Features

- **📂 PDF Upload:** Upload one or more voter list PDF files through a simple web interface.  
- **🤖 AI-Powered Data Extraction:** Utilizes the **Google Gemini 1.5 Flash** vision model to perform OCR and extract structured data from each page.  
- **⚡ Asynchronous Processing:** Handles PDF processing in the background, allowing for a non-blocking user experience.  
- **🗄️ Structured Database:** Stores extracted information (PDF metadata, polling sections, voter details, and summary stats) in a local **SQLite** database.  
- **📥 Data Export:** Download the entire database as a `.zip` file containing `.csv` files for each table.  
- **📊 Analytics Dashboard:** An interactive dashboard to visualize voter demographics (gender, age groups) for each processed PDF and polling section.

---

## 🛠 Technology Stack

**Backend:** Python, Flask, Waitress  
**AI Model:** Google Gemini 1.5 Flash  
**Database:** SQLite  
**Libraries:**  
- PyMuPDF – PDF handling  
- Pandas – Data manipulation  
- TheFuzz – String matching  
**Frontend:** HTML, CSS, JavaScript (with Chart.js for visualizations)

---

## 📋 Prerequisites

Before you begin, ensure you have the following installed on your system:

- Python **3.8** or higher  
- Git

---

## ⚙️ Setup and Installation

### 1️⃣ Clone the Repository
Open a terminal or command prompt and run:
```
git clone <your-repository-url>
cd <your-repository-folder>
```

### 2️⃣ Create and Activate a Virtual Environment
It’s recommended to use a virtual environment to manage dependencies.

**Windows:**
```
python -m venv venv
.
env\Scripts ctivate
```

**macOS / Linux:**
```
python3 -m venv venv
source venv/bin/activate
```

### 3️⃣ Install Dependencies
Run:
```
pip install -r requirements.txt
```

### 4️⃣ Create the Environment File
The app requires a **Google Gemini API key**.

Create a file named `.env` in your project’s root folder and add:
```
GEMINI_API_KEY="YOUR_GOOGLE_AI_API_KEY_HERE"
```
**Note:** `.env` is included in `.gitignore` so it will not be pushed to GitHub.

---

## ▶️ Running the Application

Start the web application:
```
python webapp.py
```

You should see output indicating the app is running, typically at:
```
http://127.0.0.1:8080
```
Open this link in your web browser. The browser may also open automatically.

---

## 📖 How to Use

1. **Upload PDFs:** On the main page, click **"Choose Files"** and select one or more voter list PDF files.  
2. **Start Processing:** Click **"Upload and Process"** to start. You’ll be taken to a status page with a **Job ID**.  
3. **Monitor Status:** The status page refreshes automatically to show processing progress.  
4. **View Dashboard:** When processing finishes, open the **Dashboard** to see voter demographics and statistics.  
5. **Download Data:** Click **"Download Full Database as CSV"** to get all extracted data in `.csv` format inside a `.zip` file.

---

## 📜 License
This project is licensed under the **MIT License** – feel free to use and modify it.

---
