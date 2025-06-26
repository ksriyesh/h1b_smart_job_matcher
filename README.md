Smart H-1B Job Matcher
This project helps international students identify job listings most likely to offer H-1B visa sponsorship, using real-time job scraping, PySpark pipelines, and AI-powered matching agents.

🧠 Overview
International students often struggle to find companies that sponsor H-1B visas. This tool scrapes real-time job data, processes USCIS sponsorship records, and uses semantic search + agentic reasoning (via CrewAI) to recommend the most relevant, high-probability job opportunities.

🚀 Features
🔍 Scrapes job listings from Serper.dev and Tavily APIs

🧪 Uses PySpark pipelines for scalable data preprocessing

🤖 Employs CrewAI agents for intelligent resume-job matching

📈 Ranks jobs using cosine similarity and H-1B sponsorship likelihood

🧑‍💻 Frontend built with Streamlit for user interaction

🛠️ Tech Stack
PySpark

CrewAI

Pinecone

Streamlit

Serper.dev API

USCIS H-1B certification data

📦 How to Run!
[Your paragraph text (1) (2)](https://github.com/user-attachments/assets/179c4c7d-e812-4d2b-8d45-62c88fa1f0d3)

bash
Copy
Edit
# Clone the repo
git clone https://github.com/sriyesh1999/smart-h1b-job-matcher.git
cd smart-h1b-job-matcher

# Install dependencies
pip install -r requirements.txt

# Start the app
streamlit run app.py
📁 Project Structure
data/ – USCIS and employer datasets

agents/ – CrewAI agent logic

match_engine/ – Matching + ranking logic

frontend/ – Streamlit UI

utils/ – Data loading, preprocessing, and Pinecone operations

📈 Results
Improved relevance by 25% over keyword-only search

Reduced manual search time by surfacing visa-friendly jobs first

👨‍💻 Author
Built by Sriyesh Karampuri
📎 LinkedIn
💻 GitHub

📜 License
MIT License
