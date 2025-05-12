import streamlit as st
import pandas as pd
import os
import sys
import traceback
# Add root path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from resume.resume_parser import extract_text_from_resume
from spark_pipeline.pipeline import build_resume_job_match_pipeline
import os
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# Paths
PROCESSED_CSV_PATH = "data/processed/processed_employer_data.csv"
RELOAD_FLAG_PATH = "data/.reload_flag"

# -----------------------------------
# Load Processed Data
# -----------------------------------
print(f"Checking for processed data at: {PROCESSED_CSV_PATH}")
if not os.path.exists(PROCESSED_CSV_PATH):
    st.error("❌ Processed employer data not found. Please ensure 'data/processed/processed_employer_data.csv' exists.")
    st.stop()

try:
    df = pd.read_csv(PROCESSED_CSV_PATH)
    print("✅ Processed data loaded successfully")
except Exception as e:
    print("❌ Failed to load processed CSV:", e)
    st.error("Error loading processed data.")
    st.stop()

# -----------------------------------
# Streamlit UI
# -----------------------------------
st.set_page_config(page_title="Smart H1B Job Matcher", layout="wide")
st.title("🧠 Smart H1B Job Matcher")
st.write("Match your resume with jobs from top H1B-sponsoring companies.")

# Sidebar
st.sidebar.header("Search Filters")

role_options = [
    "Software Developer", "Backend Developer", "Frontend Developer",
    "Full Stack Developer", "DevOps Engineer", "Data Analyst", "Data Scientist",
    "Machine Learning Engineer", "Cloud Engineer", "Business Analyst",
    "QA Engineer", "Product Manager", "UI/UX Designer", "Other (General IT Role)"
]
selected_role = st.sidebar.selectbox("Select Desired Role", role_options, index=0)

available_states = sorted(df["state"].dropna().str.upper().unique())
selected_state = st.sidebar.selectbox("Select U.S. State", available_states)

num_companies = st.sidebar.slider("Number of Top H1B Companies", 3, 30, 10)
uploaded_file = st.sidebar.file_uploader("Upload Your Resume", type=["pdf", "docx", "txt"])

# -----------------------------------
# Matching Logic
# -----------------------------------
if uploaded_file and selected_role and selected_state:
    file_ext = uploaded_file.name.split(".")[-1]
    resume_path = f"temp_resume.{file_ext}"
    try:
        with open(resume_path, "wb") as f:
            f.write(uploaded_file.getvalue())

        resume_text = extract_text_from_resume(resume_path)
        st.success("✅ Resume parsed successfully.")
        st.markdown("#### Resume Preview")
        st.text(resume_text)

        df_filtered = df[df["state"].str.upper() == selected_state]
        top_companies = df_filtered.sort_values("filing_score", ascending=False).head(num_companies)
        company_list = top_companies["company"].tolist()

        selected_company = st.selectbox(
            "Select a company to match jobs against:",
            ["-- Select a company --"] + company_list,
            index=0
        )

        if selected_company == "-- Select a company --":
            st.info("Please select a valid company to proceed.")
            st.stop()

        st.info(f"🔍 Fetching jobs for '{selected_role}' at '{selected_company}'...")

        results = build_resume_job_match_pipeline(
            resume_text=resume_text,
            selected_company=selected_company,
            role=selected_role
        )

        st.subheader("📈 Top Job Matches")
        for i, row in enumerate(results.collect()):
            st.markdown(f"### {i+1}. **{row['job_title']}** at **{row['company_name'].title()}**")
            st.write(f"📊 H1B Score: `{round(row['filing_score'] * 100, 2)}%`")
            st.write(f"📈 Resume Match Score: `{round(row['match_score'] * 100, 2)}%`")
            st.write(f"🔗 [Job Link]({row['job_url']})")
            st.markdown("---")
    except Exception as e:
        st.error(f"❌ Error: {e}")
        st.text(traceback.format_exc())
    finally:
        if os.path.exists(resume_path):
            os.remove(resume_path)
else:
    st.info("⬅️ Upload a resume and select a role and state to begin.")
