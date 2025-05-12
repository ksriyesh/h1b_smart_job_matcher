import os
import docx2txt
from PyPDF2 import PdfReader

def extract_text_from_resume(file_path: str) -> str:
    """
    Extract text from a resume file (PDF, DOCX, or TXT).
    """
    ext = os.path.splitext(file_path)[-1].lower()

    try:
        if ext == ".pdf":
            return extract_text_from_pdf(file_path)
        elif ext == ".docx":
            return extract_text_from_docx(file_path)
        elif ext == ".txt":
            return extract_text_from_txt(file_path)
        else:
            return "Unsupported file format."
    except Exception as e:
        return f"❌ Failed to extract resume text: {str(e)}"

def extract_text_from_pdf(file_path: str) -> str:
    text = ""
    with open(file_path, "rb") as f:
        reader = PdfReader(f)
        for page in reader.pages:
            text += page.extract_text() or ""
    return text

def extract_text_from_docx(file_path: str) -> str:
    return docx2txt.process(file_path)

def extract_text_from_txt(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()
