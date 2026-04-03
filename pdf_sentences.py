import nltk
nltk.download('punkt')
nltk.download('punkt_tab')

import pdfplumber
import psycopg2
from psycopg2.extras import execute_batch
from nltk.tokenize import sent_tokenize
import re
import os

# -----------------------
# CONFIG
# -----------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "root"
}

PDF_PATH = "Sample.pdf"
BATCH_SIZE = 100


# -----------------------
# CLEAN TEXT
# -----------------------
def clean_text(text: str) -> str:
    text = re.sub(r'\s+', ' ', text)  # normalize whitespace
    text = text.strip()
    return text


# -----------------------
# EXTRACT + SPLIT
# -----------------------
def extract_sentences_from_pdf(pdf_path):
    results = []
    file_name = os.path.basename(pdf_path)

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            raw_text = page.extract_text()
            if not raw_text:
                continue

            cleaned = clean_text(raw_text)

            sentences = sent_tokenize(cleaned)

            for sentence in sentences:
                results.append((file_name, page_num, sentence))

    return results


# -----------------------
# INSERT TO POSTGRES
# -----------------------
def insert_to_postgres(records):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO db_assignment.pdf_sentences (file_name, page_number, sentence)
        VALUES (%s, %s, %s)
    """

    try:
        execute_batch(cursor, insert_query, records, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Inserted {len(records)} sentences.")
    except Exception as e:
        conn.rollback()
        print("Error:", e)
    finally:
        cursor.close()
        conn.close()


# -----------------------
# MAIN PIPELINE
# -----------------------
def main():
    print("Extracting sentences from PDF...")
    sentences = extract_sentences_from_pdf(PDF_PATH)

    print(f"Total sentences: {len(sentences)}")

    if sentences:
        print("Saving to PostgreSQL...")
        insert_to_postgres(sentences)

    print("Done.")


if __name__ == "__main__":
    main()
