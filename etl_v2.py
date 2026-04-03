import PyPDF2
import nltk
import psycopg2
import uuid
from datetime import datetime

# Load library for split sentences
nltk.download('punkt')


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

def run_etl_v2(file_path, author_name, essay_title):
    conn = None
    try:
        # 1. Connect Database
        conn = psycopg2.connect(**DB_CONFIG)

        cur = conn.cursor()

        # Get time for timestamp column
        now = datetime.now()

        # 2. EXTRACT: Read file PDF
        with open(file_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            extracted_text = ""
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    extracted_text += text

        # 3. TRANSFORM: Split sentences with NLTK
        raw_sentences = nltk.sent_tokenize(extracted_text)

        # Create UUID
        author_id = str(uuid.uuid4())
        doc_id = str(uuid.uuid4())
        version_id = str(uuid.uuid4())

        # 4. LOAD: save data into Postgres based on schema

        # table 1: authors
        cur.execute(
            "INSERT INTO db_assignment.authors (author_id, name) VALUES (%s, %s)",
            (author_id, author_name)
        )

        # table 2: documents
        cur.execute(
            """INSERT INTO db_assignment.documents (document_id, title, file_type, file_path, created_at) 
               VALUES (%s, %s, %s, %s, %s)""",
            (doc_id, essay_title, 'pdf', file_path, now)
        )

        # table 3: document_authors
        cur.execute(
            "INSERT INTO db_assignment.document_authors (document_id, author_id) VALUES (%s, %s)",
            (doc_id, author_id)
        )

        # table 4: document_versions
        cur.execute(
            """INSERT INTO db_assignment.document_versions (version_id, document_id, extracted_text, extraction_method, created_at) 
               VALUES (%s, %s, %s, %s, %s)""",
            (version_id, doc_id, extracted_text, 'PyPDF2_Parser', now)
        )

        # table 5: sentences
        for i, content in enumerate(raw_sentences):
            sentence_id = str(uuid.uuid4())
            cur.execute(
                """INSERT INTO db_assignment.sentences (sentence_id, version_id, content, position, is_clean, created_at) 
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (sentence_id, version_id, content.strip(), i + 1, True, now)
            )

        conn.commit()
        print(f"ETL Completed: Saved essay '{essay_title}' with {len(raw_sentences)} sentences.")

    except Exception as e:
        print(f"Have an error on this ETL process: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()


# Ví dụ thực thi
if __name__ == "__main__":
    run_etl_v2('Sample.pdf', 'Trần Thị B', 'The Impact of AI on Education')
