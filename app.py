# app.py
# Umfangreiche Streamlit Web-App: Audit + Betreiberpflichten + Dashboard + Uploads + PDF + Outlook/Teams Notifications
#
# Outlook (Graph) via Client Credentials:
#   MS_TENANT_ID, MS_CLIENT_ID, MS_CLIENT_SECRET
#   MAIL_SENDER_UPN   (z.B. shared mailbox oder User, in dessen Kontext gesendet werden darf)
#
# Teams via Incoming Webhook:
#   TEAMS_WEBHOOK_URL
#
# Optional:
#   APP_BASE_URL (z.B. https://deine-app.streamlit.app) für Links in Notifications
#
# Start:
#   pip install -r requirements.txt
#   streamlit run app.py

import os
import sqlite3
import hashlib
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List
from io import BytesIO

import pandas as pd
import requests
import streamlit as st
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm

APP_TITLE = "Audit & Betreiberpflichten – Hotel Web-App"
DB_PATH = os.environ.get("AUDIT_APP_DB", "audit_app.db")
UPLOAD_DIR = os.environ.get("AUDIT_APP_UPLOAD_DIR", "uploads")  # local folder
APP_BASE_URL = os.environ.get("APP_BASE_URL", "").rstrip("/")

# Microsoft Graph / Outlook
MS_TENANT_ID = os.environ.get("MS_TENANT_ID")
MS_CLIENT_ID = os.environ.get("MS_CLIENT_ID")
MS_CLIENT_SECRET = os.environ.get("MS_CLIENT_SECRET")
MAIL_SENDER_UPN = os.environ.get("MAIL_SENDER_UPN")  # sender mailbox/user principal name

# Teams
TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL")

# ---------------------------
# Helpers
# ---------------------------
def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def today() -> date:
    return date.today()

def parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def fmt_date(d: Optional[date]) -> str:
    return d.strftime("%d.%m.%Y") if d else ""

def utc_now_iso() -> str:
    return datetime.utcnow().isoformat()

def add_months(d: date, months: int) -> date:
    import calendar
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    last_day = calendar.monthrange(y, m)[1]
    day = min(d.day, last_day)
    return date(y, m, day)

def status_from_days(days: Optional[int], warn_days=30) -> str:
    if days is None:
        return "—"
    if days < 0:
        return "Überfällig"
    if days == 0:
        return "Fällig"
    if days <= warn_days:
        return "Bald fällig"
    return "OK"

def severity_rank(status: str) -> int:
    return {"Überfällig": 0, "Fällig": 1, "Bald fällig": 2, "OK": 3, "—": 4}.get(status, 99)

def require_login():
    if "user" not in st.session_state or not st.session_state["user"]:
        st.info("Bitte einloggen.")
        st.stop()

def role_in(*roles):
    u = st.session_state.get("user")
    return bool(u) and u["role"] in roles

def can_access_hotel(hotel_code: str) -> bool:
    u = st.session_state.get("user")
    if not u:
        return False
    if u["role"] == "Admin":
        return True
    if u["role"] in ("Direktor", "Techniker"):
        return u.get("hotel_code") == hotel_code
    if u["role"] == "Auditor":
        return True
    return False

def ensure_upload_dir():
    os.makedirs(UPLOAD_DIR, exist_ok=True)

def safe_filename(name: str) -> str:
    # very simple
    name = name.replace("\\", "_").replace("/", "_")
    return "".join(c for c in name if c.isalnum() or c in (" ", ".", "_", "-", "(", ")")).strip()

# ---------------------------
# DB
# ---------------------------
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    cur = conn.cursor()
    cur.executescript("""
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS hotels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        city TEXT,
        rooms INTEGER,
        sqm INTEGER,
        director_name TEXT,
        technician_name TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        hotel_code TEXT,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        FOREIGN KEY (hotel_code) REFERENCES hotels(code) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS compliance_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotel_code TEXT NOT NULL,
        asset TEXT NOT NULL,
        task TEXT NOT NULL,
        interval_months INTEGER NOT NULL,
        last_date TEXT,
        next_date TEXT,
        owner_name TEXT,
        evidence_link TEXT,
        notes TEXT,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (hotel_code) REFERENCES hotels(code) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS audits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        audit_code TEXT UNIQUE NOT NULL,
        hotel_code TEXT NOT NULL,
        norm TEXT NOT NULL,
        area TEXT NOT NULL,
        auditor_name TEXT,
        audit_date TEXT,
        status TEXT NOT NULL,
        score REAL,
        summary TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (hotel_code) REFERENCES hotels(code) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS audit_questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        norm TEXT NOT NULL,
        chapter TEXT NOT NULL,
        question TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS audit_answers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        audit_id INTEGER NOT NULL,
        question_id INTEGER NOT NULL,
        score TEXT,
        deviation TEXT,
        evidence TEXT,
        notes TEXT,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (audit_id) REFERENCES audits(id) ON DELETE CASCADE,
        FOREIGN KEY (question_id) REFERENCES audit_questions(id) ON DELETE CASCADE,
        UNIQUE(audit_id, question_id)
    );

    CREATE TABLE IF NOT EXISTS actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotel_code TEXT NOT NULL,
        audit_id INTEGER,
        title TEXT NOT NULL,
        category TEXT NOT NULL,
        owner_name TEXT,
        due_date TEXT,
        status TEXT NOT NULL,
        effectiveness_date TEXT,
        notes TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (hotel_code) REFERENCES hotels(code) ON DELETE CASCADE,
        FOREIGN KEY (audit_id) REFERENCES audits(id) ON DELETE SET NULL
    );

    -- Attachments: can be linked to compliance/audit/action
    CREATE TABLE IF NOT EXISTS attachments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotel_code TEXT NOT NULL,
        entity_type TEXT NOT NULL,          -- "compliance" | "audit" | "action"
        entity_id INTEGER NOT NULL,
        filename TEXT NOT NULL,
        stored_path TEXT NOT NULL,
        mime_type TEXT,
        uploaded_by TEXT,
        uploaded_at TEXT NOT NULL,
        FOREIGN KEY (hotel_code) REFERENCES hotels(code) ON DELETE CASCADE
    );
    """)
    conn.commit()
    conn.close()

def seed_if_empty():
    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) c FROM hotels")
    if cur.fetchone()["c"] == 0:
        now = utc_now_iso()
        hotels = [
            ("H1","Hotel 1","",None,None,"","",now),
            ("H2","Hotel 2","",None,None,"","",now),
            ("H3","Hotel 3","",None,None,"","",now),
            ("H4","Hotel 4","",None,None,"","",now),
            ("H5","Hotel 5","",None,None,"","",now),
        ]
        cur.executemany("""
            INSERT INTO hotels(code,name,city,rooms,sqm,director_name,technician_name,created_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, hotels)

    cur.execute("SELECT COUNT(*) c FROM users")
    if cur.fetchone()["c"] == 0:
        now = utc_now_iso()
        cur.execute("""
            INSERT INTO users(email,name,password_hash,role,hotel_code,is_active,created_at)
            VALUES (?,?,?,?,?,?,?)
        """, ("admin@local", "Admin", sha256("admin123"), "Admin", None, 1, now))

        # Example directors
        for hc in ["H1","H2","H3","H4","H5"]:
            cur.execute("""
                INSERT INTO users(email,name,password_hash,role,hotel_code,is_active,created_at)
                VALUES (?,?,?,?,?,?,?)
            """, (f"direktor_{hc.lower()}@local", f"Direktor {hc}", sha256("director123"), "Direktor", hc, 1, now))

    cur.execute("SELECT COUNT(*) c FROM audit_questions")
    if cur.fetchone()["c"] == 0:
        questions = []
        questions += [
            ("ISO 9001","4","Sind Kontext und interessierte Parteien bestimmt und dokumentiert?"),
            ("ISO 9001","5","Sind Rollen/Verantwortlichkeiten und Qualitäts-Politik festgelegt?"),
            ("ISO 9001","6","Sind Risiken/Chancen bewertet und Qualitätsziele geplant?"),
            ("ISO 9001","7","Sind Kompetenzen/Schulungen nachweisbar und Dokumente gelenkt?"),
            ("ISO 9001","8","Sind Prozesse definiert, umgesetzt und überwacht (inkl. Outsourcing)?"),
            ("ISO 9001","9","Werden Kennzahlen geprüft und Managementbewertungen durchgeführt?"),
            ("ISO 9001","10","Werden Abweichungen, Korrekturmaßnahmen und KVP wirksam umgesetzt?"),
        ]
        questions += [
            ("ISO 50001","4","Ist der EnMS-Geltungsbereich definiert und Kontext/Stakeholder bewertet?"),
            ("ISO 50001","5","Energiepolitik, Rollen und Top-Management-Commitment vorhanden?"),
            ("ISO 50001","6","Energetische Bewertung: SEU, EnPI und Baseline festgelegt?"),
            ("ISO 50001","6","Energieziele & Aktionspläne: Zuständigkeiten, Termine, Monitoring?"),
            ("ISO 50001","8","Betriebliche Steuerung: Betrieb/Wartung/Beschaffung energiebezogen geregelt?"),
            ("ISO 50001","9","Messkonzept & Datenqualität: EnPI-Überwachung, Analyse, interne Audits?"),
            ("ISO 50001","10","Korrekturmaßnahmen & kontinuierliche Verbesserung nachweisbar?"),
        ]
        questions += [
            ("ISO 14001","4","Kontext/Stakeholder & Geltungsbereich des UMS definiert?"),
            ("ISO 14001","6","Umweltaspekte & bindende Verpflichtungen identifiziert und bewertet?"),
            ("ISO 14001","6","Umweltziele & Programme mit Verantwortlichkeiten/Terminen?"),
            ("ISO 14001","8","Betriebliche Steuerung inkl. Notfallvorsorge/Reaktion umgesetzt?"),
            ("ISO 14001","9","Überwachung/Messung, Compliance-Bewertung und Managementreview?"),
            ("ISO 14001","10","Nichtkonformitäten & Korrekturmaßnahmen wirksam?"),
        ]
        questions += [
            ("ISO 45001","4","Kontext/Stakeholder & SGA-Geltungsbereich definiert?"),
            ("ISO 45001","5","Rollen/Verantwortlichkeiten, Beteiligung/Konsultation geregelt?"),
            ("ISO 45001","6","Gefährdungsbeurteilung, Risiken/Chancen & Rechtsanforderungen umgesetzt?"),
            ("ISO 45001","8","Operative Steuerung: Maßnahmen, Unterweisungen, Fremdfirmen?"),
            ("ISO 45001","9","Leistungsbewertung, interne Audits und Managementreview?"),
            ("ISO 45001","10","Vorfallmanagement, Korrekturmaßnahmen, kontinuierliche Verbesserung?"),
        ]
        cur.executemany("INSERT INTO audit_questions(norm,chapter,question,is_active) VALUES (?,?,?,1)", questions)

    cur.execute("SELECT COUNT(*) c FROM compliance_items")
    if cur.fetchone()["c"] == 0:
        now = utc_now_iso()
        templates = [
            ("Aufzug", "SV-Prüfung", 12),
            ("Brandmeldeanlage", "Wartung", 12),
            ("Sprinkleranlage", "Inspektion/Wartung", 12),
            ("RWA", "Wartung", 12),
            ("Notbeleuchtung", "Prüfung", 12),
            ("Trinkwasser", "Legionellenprüfung", 36),
            ("Lüftungsanlage", "Hygieneinspektion (VDI 6022)", 12),
            ("Heizungsanlage", "Wartung", 12),
            ("Kälteanlage", "Wartung", 12),
            ("Fettabscheider", "Entsorgung/Inspektion", 1),
        ]
        rows = []
        for hc in ["H1","H2","H3","H4","H5"]:
            for asset, task, interval in templates:
                rows.append((hc, asset, task, interval, None, None, "", "", "", now))
        cur.executemany("""
            INSERT INTO compliance_items(hotel_code,asset,task,interval_months,last_date,next_date,owner_name,evidence_link,notes,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, rows)

    conn.commit()
    conn.close()

def compute_and_store_next_dates():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id, last_date, interval_months FROM compliance_items")
    rows = cur.fetchall()
    now = utc_now_iso()
    for r in rows:
        last_d = parse_date(r["last_date"])
        next_d = add_months(last_d, int(r["interval_months"])) if last_d else None
        cur.execute("UPDATE compliance_items SET next_date=?, updated_at=? WHERE id=?",
                    (next_d.isoformat() if next_d else None, now, r["id"]))
    conn.commit()
    conn.close()

# ---------------------------
# Data Access
# ---------------------------
def get_hotels() -> pd.DataFrame:
    conn = db()
    df = pd.read_sql_query("SELECT * FROM hotels ORDER BY code", conn)
    conn.close()
    return df

def get_user_by_email(email: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE email=? AND is_active=1", (email,))
    r = cur.fetchone()
    conn.close()
    return dict(r) if r else None

def list_users() -> pd.DataFrame:
    conn = db()
    df = pd.read_sql_query("SELECT id,email,name,role,hotel_code,is_active,created_at FROM users ORDER BY role, email", conn)
    conn.close()
    return df

def upsert_user(email, name, role, hotel_code, password_plain: Optional[str], is_active: bool):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute("SELECT id FROM users WHERE email=?", (email,))
    row = cur.fetchone()
    if row:
        if password_plain:
            cur.execute("""
                UPDATE users SET name=?, role=?, hotel_code=?, password_hash=?, is_active=? WHERE email=?
            """, (name, role, hotel_code, sha256(password_plain), 1 if is_active else 0, email))
        else:
            cur.execute("""
                UPDATE users SET name=?, role=?, hotel_code=?, is_active=? WHERE email=?
            """, (name, role, hotel_code, 1 if is_active else 0, email))
    else:
        cur.execute("""
            INSERT INTO users(email,name,password_hash,role,hotel_code,is_active,created_at)
            VALUES (?,?,?,?,?,?,?)
        """, (email, name, sha256(password_plain or "changeme123"), role, hotel_code, 1 if is_active else 0, now))
    conn.commit()
    conn.close()

def select_hotel_filter(hotels_df: pd.DataFrame) -> Optional[str]:
    u = st.session_state.get("user")
    if u["role"] in ("Direktor","Techniker"):
        return u["hotel_code"]
    options = ["Alle"] + hotels_df["code"].tolist()
    sel = st.selectbox("Hotel-Filter", options, index=0)
    return None if sel == "Alle" else sel

# ---------------------------
# Compliance
# ---------------------------
def compliance_df(hotel_code: Optional[str]=None) -> pd.DataFrame:
    conn = db()
    if hotel_code:
        df = pd.read_sql_query("""
            SELECT * FROM compliance_items WHERE hotel_code=? ORDER BY next_date IS NULL, next_date, asset
        """, conn, params=(hotel_code,))
    else:
        df = pd.read_sql_query("""
            SELECT * FROM compliance_items ORDER BY hotel_code, next_date IS NULL, next_date, asset
        """, conn)
    conn.close()
    return df

def compliance_kpis(hotel_code: Optional[str]=None, warn_days=30):
    df = compliance_df(hotel_code)
    statuses = {"Überfällig":0,"Fällig":0,"Bald fällig":0,"OK":0,"—":0}
    td = today()
    for _, r in df.iterrows():
        nd = parse_date(r["next_date"])
        if not nd:
            statuses["—"] += 1
            continue
        days = (nd - td).days
        s = status_from_days(days, warn_days=warn_days)
        statuses[s] += 1
    return statuses, len(df)

def update_compliance_item(item_id: int, interval_months: int, last_date_str: Optional[str],
                           owner_name: str, evidence_link: str, notes: str):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    last_d = parse_date(last_date_str)
    next_d = add_months(last_d, interval_months) if last_d else None
    cur.execute("""
        UPDATE compliance_items
        SET interval_months=?, last_date=?, next_date=?, owner_name=?, evidence_link=?, notes=?, updated_at=?
        WHERE id=?
    """, (interval_months,
          last_d.isoformat() if last_d else None,
          next_d.isoformat() if next_d else None,
          owner_name, evidence_link, notes, now, item_id))
    conn.commit()
    conn.close()

def add_compliance_item(hotel_code, asset, task, interval_months):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute("""
        INSERT INTO compliance_items(hotel_code,asset,task,interval_months,last_date,next_date,owner_name,evidence_link,notes,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (hotel_code, asset, task, interval_months, None, None, "", "", "", now))
    conn.commit()
    conn.close()

def delete_compliance_item(item_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM compliance_items WHERE id=?", (item_id,))
    conn.commit()
    conn.close()

# ---------------------------
# Audits
# ---------------------------
def next_audit_code() -> str:
    y = today().year
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT audit_code FROM audits WHERE audit_code LIKE ? ORDER BY audit_code DESC LIMIT 1", (f"A-{y}-%",))
    r = cur.fetchone()
    conn.close()
    if not r:
        return f"A-{y}-0001"
    n = int(r["audit_code"].split("-")[-1]) + 1
    return f"A-{y}-{n:04d}"

def list_audits(hotel_code: Optional[str]=None) -> pd.DataFrame:
    conn = db()
    if hotel_code:
        df = pd.read_sql_query("SELECT * FROM audits WHERE hotel_code=? ORDER BY audit_date DESC, created_at DESC",
                               conn, params=(hotel_code,))
    else:
        df = pd.read_sql_query("SELECT * FROM audits ORDER BY audit_date DESC, created_at DESC", conn)
    conn.close()
    return df

def create_audit(hotel_code, norm, area, auditor_name, audit_date_str, status):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    acode = next_audit_code()
    cur.execute("""
        INSERT INTO audits(audit_code,hotel_code,norm,area,auditor_name,audit_date,status,score,summary,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (acode, hotel_code, norm, area, auditor_name, audit_date_str or None, status, None, "", now, now))
    audit_id = cur.lastrowid
    cur.execute("SELECT id FROM audit_questions WHERE norm=? AND is_active=1", (norm,))
    qids = [row["id"] for row in cur.fetchall()]
    for qid in qids:
        cur.execute("""
            INSERT OR IGNORE INTO audit_answers(audit_id,question_id,score,deviation,evidence,notes,updated_at)
            VALUES (?,?,?,?,?,?,?)
        """, (audit_id, qid, "", "", "", "", now))
    conn.commit()
    conn.close()
    return acode

def get_audit(audit_id: int) -> Dict:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM audits WHERE id=?", (audit_id,))
    r = cur.fetchone()
    conn.close()
    return dict(r) if r else {}

def audit_questions_answers(audit_id: int) -> pd.DataFrame:
    conn = db()
    df = pd.read_sql_query("""
        SELECT aq.id as question_id, aq.norm, aq.chapter, aq.question,
               aa.id as answer_id, aa.score, aa.deviation, aa.evidence, aa.notes, aa.updated_at
        FROM audit_answers aa
        JOIN audit_questions aq ON aq.id = aa.question_id
        WHERE aa.audit_id=?
        ORDER BY aq.chapter, aq.id
    """, conn, params=(audit_id,))
    conn.close()
    return df

def update_audit_answer(answer_id: int, score: str, deviation: str, evidence: str, notes: str):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute("""
        UPDATE audit_answers
        SET score=?, deviation=?, evidence=?, notes=?, updated_at=?
        WHERE id=?
    """, (score, deviation, evidence, notes, now, answer_id))
    conn.commit()
    conn.close()

def recompute_audit_score(audit_id: int) -> Optional[float]:
    df = audit_questions_answers(audit_id)
    vals = []
    for s in df["score"].tolist():
        if s in ("0","1","2"):
            vals.append(int(s))
    score = round(sum(vals)/len(vals), 2) if vals else None
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute("UPDATE audits SET score=?, updated_at=? WHERE id=?", (score, now, audit_id))
    conn.commit()
    conn.close()
    return score

def update_audit_meta(audit_id: int, status: str, audit_date_str: Optional[str], auditor_name: str, summary: str):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute("""
        UPDATE audits
        SET status=?, audit_date=?, auditor_name=?, summary=?, updated_at=?
        WHERE id=?
    """, (status, audit_date_str or None, auditor_name, summary, now, audit_id))
    conn.commit()
    conn.close()

# ---------------------------
# Actions
# ---------------------------
def list_actions(hotel_code: Optional[str]=None) -> pd.DataFrame:
    conn = db()
    if hotel_code:
        df = pd.read_sql_query("""
            SELECT a.*, au.audit_code
            FROM actions a
            LEFT JOIN audits au ON au.id=a.audit_id
            WHERE a.hotel_code=?
            ORDER BY (a.due_date IS NULL), a.due_date, a.created_at DESC
        """, conn, params=(hotel_code,))
    else:
        df = pd.read_sql_query("""
            SELECT a.*, au.audit_code
            FROM actions a
            LEFT JOIN audits au ON au.id=a.audit_id
            ORDER BY (a.due_date IS NULL), a.due_date, a.created_at DESC
        """, conn)
    conn.close()
    return df

def create_action(hotel_code, audit_id, title, category, owner_name, due_date_str, status, notes):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute("""
        INSERT INTO actions(hotel_code,audit_id,title,category,owner_name,due_date,status,effectiveness_date,notes,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (hotel_code, audit_id, title, category, owner_name, due_date_str or None, status, None, notes, now, now))
    conn.commit()
    conn.close()

def update_action(action_id: int, title, category, owner_name, due_date_str, status, effectiveness_date_str, notes):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute("""
        UPDATE actions
        SET title=?, category=?, owner_name=?, due_date=?, status=?, effectiveness_date=?, notes=?, updated_at=?
        WHERE id=?
    """, (title, category, owner_name, due_date_str or None, status, effectiveness_date_str or None, notes, now, action_id))
    conn.commit()
    conn.close()

def delete_action(action_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM actions WHERE id=?", (action_id,))
    conn.commit()
    conn.close()

# ---------------------------
# Attachments
# ---------------------------
def list_attachments(hotel_code: str, entity_type: str, entity_id: int) -> pd.DataFrame:
    conn = db()
    df = pd.read_sql_query("""
        SELECT * FROM attachments
        WHERE hotel_code=? AND entity_type=? AND entity_id=?
        ORDER BY uploaded_at DESC
    """, conn, params=(hotel_code, entity_type, entity_id))
    conn.close()
    return df

def add_attachment(hotel_code: str, entity_type: str, entity_id: int, filename: str,
                   stored_path: str, mime_type: str, uploaded_by: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO attachments(hotel_code,entity_type,entity_id,filename,stored_path,mime_type,uploaded_by,uploaded_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, (hotel_code, entity_type, entity_id, filename, stored_path, mime_type, uploaded_by, utc_now_iso()))
    conn.commit()
    conn.close()

def delete_attachment(att_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT stored_path FROM attachments WHERE id=?", (att_id,))
    r = cur.fetchone()
    if r:
        try:
            os.remove(r["stored_path"])
        except Exception:
            pass
    cur.execute("DELETE FROM attachments WHERE id=?", (att_id,))
    conn.commit()
    conn.close()

def upload_attachment_ui(hotel_code: str, entity_type: str, entity_id: int):
    ensure_upload_dir()
    u = st.session_state["user"]
    up = st.file_uploader("Datei hochladen", type=None, key=f"up_{entity_type}_{entity_id}")
    if up is not None:
        fn = safe_filename(up.name)
        folder = os.path.join(UPLOAD_DIR, hotel_code, entity_type, str(entity_id))
        os.makedirs(folder, exist_ok=True)
        stored_path = os.path.join(folder, f"{int(datetime.utcnow().timestamp())}_{fn}")
        with open(stored_path, "wb") as f:
            f.write(up.getbuffer())
        add_attachment(hotel_code, entity_type, entity_id, fn, stored_path, up.type or "", u["name"])
        st.success("Upload gespeichert.")
        st.rerun()

def attachments_list_ui(hotel_code: str, entity_type: str, entity_id: int):
    df = list_attachments(hotel_code, entity_type, entity_id)
    if df.empty:
        st.caption("Keine Anhänge.")
        return
    for _, r in df.iterrows():
        cols = st.columns([3,2,2,1,1])
        cols[0].write(f"📎 **{r['filename']}**")
        cols[1].write(r.get("uploaded_by") or "")
        cols[2].write(r.get("uploaded_at")[:19].replace("T"," "))
        # download
        try:
            with open(r["stored_path"], "rb") as f:
                data = f.read()
            cols[3].download_button("Download", data, file_name=r["filename"], mime=r.get("mime_type") or "application/octet-stream")
        except Exception:
            cols[3].write("—")
        # delete
        if role_in("Admin") and cols[4].button("Löschen", key=f"del_att_{r['id']}"):
            delete_attachment(int(r["id"]))
            st.success("Anhang gelöscht.")
            st.rerun()

# ---------------------------
# PDF Export (Audit Report)
# ---------------------------
def make_audit_pdf(audit: Dict, dfq: pd.DataFrame) -> bytes:
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    def header(y):
        c.setFont("Helvetica-Bold", 14)
        c.drawString(20*mm, y, "Auditbericht")
        c.setFont("Helvetica", 10)
        c.drawRightString(width-20*mm, y, datetime.now().strftime("%d.%m.%Y %H:%M"))
        return y - 10*mm

    y = height - 20*mm
    y = header(y)

    c.setFont("Helvetica-Bold", 11)
    c.drawString(20*mm, y, f"Audit: {audit.get('audit_code','')}")
    y -= 6*mm
    c.setFont("Helvetica", 10)
    c.drawString(20*mm, y, f"Hotel: {audit.get('hotel_code','')}   Norm: {audit.get('norm','')}   Bereich: {audit.get('area','')}")
    y -= 6*mm
    c.drawString(20*mm, y, f"Auditdatum: {fmt_date(parse_date(audit.get('audit_date')))}   Auditor: {audit.get('auditor_name') or ''}")
    y -= 6*mm
    c.drawString(20*mm, y, f"Status: {audit.get('status','')}   Score: {audit.get('score') if audit.get('score') is not None else '—'}")
    y -= 10*mm

    c.setFont("Helvetica-Bold", 11)
    c.drawString(20*mm, y, "Zusammenfassung")
    y -= 6*mm
    c.setFont("Helvetica", 10)
    summary = audit.get("summary") or ""
    for line in wrap_text(summary, 95):
        c.drawString(20*mm, y, line)
        y -= 5*mm
        if y < 20*mm:
            c.showPage()
            y = height - 20*mm
            y = header(y)

    y -= 4*mm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(20*mm, y, "Checkliste & Ergebnisse")
    y -= 8*mm

    c.setFont("Helvetica-Bold", 9)
    c.drawString(20*mm, y, "Kapitel")
    c.drawString(40*mm, y, "Bew.")
    c.drawString(55*mm, y, "Abw.")
    c.drawString(70*mm, y, "Frage (gekürzt)")
    y -= 5*mm
    c.setFont("Helvetica", 9)

    for _, row in dfq.iterrows():
        kap = str(row["chapter"])
        sc = row["score"] or ""
        dv = row["deviation"] or ""
        q = (row["question"] or "")[:120]
        c.drawString(20*mm, y, kap)
        c.drawString(40*mm, y, sc)
        c.drawString(55*mm, y, dv)
        for line in wrap_text(q, 70):
            c.drawString(70*mm, y, line)
            y -= 4.5*mm
            if y < 20*mm:
                c.showPage()
                y = height - 20*mm
                y = header(y)
                c.setFont("Helvetica", 9)
        y -= 2*mm

    c.showPage()
    c.save()
    return buf.getvalue()

def wrap_text(text: str, max_chars: int) -> List[str]:
    words = (text or "").split()
    lines = []
    line = ""
    for w in words:
        if len(line) + len(w) + 1 <= max_chars:
            line = (line + " " + w).strip()
        else:
            lines.append(line)
            line = w
    if line:
        lines.append(line)
    if not lines:
        return [""]
    return lines

# ---------------------------
# Notifications (Outlook via Graph, Teams via Webhook)
# ---------------------------
_graph_token_cache = {"token": None, "expires_at": 0}

def graph_get_token() -> Optional[str]:
    # Client Credentials token (app-only)
    if not (MS_TENANT_ID and MS_CLIENT_ID and MS_CLIENT_SECRET):
        return None
    now = int(datetime.utcnow().timestamp())
    if _graph_token_cache["token"] and now < _graph_token_cache["expires_at"] - 60:
        return _graph_token_cache["token"]

    url = f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": MS_CLIENT_ID,
        "client_secret": MS_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default"
    }
    r = requests.post(url, data=data, timeout=20)
    if r.status_code != 200:
        return None
    js = r.json()
    token = js.get("access_token")
    expires_in = int(js.get("expires_in", 3600))
    _graph_token_cache["token"] = token
    _graph_token_cache["expires_at"] = now + expires_in
    return token

def graph_send_mail(to_emails: List[str], subject: str, html_body: str) -> bool:
    token = graph_get_token()
    if not token or not MAIL_SENDER_UPN:
        return False

    url = f"https://graph.microsoft.com/v1.0/users/{MAIL_SENDER_UPN}/sendMail"
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": e}} for e in to_emails],
        },
        "saveToSentItems": "true"
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    return r.status_code in (202,)

def teams_post_message(title: str, text: str) -> bool:
    if not TEAMS_WEBHOOK_URL:
        return False
    # Simple MessageCard payload (works for Incoming Webhook)
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": title,
        "themeColor": "0078D7",
        "title": title,
        "text": text
    }
    r = requests.post(TEAMS_WEBHOOK_URL, json=payload, timeout=20)
    return r.status_code in (200, 201)

def build_compliance_digest(hotel_code: Optional[str], warn_days: int) -> Dict:
    df = compliance_df(hotel_code)
    td = today()
    items = []
    for _, r in df.iterrows():
        nd = parse_date(r["next_date"])
        if not nd:
            continue
        days = (nd - td).days
        stt = status_from_days(days, warn_days=warn_days)
        if stt in ("Überfällig", "Fällig", "Bald fällig"):
            items.append({
                "hotel": r["hotel_code"],
                "asset": r["asset"],
                "task": r["task"],
                "next": nd,
                "days": days,
                "status": stt,
                "owner": r["owner_name"] or ""
            })
    items.sort(key=lambda x: (severity_rank(x["status"]), x["days"], x["hotel"], x["asset"]))
    return {"items": items, "count": len(items)}

def build_actions_digest(hotel_code: Optional[str]) -> Dict:
    df = list_actions(hotel_code)
    td = today()
    items = []
    if len(df):
        for _, r in df.iterrows():
            if r["status"] == "Erledigt":
                continue
            dd = parse_date(r["due_date"])
            overdue = bool(dd and dd < td)
            items.append({
                "hotel": r["hotel_code"],
                "title": r["title"],
                "category": r["category"],
                "due": dd,
                "overdue": overdue,
                "status": r["status"],
                "owner": r["owner_name"] or "",
                "audit_code": r.get("audit_code") or ""
            })
    # overdue first, then due date
    items.sort(key=lambda x: (0 if x["overdue"] else 1, x["due"] or date(2999,1,1)))
    return {"items": items, "count": len(items)}

def send_digest(to_emails: List[str], hotel_filter: Optional[str], warn_days: int,
                send_mail: bool, send_teams: bool) -> Dict[str, bool]:
    comp = build_compliance_digest(hotel_filter, warn_days)
    acts = build_actions_digest(hotel_filter)

    title = f"Audit/Compliance Digest – {fmt_date(today())}"
    scope = f"Hotel: {hotel_filter}" if hotel_filter else "Alle Hotels"
    base_link = f"<p><b>{scope}</b></p>" if send_mail else ""
    link_hint = ""
    if APP_BASE_URL:
        link_hint = f"<p>App: <a href='{APP_BASE_URL}'>{APP_BASE_URL}</a></p>"

    # HTML for mail
    html = f"""
    <html><body>
    <h2>{title}</h2>
    {base_link}
    {link_hint}
    <h3>Betreiberpflichten (Überfällig/Fällig/Bald fällig): {comp['count']}</h3>
    <ul>
    """
    for it in comp["items"][:40]:
        html += f"<li><b>{it['hotel']}</b> – {it['asset']} / {it['task']} – <b>{it['status']}</b> – {fmt_date(it['next'])} ({it['days']} Tage) {('– Owner: '+it['owner']) if it['owner'] else ''}</li>"
    html += "</ul>"
    html += f"<h3>Offene Maßnahmen: {acts['count']}</h3><ul>"
    for it in acts["items"][:40]:
        due = fmt_date(it["due"])
        flag = "🚨" if it["overdue"] else "⏳"
        html += f"<li>{flag} <b>{it['hotel']}</b> – [{it['category']}] {it['title']} – Frist: <b>{due}</b> – Status: {it['status']} {('– Owner: '+it['owner']) if it['owner'] else ''}</li>"
    html += "</ul></body></html>"

    # Text for Teams
    t = f"**{title}**\n\n**{scope}**\n\n"
    t += f"**Betreiberpflichten fällig:** {comp['count']}\n"
    for it in comp["items"][:20]:
        t += f"- **{it['hotel']}** {it['asset']} / {it['task']} → **{it['status']}** ({fmt_date(it['next'])}, {it['days']} Tage)\n"
    t += f"\n**Offene Maßnahmen:** {acts['count']}\n"
    for it in acts["items"][:20]:
        due = fmt_date(it["due"])
        flag = "🚨" if it["overdue"] else "⏳"
        t += f"- {flag} **{it['hotel']}** [{it['category']}] {it['title']} → Frist **{due}**\n"
    if APP_BASE_URL:
        t += f"\nApp: {APP_BASE_URL}"

    out = {"mail": False, "teams": False}
    if send_mail:
        out["mail"] = graph_send_mail(to_emails, title, html)
    if send_teams:
        out["teams"] = teams_post_message(title, t)
    return out

# ---------------------------
# Auth UI
# ---------------------------
def login_ui():
    st.subheader("Login")
    with st.form("login_form", clear_on_submit=False):
        email = st.text_input("E-Mail", value=st.session_state.get("login_email",""))
        pw = st.text_input("Passwort", type="password")
        submitted = st.form_submit_button("Einloggen")
    if submitted:
        u = get_user_by_email(email.strip().lower())
        if not u:
            st.error("User nicht gefunden oder deaktiviert.")
            return
        if sha256(pw) != u["password_hash"]:
            st.error("Falsches Passwort.")
            return
        st.session_state["user"] = {
            "id": u["id"],
            "email": u["email"],
            "name": u["name"],
            "role": u["role"],
            "hotel_code": u["hotel_code"]
        }
        st.session_state["login_email"] = email.strip().lower()
        st.success(f"Eingeloggt als {u['name']} ({u['role']})")
        st.rerun()

def header_ui():
    u = st.session_state.get("user")
    cols = st.columns([3,2,1])
    with cols[0]:
        st.title(APP_TITLE)
    with cols[1]:
        if u:
            st.caption(f"Angemeldet: **{u['name']}** · Rolle: **{u['role']}** · Hotel: **{u.get('hotel_code') or 'Alle'}**")
    with cols[2]:
        if u and st.button("Logout"):
            st.session_state["user"] = None
            st.rerun()

# ---------------------------
# Pages
# ---------------------------
def page_dashboard():
    require_login()
    hotels = get_hotels()
    st.subheader("Dashboard")

    warn_days = st.slider("Warnschwelle (Tage bis fällig)", min_value=7, max_value=90, value=30, step=1)
    hotel_filter = select_hotel_filter(hotels)

    statuses, total = compliance_kpis(hotel_filter, warn_days=warn_days)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Überfällig", statuses["Überfällig"])
    c2.metric("Fällig", statuses["Fällig"])
    c3.metric("Bald fällig", statuses["Bald fällig"])
    c4.metric("OK", statuses["OK"])
    c5.metric("Gesamt", total)

    actions = list_actions(hotel_filter)
    open_actions = actions[actions["status"] != "Erledigt"] if len(actions) else actions
    overdue = 0
    td = today()
    if len(open_actions):
        for dd in open_actions["due_date"].tolist():
            d = parse_date(dd)
            if d and d < td:
                overdue += 1
    a1, a2, a3 = st.columns(3)
    a1.metric("Offene Maßnahmen", len(open_actions))
    a2.metric("Major offen", int((open_actions["category"] == "Major").sum()) if len(open_actions) else 0)
    a3.metric("Maßnahmen überfällig", overdue)

    st.divider()
    st.markdown("### Top Betreiberpflichten (Überfällig/Fällig/Bald fällig)")
    comp = build_compliance_digest(hotel_filter, warn_days)
    if comp["count"] == 0:
        st.info("Keine fälligen/überfälligen Betreiberpflichten.")
    else:
        view = pd.DataFrame([{
            "Hotel": it["hotel"],
            "Anlage": it["asset"],
            "Aufgabe": it["task"],
            "Status": it["status"],
            "Nächste Prüfung": fmt_date(it["next"]),
            "Tage": it["days"],
            "Owner": it["owner"]
        } for it in comp["items"][:50]])
        st.dataframe(view, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### Notifications (Outlook / Teams)")
    with st.expander("Digest senden", expanded=False):
        st.caption("Sende eine Zusammenfassung der fälligen Betreiberpflichten + offenen Maßnahmen.")
        send_mail = st.checkbox("Outlook E-Mail senden (Graph)", value=bool(MS_TENANT_ID and MAIL_SENDER_UPN))
        send_teams = st.checkbox("Teams Nachricht senden (Webhook)", value=bool(TEAMS_WEBHOOK_URL))

        default_to = st.session_state.get("digest_to", "")
        to_emails = st.text_input("Empfänger (Komma-separiert)", value=default_to, help="z.B. direktor_h1@firma.de, direktor_h2@firma.de")
        if st.button("Digest jetzt senden"):
            recipients = [e.strip() for e in to_emails.split(",") if e.strip()]
            st.session_state["digest_to"] = to_emails
            if send_mail and not (MS_TENANT_ID and MS_CLIENT_ID and MS_CLIENT_SECRET and MAIL_SENDER_UPN):
                st.error("Graph Credentials fehlen (MS_TENANT_ID/MS_CLIENT_ID/MS_CLIENT_SECRET/MAIL_SENDER_UPN).")
            elif send_mail and not recipients:
                st.error("Bitte E-Mail Empfänger eintragen.")
            else:
                res = send_digest(recipients, hotel_filter, warn_days, send_mail, send_teams)
                st.success(f"Ergebnis: Mail={res['mail']} · Teams={res['teams']}")

def page_betreiberpflichten():
    require_login()
    hotels = get_hotels()
    st.subheader("Betreiberpflichten / Prüfkalender")

    warn_days = st.slider("Warnschwelle (Tage bis fällig)", 7, 90, 30, 1, key="warn_ops")
    hotel_filter = select_hotel_filter(hotels)

    df = compliance_df(hotel_filter)
    td = today()

    rows = []
    for _, r in df.iterrows():
        nd = parse_date(r["next_date"])
        days = (nd - td).days if nd else None
        stt = status_from_days(days, warn_days=warn_days) if nd else "—"
        rows.append({
            "ID": r["id"],
            "Hotel": r["hotel_code"],
            "Anlage": r["asset"],
            "Aufgabe": r["task"],
            "Intervall (Monate)": r["interval_months"],
            "Letzte Prüfung": fmt_date(parse_date(r["last_date"])),
            "Nächste Prüfung": fmt_date(nd),
            "Tage": days if days is not None else "",
            "Status": stt,
            "Owner": r["owner_name"] or "",
            "Link": r["evidence_link"] or "",
        })
    view = pd.DataFrame(rows)
    if not view.empty:
        view["sev"] = view["Status"].apply(severity_rank)
        view["days_sort"] = pd.to_numeric(view["Tage"], errors="coerce").fillna(999999)
        view = view.sort_values(["sev", "days_sort", "Hotel", "Anlage"]).drop(columns=["sev","days_sort"])
    st.dataframe(view, use_container_width=True, hide_index=True)
    st.download_button("CSV export", view.to_csv(index=False).encode("utf-8"), "betreiberpflichten.csv", "text/csv")

    st.divider()
    st.markdown("### Eintrag bearbeiten / neu anlegen")

    ids = df["id"].tolist()
    sel_id = st.selectbox("Eintrag wählen", options=["Neu"] + ids, index=0)

    if sel_id == "Neu":
        with st.form("add_compliance"):
            hc_opts = hotels["code"].tolist()
            if role_in("Direktor","Techniker"):
                hc_opts = [st.session_state["user"]["hotel_code"]]
            hc = st.selectbox("Hotel", hc_opts)
            asset = st.text_input("Anlage", "")
            task = st.text_input("Prüfung/Wartung", "")
            interval = st.number_input("Intervall (Monate)", min_value=1, max_value=120, value=12, step=1)
            ok = st.form_submit_button("Anlegen")
        if ok:
            if not can_access_hotel(hc):
                st.error("Keine Berechtigung.")
            elif not asset.strip() or not task.strip():
                st.error("Bitte Anlage und Aufgabe ausfüllen.")
            else:
                add_compliance_item(hc, asset.strip(), task.strip(), int(interval))
                st.success("Angelegt.")
                st.rerun()
    else:
        r = df[df["id"] == sel_id].iloc[0].to_dict()
        if not can_access_hotel(r["hotel_code"]):
            st.error("Keine Berechtigung.")
            return
        with st.form("edit_compliance"):
            st.write(f"**Hotel:** {r['hotel_code']} · **Anlage:** {r['asset']} · **Aufgabe:** {r['task']}")
            interval = st.number_input("Intervall (Monate)", 1, 120, int(r["interval_months"]), 1)
            last = parse_date(r["last_date"])
            last_new = st.date_input("Letzte Prüfung", value=last or today())
            owner = st.text_input("Verantwortlich", value=r["owner_name"] or "")
            link = st.text_input("Nachweis/Link/Ticket", value=r["evidence_link"] or "")
            notes = st.text_area("Bemerkung", value=r["notes"] or "", height=120)
            c1, c2 = st.columns([1,1])
            save = c1.form_submit_button("Speichern")
            delete = c2.form_submit_button("Löschen")
        if save:
            update_compliance_item(int(sel_id), int(interval), last_new.isoformat(), owner, link, notes)
            st.success("Gespeichert.")
            st.rerun()
        if delete:
            if not role_in("Admin"):
                st.error("Löschen nur Admin.")
            else:
                delete_compliance_item(int(sel_id))
                st.success("Gelöscht.")
                st.rerun()

        st.divider()
        st.markdown("### Anhänge (Prüfprotokolle, Fotos, etc.)")
        upload_attachment_ui(r["hotel_code"], "compliance", int(sel_id))
        attachments_list_ui(r["hotel_code"], "compliance", int(sel_id))

def page_audits():
    require_login()
    hotels = get_hotels()
    st.subheader("Audits")

    hotel_filter = select_hotel_filter(hotels)

    st.markdown("### Auditliste")
    dfa = list_audits(hotel_filter)
    if len(dfa):
        show = dfa.copy()
        show["Auditdatum"] = show["audit_date"].apply(lambda x: fmt_date(parse_date(x)))
        show = show[["id","audit_code","hotel_code","norm","area","Auditdatum","status","score","auditor_name"]]
        st.dataframe(show, use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine Audits vorhanden.")

    st.divider()
    st.markdown("### Audit anlegen")
    with st.form("create_audit"):
        hc_opts = hotels["code"].tolist()
        if role_in("Direktor","Techniker"):
            hc_opts = [st.session_state["user"]["hotel_code"]]
        hc = st.selectbox("Hotel", hc_opts)
        norm = st.selectbox("Norm", ["ISO 9001","ISO 14001","ISO 45001","ISO 50001"])
        area = st.text_input("Bereich/Prozess", "Technik")
        auditor = st.text_input("Auditor", st.session_state["user"]["name"])
        adate = st.date_input("Auditdatum", value=today())
        status = st.selectbox("Status", ["Geplant","In Durchführung","Abgeschlossen"])
        ok = st.form_submit_button("Audit anlegen")
    if ok:
        if not can_access_hotel(hc):
            st.error("Keine Berechtigung.")
        else:
            acode = create_audit(hc, norm, area, auditor, adate.isoformat(), status)
            st.success(f"Audit erstellt: {acode}")
            st.rerun()

    st.divider()
    st.markdown("### Audit durchführen / bearbeiten")
    if len(dfa):
        audit_ids = dfa["id"].tolist()
        sel_audit_id = st.selectbox(
            "Audit auswählen",
            options=audit_ids,
            format_func=lambda i: f"{int(i)} – {dfa[dfa['id']==i].iloc[0]['audit_code']}"
        )
        audit = get_audit(int(sel_audit_id))
        if not can_access_hotel(audit["hotel_code"]):
            st.error("Keine Berechtigung.")
            return

        dfq = audit_questions_answers(audit["id"])

        with st.expander("Audit-Metadaten", expanded=True):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.write(f"**Audit:** {audit['audit_code']}")
                st.write(f"**Hotel:** {audit['hotel_code']} · **Norm:** {audit['norm']}")
                st.write(f"**Bereich:** {audit['area']}")
            with c2:
                status = st.selectbox("Status", ["Geplant","In Durchführung","Abgeschlossen"],
                                      index=["Geplant","In Durchführung","Abgeschlossen"].index(audit["status"]))
                ad = parse_date(audit["audit_date"]) or today()
                ad_new = st.date_input("Auditdatum", value=ad, key=f"ad_{audit['id']}")
            with c3:
                auditor_name = st.text_input("Auditor", value=audit.get("auditor_name") or "")
                st.metric("Score", "—" if audit.get("score") is None else audit.get("score"))

            summary = st.text_area("Zusammenfassung", value=audit.get("summary") or "", height=120)

            colx, coly, colz = st.columns([1,1,1])
            if colx.button("Metadaten speichern"):
                update_audit_meta(audit["id"], status, ad_new.isoformat(), auditor_name, summary)
                st.success("Gespeichert.")
                st.rerun()

            if coly.button("Score neu berechnen"):
                s = recompute_audit_score(audit["id"])
                st.success(f"Neuer Score: {s}")
                st.rerun()

            if colz.button("Auditbericht als PDF"):
                # Make sure score up to date
                recompute_audit_score(audit["id"])
                audit = get_audit(int(sel_audit_id))
                dfq = audit_questions_answers(audit["id"])
                pdf_bytes = make_audit_pdf(audit, dfq)
                st.download_button(
                    "PDF herunterladen",
                    pdf_bytes,
                    file_name=f"{audit['audit_code']}_Auditbericht.pdf",
                    mime="application/pdf"
                )

        st.markdown("#### Checkliste")
        for _, row in dfq.iterrows():
            with st.container(border=True):
                st.write(f"**{row['chapter']}** · {row['question']}")
                c1, c2, c3, c4 = st.columns([1,1,2,2])
                with c1:
                    score = st.selectbox("Bewertung", ["","0","1","2","NA"],
                                         index=["","0","1","2","NA"].index(row["score"] if row["score"] in ("0","1","2","NA") else ""),
                                         key=f"sc_{row['answer_id']}")
                with c2:
                    dev = st.selectbox("Abweichung", ["","Nein","Ja"],
                                       index=["","Nein","Ja"].index(row["deviation"] if row["deviation"] in ("Ja","Nein") else ""),
                                       key=f"dv_{row['answer_id']}")
                with c3:
                    evidence = st.text_input("Nachweis", value=row["evidence"] or "", key=f"ev_{row['answer_id']}")
                with c4:
                    notes = st.text_input("Bemerkung", value=row["notes"] or "", key=f"nt_{row['answer_id']}")

                if st.button("Speichern", key=f"save_{row['answer_id']}"):
                    update_audit_answer(int(row["answer_id"]), score, dev, evidence, notes)
                    recompute_audit_score(audit["id"])
                    st.success("Gespeichert.")
                    st.rerun()

        st.divider()
        st.markdown("### Maßnahmen aus Abweichungen generieren")
        if st.button("Abweichungen (Ja) → Maßnahmen erstellen"):
            devs = dfq[dfq["deviation"] == "Ja"]
            created = 0
            for _, row in devs.iterrows():
                title = f"[{audit['audit_code']}] {row['chapter']}: {row['question'][:120]}"
                create_action(audit["hotel_code"], audit["id"], title, "Minor", "", None, "Offen", "Auto erzeugt aus Audit-Abweichung")
                created += 1
            st.success(f"{created} Maßnahmen erstellt.")
            st.rerun()

        st.divider()
        st.markdown("### Anhänge (Audit-Unterlagen)")
        upload_attachment_ui(audit["hotel_code"], "audit", int(audit["id"]))
        attachments_list_ui(audit["hotel_code"], "audit", int(audit["id"]))

    else:
        st.info("Lege zuerst ein Audit an.")

def page_massnahmen():
    require_login()
    hotels = get_hotels()
    st.subheader("Maßnahmen / Findings")

    hotel_filter = select_hotel_filter(hotels)
    df = list_actions(hotel_filter)

    td = today()
    overdue = 0
    if len(df):
        open_df = df[df["status"] != "Erledigt"]
        for dd in open_df["due_date"].tolist():
            d = parse_date(dd)
            if d and d < td:
                overdue += 1

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Offen", int((df["status"]=="Offen").sum()) if len(df) else 0)
    c2.metric("In Bearbeitung", int((df["status"]=="In Bearbeitung").sum()) if len(df) else 0)
    c3.metric("Erledigt", int((df["status"]=="Erledigt").sum()) if len(df) else 0)
    c4.metric("Überfällig", overdue)

    st.divider()
    st.markdown("### Liste")
    if len(df):
        show = df.copy()
        show["Frist"] = show["due_date"].apply(lambda x: fmt_date(parse_date(x)))
        show["Wirksamkeit"] = show["effectiveness_date"].apply(lambda x: fmt_date(parse_date(x)))
        show = show[["id","hotel_code","audit_code","category","title","owner_name","Frist","status","Wirksamkeit","notes"]]
        st.dataframe(show, use_container_width=True, hide_index=True)
        st.download_button("CSV export", show.to_csv(index=False).encode("utf-8"), "massnahmen.csv", "text/csv")
    else:
        st.info("Noch keine Einträge.")

    st.divider()
    st.markdown("### Maßnahme anlegen / bearbeiten")
    ids = df["id"].tolist() if len(df) else []
    sel = st.selectbox("Auswählen", options=["Neu"] + ids, index=0)

    if sel == "Neu":
        with st.form("create_action_form"):
            hc_opts = hotels["code"].tolist()
            if role_in("Direktor","Techniker"):
                hc_opts = [st.session_state["user"]["hotel_code"]]
            hc = st.selectbox("Hotel", hc_opts)

            audits = list_audits(hc)
            audit_map = {"—": None}
            for _, r in audits.iterrows():
                audit_map[f"{r['audit_code']} · {r['norm']} · {r['area']}"] = int(r["id"])
            audit_sel = st.selectbox("Bezug zu Audit (optional)", options=list(audit_map.keys()))

            title = st.text_input("Titel", "")
            category = st.selectbox("Kategorie", ["Major","Minor","Beobachtung","Verbesserung"])
            owner = st.text_input("Verantwortlich", "")
            due = st.date_input("Frist", value=today() + timedelta(days=14))
            status = st.selectbox("Status", ["Offen","In Bearbeitung","Erledigt"])
            notes = st.text_area("Notizen", "", height=100)
            ok = st.form_submit_button("Anlegen")
        if ok:
            if not can_access_hotel(hc):
                st.error("Keine Berechtigung.")
            elif not title.strip():
                st.error("Titel fehlt.")
            else:
                create_action(hc, audit_map[audit_sel], title.strip(), category, owner, due.isoformat(), status, notes)
                st.success("Angelegt.")
                st.rerun()
    else:
        row = df[df["id"]==sel].iloc[0].to_dict()
        if not can_access_hotel(row["hotel_code"]):
            st.error("Keine Berechtigung.")
            return

        with st.form("edit_action_form"):
            st.write(f"**Hotel:** {row['hotel_code']} · **Audit:** {row.get('audit_code') or '—'}")
            title = st.text_input("Titel", value=row["title"] or "")
            category = st.selectbox("Kategorie", ["Major","Minor","Beobachtung","Verbesserung"],
                                   index=["Major","Minor","Beobachtung","Verbesserung"].index(row["category"]))
            owner = st.text_input("Verantwortlich", value=row["owner_name"] or "")
            due_old = parse_date(row["due_date"]) or (today() + timedelta(days=14))
            due = st.date_input("Frist", value=due_old)
            status = st.selectbox("Status", ["Offen","In Bearbeitung","Erledigt"],
                                  index=["Offen","In Bearbeitung","Erledigt"].index(row["status"]))
            eff_old = parse_date(row["effectiveness_date"])
            eff = st.date_input("Wirksamkeitsprüfung (optional)", value=eff_old or today())
            eff_clear = st.checkbox("Wirksamkeitsdatum löschen", value=False)
            notes = st.text_area("Notizen", value=row["notes"] or "", height=100)
            c1, c2 = st.columns(2)
            save = c1.form_submit_button("Speichern")
            delete = c2.form_submit_button("Löschen")
        if save:
            eff_val = None if eff_clear else eff.isoformat()
            update_action(int(sel), title, category, owner, due.isoformat(), status, eff_val, notes)
            st.success("Gespeichert.")
            st.rerun()
        if delete:
            if not role_in("Admin"):
                st.error("Löschen nur Admin.")
            else:
                delete_action(int(sel))
                st.success("Gelöscht.")
                st.rerun()

        st.divider()
        st.markdown("### Anhänge (Maßnahmen-Nachweise)")
        upload_attachment_ui(row["hotel_code"], "action", int(sel))
        attachments_list_ui(row["hotel_code"], "action", int(sel))

def page_admin():
    require_login()
    if not role_in("Admin"):
        st.error("Nur Admin.")
        return

    st.subheader("Admin")
    tab1, tab2, tab3, tab4 = st.tabs(["Hotels", "User", "Auditfragen", "Integrationen"])

    with tab1:
        st.markdown("### Hotels")
        hotels = get_hotels()
        st.dataframe(hotels, use_container_width=True, hide_index=True)

        st.markdown("#### Hotel bearbeiten")
        hc = st.selectbox("Hotel", hotels["code"].tolist(), key="adm_hotel_sel")
        row = hotels[hotels["code"]==hc].iloc[0].to_dict()
        with st.form("edit_hotel"):
            name = st.text_input("Name", value=row["name"])
            city = st.text_input("Stadt", value=row.get("city") or "")
            rooms = st.number_input("Zimmer", min_value=0, max_value=5000, value=int(row["rooms"] or 0))
            sqm = st.number_input("m²", min_value=0, max_value=200000, value=int(row["sqm"] or 0))
            director = st.text_input("Direktor Name", value=row.get("director_name") or "")
            tech = st.text_input("Techniker Name", value=row.get("technician_name") or "")
            ok = st.form_submit_button("Speichern")
        if ok:
            conn = db()
            cur = conn.cursor()
            cur.execute("""
                UPDATE hotels SET name=?, city=?, rooms=?, sqm=?, director_name=?, technician_name=?
                WHERE code=?
            """, (name, city, rooms or None, sqm or None, director, tech, hc))
            conn.commit()
            conn.close()
            st.success("Gespeichert.")
            st.rerun()

    with tab2:
        st.markdown("### User")
        users = list_users()
        st.dataframe(users, use_container_width=True, hide_index=True)

        st.markdown("#### User anlegen/ändern")
        hotels = get_hotels()
        hc_opts = ["—"] + hotels["code"].tolist()
        with st.form("upsert_user_form"):
            email = st.text_input("E-Mail", "")
            name = st.text_input("Name", "")
            role = st.selectbox("Rolle", ["Admin","Direktor","Techniker","Auditor"])
            hotel_code = st.selectbox("Hotel (für Direktor/Techniker)", hc_opts)
            pw = st.text_input("Neues Passwort (leer = nicht ändern)", type="password")
            active = st.checkbox("Aktiv", value=True)
            ok = st.form_submit_button("Speichern")
        if ok:
            if not email.strip() or not name.strip():
                st.error("E-Mail und Name sind Pflicht.")
            else:
                hc_val = None if hotel_code == "—" else hotel_code
                upsert_user(email.strip().lower(), name.strip(), role, hc_val, pw if pw else None, active)
                st.success("Gespeichert.")
                st.rerun()

        st.info("Default Admin: admin@local / admin123 (bitte sofort ändern).")

    with tab3:
        st.markdown("### Auditfragen-Katalog")
        conn = db()
        dfq = pd.read_sql_query("SELECT * FROM audit_questions ORDER BY norm, chapter, id", conn)
        conn.close()
        st.dataframe(dfq, use_container_width=True, hide_index=True)

        st.markdown("#### Frage hinzufügen")
        with st.form("add_q"):
            norm = st.selectbox("Norm", ["ISO 9001","ISO 14001","ISO 45001","ISO 50001"])
            chapter = st.text_input("Kapitel", "6")
            question = st.text_area("Frage", "", height=80)
            ok = st.form_submit_button("Hinzufügen")
        if ok:
            if not question.strip():
                st.error("Frage fehlt.")
            else:
                conn = db()
                cur = conn.cursor()
                cur.execute("INSERT INTO audit_questions(norm,chapter,question,is_active) VALUES (?,?,?,1)",
                            (norm, chapter.strip(), question.strip()))
                conn.commit()
                conn.close()
                st.success("Hinzugefügt.")
                st.rerun()

    with tab4:
        st.markdown("### Integrationen (Outlook / Teams)")
        st.write("**Teams Webhook aktiv:**", bool(TEAMS_WEBHOOK_URL))
        st.write("**Microsoft Graph aktiv:**", bool(MS_TENANT_ID and MS_CLIENT_ID and MS_CLIENT_SECRET and MAIL_SENDER_UPN))
        st.caption("Für Outlook/Graph brauchst du eine Azure App Registration (Client Credentials) + Mail.Send Berechtigung (Application).")
        st.caption("Für Teams reicht ein Incoming Webhook im gewünschten Channel.")

# ---------------------------
# App
# ---------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    init_db()
    seed_if_empty()
    compute_and_store_next_dates()

    header_ui()

    if "user" not in st.session_state or not st.session_state["user"]:
        st.warning("Nicht eingeloggt.")
        st.info("Standard-Login: admin@local / admin123 (danach User anlegen & Passwörter ändern).")
        login_ui()
        return

    pages = {
        "Dashboard": page_dashboard,
        "Betreiberpflichten": page_betreiberpflichten,
        "Audits": page_audits,
        "Maßnahmen": page_massnahmen,
    }
    if role_in("Admin"):
        pages["Admin"] = page_admin

    st.sidebar.radio("Navigation", list(pages.keys()), key="nav")
    choice = st.session_state["nav"]
    st.sidebar.caption("Direktor/Techniker sehen automatisch nur das eigene Hotel.")

    pages[choice]()

if __name__ == "__main__":
    main()
