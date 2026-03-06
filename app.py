# app.py
# MAXI-Version mit:
# - 5 Hotels fest hinterlegt
# - Betreiberpflichten + ISO-Audits
# - city-spezifische Auditfragen (Frankfurt / München)
# - Betreiberpflichten-Prüfkalender
# - Maßnahmenworkflow inkl. Major/Minor/OFI
# - Auditprogramm
# - Anhänge
# - PDF-Export
# - Outlook/Teams-Digest (optional)

import os
import sqlite3
import hashlib
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Tuple
from io import BytesIO

import pandas as pd
import requests
import streamlit as st
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm


APP_TITLE = "Audit & Betreiberpflichten – Hotel Web-App"
DB_PATH = os.environ.get("AUDIT_APP_DB", "audit_app.db")
UPLOAD_DIR = os.environ.get("AUDIT_APP_UPLOAD_DIR", "uploads")
APP_BASE_URL = os.environ.get("APP_BASE_URL", "").rstrip("/")

HOTELS = [
    ("6502", "Hotel München City Center Affiliated by Melia", "München"),
    ("6513", "Hotel Frankfurt Messe Affiliated by Melia", "Frankfurt"),
    ("6527", "INNSiDE by Meliá München Parkstadt Schwabing", "München"),
    ("6551", "INNSiDE by Meliá Frankfurt Ostend", "Frankfurt"),
    ("6595", "Melia Frankfurt City", "Frankfurt"),
]

HOTEL_CODES = [h[0] for h in HOTELS]

MS_TENANT_ID = os.environ.get("MS_TENANT_ID")
MS_CLIENT_ID = os.environ.get("MS_CLIENT_ID")
MS_CLIENT_SECRET = os.environ.get("MS_CLIENT_SECRET")
MAIL_SENDER_UPN = os.environ.get("MAIL_SENDER_UPN")
TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL")

ACTION_STATUSES = ["Offen", "In Bearbeitung", "Wirksamkeit offen", "Erledigt"]
ACTION_CATEGORIES = ["Major", "Minor", "Beobachtung", "Verbesserung"]
RISK_LEVELS = ["", "Niedrig", "Mittel", "Hoch"]
PROGRAM_STATUSES = ["Geplant", "Durchgeführt", "Abgesagt"]


# ---------------------------
# Helpers
# ---------------------------
def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def today() -> date:
    return date.today()


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat()


def parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def fmt_date(d: Optional[date]) -> str:
    return d.strftime("%d.%m.%Y") if d else ""


def add_months(d: date, months: int) -> date:
    import calendar
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    last_day = calendar.monthrange(y, m)[1]
    day = min(d.day, last_day)
    return date(y, m, day)


def status_from_days(days: Optional[int], warn_days: int = 30) -> str:
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


def ensure_upload_dir():
    os.makedirs(UPLOAD_DIR, exist_ok=True)


def safe_filename(name: str) -> str:
    name = name.replace("\\", "_").replace("/", "_")
    return "".join(c for c in name if c.isalnum() or c in (" ", ".", "_", "-", "(", ")")).strip()


# ---------------------------
# Auth helpers
# ---------------------------
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
        clause TEXT,
        topic_group TEXT,
        city_profile TEXT NOT NULL DEFAULT 'ALL',
        question TEXT NOT NULL,
        evidence_hint TEXT,
        is_active INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS audit_answers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        audit_id INTEGER NOT NULL,
        question_id INTEGER NOT NULL,
        score TEXT,
        deviation TEXT,
        deviation_type TEXT,
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
        effectiveness_result TEXT,
        risk_level TEXT,
        immediate_action TEXT,
        root_cause TEXT,
        corrective_action TEXT,
        notes TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (hotel_code) REFERENCES hotels(code) ON DELETE CASCADE,
        FOREIGN KEY (audit_id) REFERENCES audits(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS audit_program (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotel_code TEXT NOT NULL,
        norm TEXT NOT NULL,
        area TEXT NOT NULL,
        planned_date TEXT NOT NULL,
        owner_name TEXT,
        status TEXT NOT NULL,
        reminder_days INTEGER DEFAULT 14,
        notes TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (hotel_code) REFERENCES hotels(code) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS attachments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotel_code TEXT NOT NULL,
        entity_type TEXT NOT NULL,
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


def migrate_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(audit_questions)")
    cols = {r[1] for r in cur.fetchall()}
    if "clause" not in cols:
        cur.execute("ALTER TABLE audit_questions ADD COLUMN clause TEXT")
    if "topic_group" not in cols:
        cur.execute("ALTER TABLE audit_questions ADD COLUMN topic_group TEXT")
    if "city_profile" not in cols:
        cur.execute("ALTER TABLE audit_questions ADD COLUMN city_profile TEXT NOT NULL DEFAULT 'ALL'")
    if "evidence_hint" not in cols:
        cur.execute("ALTER TABLE audit_questions ADD COLUMN evidence_hint TEXT")

    cur.execute("PRAGMA table_info(audit_answers)")
    cols = {r[1] for r in cur.fetchall()}
    if "deviation_type" not in cols:
        cur.execute("ALTER TABLE audit_answers ADD COLUMN deviation_type TEXT")

    cur.execute("PRAGMA table_info(actions)")
    cols = {r[1] for r in cur.fetchall()}
    additions = [
        ("effectiveness_result", "ALTER TABLE actions ADD COLUMN effectiveness_result TEXT"),
        ("risk_level", "ALTER TABLE actions ADD COLUMN risk_level TEXT"),
        ("immediate_action", "ALTER TABLE actions ADD COLUMN immediate_action TEXT"),
        ("root_cause", "ALTER TABLE actions ADD COLUMN root_cause TEXT"),
        ("corrective_action", "ALTER TABLE actions ADD COLUMN corrective_action TEXT"),
    ]
    for col, ddl in additions:
        if col not in cols:
            cur.execute(ddl)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_program (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_code TEXT NOT NULL,
            norm TEXT NOT NULL,
            area TEXT NOT NULL,
            planned_date TEXT NOT NULL,
            owner_name TEXT,
            status TEXT NOT NULL,
            reminder_days INTEGER DEFAULT 14,
            notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (hotel_code) REFERENCES hotels(code) ON DELETE CASCADE
        )
    """)

    conn.commit()
    conn.close()


# ---------------------------
# Hotel helpers
# ---------------------------
def get_hotels() -> pd.DataFrame:
    conn = db()
    df = pd.read_sql_query("SELECT * FROM hotels ORDER BY code", conn)
    conn.close()
    return df


def hotel_label_map(hotels_df: pd.DataFrame) -> Dict[str, str]:
    return {r["code"]: f"{r['code']} – {r['name']}" for _, r in hotels_df.iterrows()}


def get_hotel_city(hotel_code: str) -> str:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT city FROM hotels WHERE code=?", (hotel_code,))
    r = cur.fetchone()
    conn.close()
    return (r["city"] if r and r["city"] else "").strip()


# ---------------------------
# Question catalogs
# ---------------------------
def build_iso_questions() -> List[Tuple[str, str, str, str, str, str, str]]:
    q = []

    def add(norm: str, chapter: str, clause: str, topic_group: str, city_profile: str, question: str, hint: str):
        q.append((norm, chapter, clause, topic_group, city_profile, question, hint))

    # ISO 50001
    add("ISO 50001", "4", "4.1", "Kontext", "ALL",
        "Ist der Kontext der Organisation energiebezogen dokumentiert, plausibel und aktuell?",
        "Nachweise: Kontextanalyse, Energiepreis-/Versorgungsrisiken, Aktualisierung.")
    add("ISO 50001", "6", "6.3", "Energetische Bewertung", "ALL",
        "Ist die energetische Bewertung nachvollziehbar dokumentiert (Energiebilanz, Hauptverbraucher, Einflussgrößen)?",
        "Nachweise: Energiebilanz, Lastgänge, Hauptverbraucher, Datenbasis.")
    add("ISO 50001", "6", "6.4", "Kennzahlen", "ALL",
        "Sind EnPI definiert und nachvollziehbar beschrieben?",
        "Nachweise: Kennzahlen, Berechnungslogik, Datenquelle.")
    add("ISO 50001", "8", "8.1", "Betrieb", "ALL",
        "Sind operative Kriterien für wesentliche Energieeinsätze festgelegt und wirksam?",
        "Nachweise: SOP, GLT-Parameter, Änderungsprotokolle.")
    add("ISO 50001", "9", "9.1.1", "Monitoring", "ALL",
        "Existiert ein Mess- und Monitoringkonzept und wird es umgesetzt?",
        "Nachweise: Messstellenplan, Zählerliste, Datenqualität.")
    add("ISO 50001", "10", "10.2", "Abweichungen", "ALL",
        "Werden Nichtkonformitäten inklusive Ursachenanalyse und Wirksamkeitsprüfung bearbeitet?",
        "Nachweise: CAPA, RCA, Wirksamkeitsnachweis.")

    # ISO 14001
    add("ISO 14001", "6", "6.1.2", "Umweltaspekte", "ALL",
        "Sind Umweltaspekte identifiziert, bewertet und aktuell?",
        "Nachweise: Aspektebewertung, Kriterien, Review.")
    add("ISO 14001", "6", "6.1.3", "Bindende Verpflichtungen", "ALL",
        "Sind bindende Verpflichtungen identifiziert und bewertet?",
        "Nachweise: Rechtskataster, Auflagen, Compliance-Bewertung.")
    add("ISO 14001", "8", "8.1", "Betrieb", "ALL",
        "Sind operative Steuerungen für wesentliche Umweltaspekte umgesetzt?",
        "Nachweise: SOP, Checklisten, Lieferanten-/Fremdfirmensteuerung.")
    add("ISO 14001", "8", "8.2", "Notfälle", "ALL",
        "Ist Notfallvorsorge/Reaktion geplant, umgesetzt und geübt?",
        "Nachweise: Notfallpläne, Übungen, Protokolle.")

    # ISO 45001
    add("ISO 45001", "6", "6.1.2", "Gefährdungsbeurteilung", "ALL",
        "Ist die Gefährdungsbeurteilung vorhanden, aktuell und vollständig?",
        "Nachweise: GBU je Bereich, Maßnahmen, Aktualisierung.")
    add("ISO 45001", "7", "7.2", "Unterweisung", "ALL",
        "Sind Kompetenzen und Unterweisungen nachweisbar und wirksam?",
        "Nachweise: Unterweisungsplan, Nachweise, Wirksamkeit.")
    add("ISO 45001", "8", "8.1.3", "Fremdfirmen", "ALL",
        "Ist die Fremdfirmensteuerung wirksam organisiert?",
        "Nachweise: Einweisungen, Freigaben, Kontrollen.")
    add("ISO 45001", "10", "10.2", "Vorfälle", "ALL",
        "Werden Vorfälle inkl. Ursachenanalyse, Maßnahmen und Wirksamkeit bearbeitet?",
        "Nachweise: Unfallberichte, RCA, CAPA.")

    # ISO 9001
    add("ISO 9001", "4", "4.1", "Kontext", "ALL",
        "Sind Kontext, interessierte Parteien und Scope definiert?",
        "Nachweise: Kontextanalyse, Stakeholderliste, Scope.")
    add("ISO 9001", "6", "6.1", "Risiken/Ziele", "ALL",
        "Sind Risiken/Chancen bewertet und Ziele geplant?",
        "Nachweise: Risiko-/Chancenliste, Zielmatrix, Maßnahmen.")
    add("ISO 9001", "9", "9.3", "Managementreview", "ALL",
        "Findet Managementreview statt und sind Outputs dokumentiert?",
        "Nachweise: Protokolle, Entscheidungen, Ressourcen.")

    return q


def build_betreiberpflichten_questions() -> List[Tuple[str, str, str, str, str, str, str]]:
    q = []

    def add(chapter: str, clause: str, topic_group: str, city_profile: str, question: str, hint: str):
        q.append(("BETREIBERPFLICHTEN", chapter, clause, topic_group, city_profile, question, hint))

    # 1 Allgemeine Betreiberverantwortung
    add("1", "1.1", "Allgemeine Betreiberverantwortung", "ALL",
        "Ist ein Betreiberpflichtensystem vorhanden, das Pflichtenermittlung, Verantwortlichkeiten, Fristenüberwachung, Nachweise und Eskalation umfasst?",
        "Nachweise: Pflichtenregister, Verantwortungsmatrix, Fristenkontrolle, Reviews.")
    add("1", "1.2", "Allgemeine Betreiberverantwortung", "ALL",
        "Sind Pflichtenübertragungen schriftlich geregelt und wird deren Wirksamkeit überwacht?",
        "Nachweise: Delegationsschreiben, Organigramm, Kontrollnachweise.")
    add("1", "1.3", "Allgemeine Betreiberverantwortung", "ALL",
        "Werden interne Audits und Managementreviews mindestens jährlich durchgeführt und dokumentiert?",
        "Nachweise: Auditprogramm, Auditberichte, Review-Protokolle.")
    add("1", "1.4", "Allgemeine Betreiberverantwortung", "ALL",
        "Sind Dokumentationen vollständig, nachvollziehbar, manipulationssicher und verfügbar?",
        "Nachweise: Dokumentenlenkung, Archivstruktur, Aufbewahrungsregeln.")

    # 2 Brandschutz / Baurecht
    add("2", "2.1", "Baurecht & Brandschutz", "ALL",
        "Sind Brandschutzordnung, Flucht- und Rettungspläne sowie Mängellisten aktuell vorhanden?",
        "Nachweise: DIN 14096 A-C, Fluchtpläne, Mängelverfolgung.")
    add("2", "2.2", "Baurecht & Brandschutz", "ALL",
        "Werden Räumungsübungen, Schulungen von Brandschutzhelfern und Unterweisungen regelmäßig durchgeführt und dokumentiert?",
        "Nachweise: Schulungsnachweise, Übungsprotokolle, Teilnehmerlisten.")
    add("2", "2.3", "Baurecht & Brandschutz", "Frankfurt",
        "Liegt für Hotels mit mehr als 60 Betten der jährliche Brandschutzbericht mit Prüfprotokollen vor?",
        "Nachweise: Brandschutzbericht, Einreichnachweis, Prüfprotokolle.")
    add("2", "2.4", "Baurecht & Brandschutz", "Frankfurt",
        "Ist die Teilnahme an der Objektbegehung der Feuerwehr Frankfurt dokumentiert und werden Feststellungen nachverfolgt?",
        "Nachweise: Begehungsberichte, Termine, Mängellisten.")
    add("2", "2.5", "Baurecht & Brandschutz", "Frankfurt",
        "Ist der Nachweis über die Schulung des Personals in Alarm- und Räumungsverfahren vollständig dokumentiert?",
        "Nachweise: Schulungsunterlagen, Teilnehmerlisten, Wiederholungsintervalle.")
    add("2", "2.6", "Baurecht & Brandschutz", "München",
        "Sind die nach PrüfVBau Bayern relevanten sicherheitstechnischen Anlagen fristgerecht durch Prüfsachverständige geprüft?",
        "Nachweise: Prüfberichte, Mängellisten, Nachverfolgung.")
    add("2", "2.7", "Baurecht & Brandschutz", "München",
        "Sind Vorgaben aus BayBO/BeV/LBK/Branddirektion organisatorisch umgesetzt und nachweisbar?",
        "Nachweise: Behördenkommunikation, Konzepte, Prüfberichte.")

    # 3 Technische Anlagen
    add("3", "3.1", "Technische Anlagen", "ALL",
        "Sind Gefährdungsbeurteilungen für relevante technische Anlagen vorhanden und aktuell?",
        "Nachweise: Gefährdungsbeurteilungen, Anlagenliste, Änderungsmanagement.")
    add("3", "3.2", "Technische Anlagen", "ALL",
        "Werden Prüf- und Wartungsintervalle für Aufzüge, elektrische Anlagen, Druckanlagen, Notstrom, Trinkwasser und Kälte organisiert und eingehalten?",
        "Nachweise: Prüfregister, Wartungspläne, Protokolle, Mängelverfolgung.")
    add("3", "3.3", "Technische Anlagen", "ALL",
        "Werden sicherheitsrelevante Mängel dokumentiert, bewertet, nachverfolgt und Anlagen bei Bedarf außer Betrieb genommen?",
        "Nachweise: Sperrvermerke, Tickets, Freigaben, Nachprüfungen.")
    add("3", "3.4", "Technische Anlagen", "Frankfurt",
        "Sind hessische Prüfpflichten für BMA/SAA, RWA/NRA/MRA, Sicherheitsstromversorgung, Blitzschutz und CO-Warnanlagen fristgerecht erfüllt?",
        "Nachweise: TPrüfV/PrüfVO-Berichte, Terminregister, Mängelverfolgung.")
    add("3", "3.5", "Technische Anlagen", "München",
        "Sind bayerische Prüfpflichten für BMA, Sicherheitsstromversorgung, RWA und Feststellanlagen fristgerecht erfüllt?",
        "Nachweise: PrüfVBau-Berichte, Prüfsachverständigenberichte, Terminregister.")
    add("3", "3.6", "Technische Anlagen", "München",
        "Sind die Prüfintervalle für Aufzüge, ortsfeste/ortsveränderliche elektrische Anlagen, Notstrom sowie Leitern/Tritte dokumentiert und eingehalten?",
        "Nachweise: Prüfprotokolle, DGUV V3, ZÜS-Berichte, Leiterprüfungen.")

    # 4 Arbeitsschutz
    add("4", "4.1", "Arbeitsschutz & Unterweisung", "ALL",
        "Sind Gefährdungsbeurteilungen für alle Tätigkeiten/Bereiche vorhanden, aktuell und wirksam?",
        "Nachweise: GBU je Bereich, Maßnahmenstatus, Aktualisierung.")
    add("4", "4.2", "Arbeitsschutz & Unterweisung", "ALL",
        "Werden Unterweisungen vor Tätigkeitsaufnahme, bei Änderungen und mindestens jährlich durchgeführt und dokumentiert?",
        "Nachweise: Unterweisungsnachweise, Inhalte, Unterschriften.")
    add("4", "4.3", "Arbeitsschutz & Unterweisung", "ALL",
        "Sind Erste Hilfe, Ersthelferquote, Verbandsmaterial, Evakuierungsorganisation und Begehungen organisiert und nachweisbar?",
        "Nachweise: Ersthelferliste, ASA/Begehungen, Prüfungen Verbandsmaterial.")
    add("4", "4.4", "Arbeitsschutz & Unterweisung", "München",
        "Sind arbeitsmedizinische Vorsorge, FASI/ASA-Prozesse und Dokumentationspflichten gemäß Münchner Vollzugspraxis nachweisbar umgesetzt?",
        "Nachweise: Vorsorgekartei, ASA-Protokolle, FASI-Begehungen.")

    # 5 Hygiene / Lebensmittel / Trinkwasser
    add("5", "5.1", "Hygiene & Lebensmittel", "ALL",
        "Ist ein HACCP-System vorhanden, aktuell und mit dokumentierten Kontrollen umgesetzt?",
        "Nachweise: HACCP-Konzept, CCP-Kontrollen, Temperaturprotokolle.")
    add("5", "5.2", "Hygiene & Lebensmittel", "ALL",
        "Sind IfSG-Belehrungen, Hygieneschulungen und Personalhygieneanforderungen dokumentiert?",
        "Nachweise: Schulungsnachweise, Belehrungen, Hygieneunterweisungen.")
    add("5", "5.3", "Hygiene & Lebensmittel", "ALL",
        "Sind Reinigungs- und Desinfektionspläne vorhanden und wird deren Umsetzung nachweisbar kontrolliert?",
        "Nachweise: Reinigungspläne, Kontrolllisten, Stichprobe Durchführung.")
    add("5", "5.4", "Hygiene & Lebensmittel", "ALL",
        "Sind Legionellenuntersuchungen, Befunde, Maßnahmen und Behördenkommunikation vollständig dokumentiert?",
        "Nachweise: Laborberichte, Maßnahmen, Gefährdungsanalyse, Kommunikation Gesundheitsamt.")
    add("5", "5.5", "Hygiene & Lebensmittel", "München",
        "Ist die jährliche Legionellenuntersuchung nachweisbar umgesetzt?",
        "Nachweise: jährliche Laborberichte, Entnahmestellen, Maßnahmen.")

    # 6 Umwelt / Energie / Gewässerschutz
    add("6", "6.1", "Umwelt / Energie / Gewässerschutz", "ALL",
        "Sind Umwelt- und Energiepflichten systematisch organisiert und dokumentiert?",
        "Nachweise: Pflichtenregister, Prüf-/Wartungsnachweise, Umwelt-/Energiekonzept.")
    add("6", "6.2", "Umwelt / Energie / Gewässerschutz", "ALL",
        "Werden Heizungs-, Kälte-, Klima- und Lüftungsanlagen fristgerecht inspiziert, gewartet und – soweit erforderlich – geprüft?",
        "Nachweise: Wartungsprotokolle, F-Gas-Dokumentation, VDI-/GEG-Nachweise.")
    add("6", "6.3", "Umwelt / Energie / Gewässerschutz", "ALL",
        "Sind Fettabscheider, Abwasserpflichten, wassergefährdende Stoffe und Entsorgungsnachweise organisiert und dokumentiert?",
        "Nachweise: Wartungen, Generalinspektionen, Entsorgungsnachweise, AwSV-/WHG-Dokumente.")
    add("6", "6.4", "Umwelt / Energie / Gewässerschutz", "Frankfurt",
        "Sind Frankfurter Anforderungen aus Abwassersatzung/Einleitbedingungen berücksichtigt?",
        "Nachweise: Behördenkommunikation, Abscheidernachweise, Einleitunterlagen.")
    add("6", "6.5", "Umwelt / Energie / Gewässerschutz", "München",
        "Sind Anforderungen der Münchner Stadtentwässerung und des Referats für Klima- und Umweltschutz berücksichtigt?",
        "Nachweise: MSE-/RKU-Kommunikation, Entsorgungsnachweise, Berichte.")

    # 7 Datenschutz / IT
    add("7", "7.1", "Datenschutz & IT-Sicherheit", "ALL",
        "Sind Datenschutzorganisation, Verantwortlichkeiten und Verzeichnis der Verarbeitungstätigkeiten vorhanden und aktuell?",
        "Nachweise: VVT, Datenschutzkonzept, TOMs.")
    add("7", "7.2", "Datenschutz & IT-Sicherheit", "ALL",
        "Sind Schulungen, Incident-Management, Rechtekonzepte und Datensicherheit für Gästedaten/Meldedaten umgesetzt?",
        "Nachweise: Schulungen, Berechtigungskonzepte, Incident-Prozess.")

    # 8 Melderecht / Steuern
    add("8", "8.1", "Melderecht & Beherbergung", "ALL",
        "Werden Meldedaten bei Anreise vollständig und rechtssicher erhoben, aufbewahrt und fristgerecht gelöscht/vernichtet?",
        "Nachweise: Meldescheinprozess, Löschprotokolle, Stichprobe.")
    add("8", "8.2", "Melderecht & Beherbergung", "Frankfurt",
        "Wird der Frankfurter Tourismusbeitrag / die Übernachtungssteuer ordnungsgemäß dokumentiert und abgeführt?",
        "Nachweise: Satzungsbezug, Abrechnungen, Befreiungen, Fristen.")
    add("8", "8.3", "Melderecht & Beherbergung", "München",
        "Wird die Münchner Beherbergungssteuer (City Tax) ordnungsgemäß dokumentiert und abgeführt?",
        "Nachweise: Steuerabrechnungen, Befreiungsnachweise, Aufzeichnungen.")
    add("8", "8.4", "Melderecht & Beherbergung", "München",
        "Sind Meldescheine, Löschfristen und Befreiungsnachweise vollständig und plausibel dokumentiert?",
        "Nachweise: Meldescheine, Löschprotokolle, Befreiungsnachweise, Prüfpfad.")

    # 9 Delegation / Dokumentation
    add("9", "9.1", "Delegation / Dokumentation", "ALL",
        "Sind Pflichtenübertragungen schriftlich, fachlich geeignet, überwacht und regelmäßig kontrolliert?",
        "Nachweise: Delegationsschreiben, Qualifikationen, Kontrollberichte.")
    add("9", "9.2", "Delegation / Dokumentation", "ALL",
        "Sind Prüf-, Wartungs-, Schulungs- und Maßnahmennachweise zentral, vollständig und revisionssicher verfügbar?",
        "Nachweise: Ablagestruktur, Audittrail, Dokumentenlenkung.")
    add("9", "9.3", "Delegation / Dokumentation", "München",
        "Existiert ein Betreiberpflichten-Register mit Pflicht, Rechtsgrundlage, Verantwortlichem, Frist, Nachweis und Status?",
        "Nachweise: Register, Aktualisierung, Reviews, IKS.")

    # 10 Haftung / Versicherung
    add("10", "10.1", "Haftung / Versicherung", "ALL",
        "Sind versicherungsrelevante Obliegenheiten organisiert und nachweisbar erfüllt?",
        "Nachweise: Versicherungsunterlagen, Prüf-/Wartungsnachweise, Meldungen.")
    add("10", "10.2", "Haftung / Versicherung", "ALL",
        "Werden typische Betreiberpflichten-Risiken systematisch überwacht und Maßnahmen zur Haftungsminimierung umgesetzt?",
        "Nachweise: Risikomatrix, Maßnahmenlisten, Schulungen, Reviews.")
    add("10", "10.3", "Haftung / Versicherung", "München",
        "Sind dokumentationsrelevante Nachweise für Versicherer im Schadenfall vollständig verfügbar?",
        "Nachweise: GBU, Prüf-/Wartungsnachweise, Unterweisungsprotokolle, Maßnahmenlisten.")

    return q


def seed_if_empty():
    conn = db()
    cur = conn.cursor()

    # Hotels
    cur.execute("SELECT COUNT(*) c FROM hotels")
    if cur.fetchone()["c"] == 0:
        now = utc_now_iso()
        rows = [(code, name, city, None, None, "", "", now) for code, name, city in HOTELS]
        cur.executemany("""
            INSERT INTO hotels(code,name,city,rooms,sqm,director_name,technician_name,created_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, rows)

    # Users
    cur.execute("SELECT COUNT(*) c FROM users")
    if cur.fetchone()["c"] == 0:
        now = utc_now_iso()
        cur.execute("""
            INSERT INTO users(email,name,password_hash,role,hotel_code,is_active,created_at)
            VALUES (?,?,?,?,?,?,?)
        """, ("admin@local", "Admin", sha256("admin123"), "Admin", None, 1, now))
        for hc, hname, _ in HOTELS:
            cur.execute("""
                INSERT INTO users(email,name,password_hash,role,hotel_code,is_active,created_at)
                VALUES (?,?,?,?,?,?,?)
            """, (f"direktor_{hc}@local", f"Direktor {hc} – {hname}", sha256("director123"), "Direktor", hc, 1, now))

    # Compliance seed
    cur.execute("SELECT COUNT(*) c FROM compliance_items")
    if cur.fetchone()["c"] == 0:
        now = utc_now_iso()
        common = [
            ("Aufzug", "SV-Prüfung", 12),
            ("Brandmeldeanlage", "Wartung", 12),
            ("Sprinkleranlage", "Inspektion/Wartung", 12),
            ("RWA", "Wartung", 12),
            ("Notbeleuchtung", "Prüfung", 12),
            ("Trinkwasser", "Legionellenprüfung", 12),
            ("Lüftungsanlage", "Hygieneinspektion (VDI 6022)", 12),
            ("Heizungsanlage", "Wartung", 12),
            ("Kälteanlage", "Wartung", 12),
            ("Fettabscheider", "Entsorgung/Inspektion", 1),
            ("DGUV V3 ortsfeste Anlagen", "Prüfung", 48),
            ("DGUV V3 ortsveränderliche Betriebsmittel", "Prüfung", 12),
            ("Leitern & Tritte", "Prüfung", 12),
            ("Evakuierungsübung", "Durchführung / Nachweis", 12),
            ("Unterweisungen", "Jährliche Unterweisung", 12),
        ]
        rows = []
        for hc in HOTEL_CODES:
            for asset, task, interval in common:
                rows.append((hc, asset, task, interval, None, None, "", "", "", now))
        cur.executemany("""
            INSERT INTO compliance_items(hotel_code,asset,task,interval_months,last_date,next_date,owner_name,evidence_link,notes,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, rows)

    # Questions seed
    cur.execute("SELECT COUNT(*) c FROM audit_questions")
    if cur.fetchone()["c"] == 0:
        all_questions = []
        all_questions.extend(build_iso_questions())
        all_questions.extend(build_betreiberpflichten_questions())
        cur.executemany("""
            INSERT INTO audit_questions(norm,chapter,clause,topic_group,city_profile,question,evidence_hint,is_active)
            VALUES (?,?,?,?,?,?,?,1)
        """, all_questions)

    # Audit program seed
    cur.execute("SELECT COUNT(*) c FROM audit_program")
    if cur.fetchone()["c"] == 0:
        now = utc_now_iso()
        y = today().year
        rows = []
        for hc in HOTEL_CODES:
            rows.append((hc, "BETREIBERPFLICHTEN", "Gesamtaudit Betreiberpflichten", date(y, 5, 15).isoformat(), "", "Geplant", 21, "Seed", now, now))
            rows.append((hc, "ISO 50001", "Technik/Energie", date(y, 9, 15).isoformat(), "", "Geplant", 14, "Seed", now, now))
        cur.executemany("""
            INSERT INTO audit_program(hotel_code,norm,area,planned_date,owner_name,status,reminder_days,notes,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, rows)

    conn.commit()
    conn.close()


def seed_city_specific_compliance_items():
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()

    city_items = {
        "Frankfurt": [
            ("Brandschutzbericht Feuerwehr Frankfurt", "Jährliche Vorlage mit Prüfprotokollen", 12),
            ("Objektbegehung Feuerwehr Frankfurt", "Begehung / Nachverfolgung", 24),
            ("Tourismusbeitrag Frankfurt", "Abrechnung / Abführung / Dokumentation", 12),
            ("BMA / SAA", "Prüfsachverständigenprüfung Hessen", 36),
            ("RWA / NRA / MRA", "Prüfsachverständigenprüfung Hessen", 36),
            ("Sicherheitsstromversorgung", "Prüfsachverständigenprüfung Hessen", 36),
            ("Blitzschutz", "Prüfung / Messung", 36),
            ("CO-Warnanlagen", "Prüfung", 36),
        ],
        "München": [
            ("BMA", "Prüfsachverständigenprüfung PrüfVBau Bayern", 36),
            ("Sicherheitsstromversorgung", "Prüfsachverständigenprüfung PrüfVBau Bayern", 36),
            ("RWA", "Prüfsachverständigenprüfung PrüfVBau Bayern", 36),
            ("Feststellanlagen", "Prüfsachverständigenprüfung PrüfVBau Bayern", 36),
            ("Beherbergungssteuer München", "City-Tax Abrechnung / Dokumentation", 12),
            ("Münchner Stadtentwässerung", "Fettabscheider / Einleitbedingungen Prüfung", 12),
            ("Notstromanlagen", "Jährliche Prüfung", 12),
        ]
    }

    hotels = get_hotels()
    for _, h in hotels.iterrows():
        city = (h["city"] or "").strip()
        for asset, task, interval in city_items.get(city, []):
            cur.execute("""
                SELECT 1 FROM compliance_items
                WHERE hotel_code=? AND asset=? AND task=?
                LIMIT 1
            """, (h["code"], asset, task))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO compliance_items(hotel_code,asset,task,interval_months,last_date,next_date,owner_name,evidence_link,notes,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (h["code"], asset, task, interval, None, None, "", "", "City-spezifische Betreiberpflicht", now))

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
# User
# ---------------------------
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


# ---------------------------
# Compliance
# ---------------------------
def compliance_df(hotel_code: Optional[str] = None) -> pd.DataFrame:
    conn = db()
    if hotel_code:
        df = pd.read_sql_query("""
            SELECT * FROM compliance_items
            WHERE hotel_code=?
            ORDER BY next_date IS NULL, next_date, asset
        """, conn, params=(hotel_code,))
    else:
        df = pd.read_sql_query("""
            SELECT * FROM compliance_items
            ORDER BY hotel_code, next_date IS NULL, next_date, asset
        """, conn)
    conn.close()
    return df


def compliance_kpis(hotel_code: Optional[str] = None, warn_days: int = 30):
    df = compliance_df(hotel_code)
    statuses = {"Überfällig": 0, "Fällig": 0, "Bald fällig": 0, "OK": 0, "—": 0}
    td = today()
    for _, r in df.iterrows():
        nd = parse_date(r["next_date"])
        if not nd:
            statuses["—"] += 1
            continue
        days = (nd - td).days
        statuses[status_from_days(days, warn_days)] += 1
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
    """, (
        interval_months,
        last_d.isoformat() if last_d else None,
        next_d.isoformat() if next_d else None,
        owner_name,
        evidence_link,
        notes,
        now,
        item_id
    ))
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
# Audit logic
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


def list_audits(hotel_code: Optional[str] = None) -> pd.DataFrame:
    conn = db()
    if hotel_code:
        df = pd.read_sql_query("""
            SELECT * FROM audits
            WHERE hotel_code=?
            ORDER BY audit_date DESC, created_at DESC
        """, conn, params=(hotel_code,))
    else:
        df = pd.read_sql_query("""
            SELECT * FROM audits
            ORDER BY audit_date DESC, created_at DESC
        """, conn)
    conn.close()
    return df


def get_audit(audit_id: int) -> Dict:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM audits WHERE id=?", (audit_id,))
    r = cur.fetchone()
    conn.close()
    return dict(r) if r else {}


def ensure_audit_answers(audit_id: int, norm: str, hotel_code: str):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    hotel_city = get_hotel_city(hotel_code)

    cur.execute("""
        SELECT id
        FROM audit_questions
        WHERE norm=?
          AND is_active=1
          AND (city_profile='ALL' OR city_profile=?)
    """, (norm, hotel_city))

    qids = [row["id"] for row in cur.fetchall()]
    for qid in qids:
        cur.execute("""
            INSERT OR IGNORE INTO audit_answers(audit_id,question_id,score,deviation,deviation_type,evidence,notes,updated_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (audit_id, qid, "", "", "", "", "", now))

    conn.commit()
    conn.close()


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
    conn.commit()
    conn.close()

    ensure_audit_answers(audit_id, norm, hotel_code)
    return acode


def delete_audit(audit_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM audits WHERE id=?", (audit_id,))
    conn.commit()
    conn.close()


def audit_questions_answers(audit_id: int) -> pd.DataFrame:
    conn = db()
    df = pd.read_sql_query("""
        SELECT aq.id as question_id,
               aq.norm,
               aq.chapter,
               aq.clause,
               aq.topic_group,
               aq.city_profile,
               aq.question,
               aq.evidence_hint,
               aa.id as answer_id,
               aa.score,
               aa.deviation,
               aa.deviation_type,
               aa.evidence,
               aa.notes,
               aa.updated_at
        FROM audit_answers aa
        JOIN audit_questions aq ON aq.id = aa.question_id
        WHERE aa.audit_id=?
        ORDER BY aq.chapter, aq.topic_group, aq.clause, aq.id
    """, conn, params=(audit_id,))
    conn.close()
    return df


def update_audit_answer(answer_id: int, score: str, deviation: str, deviation_type: str, evidence: str, notes: str):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    if deviation != "Ja":
        deviation_type = ""
    cur.execute("""
        UPDATE audit_answers
        SET score=?, deviation=?, deviation_type=?, evidence=?, notes=?, updated_at=?
        WHERE id=?
    """, (score, deviation, deviation_type, evidence, notes, now, answer_id))
    conn.commit()
    conn.close()


def recompute_audit_score(audit_id: int) -> Optional[float]:
    df = audit_questions_answers(audit_id)
    vals = [int(s) for s in df["score"].tolist() if s in ("0", "1", "2")]
    score = round(sum(vals) / len(vals), 2) if vals else None
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
def list_actions(hotel_code: Optional[str] = None) -> pd.DataFrame:
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


def create_action(hotel_code, audit_id, title, category, owner_name, due_date_str, status,
                  notes="", risk_level="", immediate_action="", root_cause="", corrective_action=""):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute("""
        INSERT INTO actions(
          hotel_code,audit_id,title,category,owner_name,due_date,status,
          effectiveness_date,effectiveness_result,risk_level,immediate_action,root_cause,corrective_action,
          notes,created_at,updated_at
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        hotel_code, audit_id, title, category, owner_name, due_date_str or None, status,
        None, None, risk_level, immediate_action, root_cause, corrective_action,
        notes, now, now
    ))
    conn.commit()
    conn.close()


def update_action(action_id: int, title, category, owner_name, due_date_str, status,
                  effectiveness_date_str, effectiveness_result,
                  risk_level, immediate_action, root_cause, corrective_action, notes):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    cur.execute("""
        UPDATE actions
        SET title=?, category=?, owner_name=?, due_date=?, status=?,
            effectiveness_date=?, effectiveness_result=?,
            risk_level=?, immediate_action=?, root_cause=?, corrective_action=?,
            notes=?, updated_at=?
        WHERE id=?
    """, (
        title, category, owner_name, due_date_str or None, status,
        effectiveness_date_str or None, effectiveness_result or None,
        risk_level, immediate_action, root_cause, corrective_action,
        notes, now, action_id
    ))
    conn.commit()
    conn.close()


def delete_action(action_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM actions WHERE id=?", (action_id,))
    conn.commit()
    conn.close()


# ---------------------------
# Audit program
# ---------------------------
def list_program(hotel_code: Optional[str] = None) -> pd.DataFrame:
    conn = db()
    if hotel_code:
        df = pd.read_sql_query("""
            SELECT * FROM audit_program WHERE hotel_code=?
            ORDER BY planned_date, norm, area
        """, conn, params=(hotel_code,))
    else:
        df = pd.read_sql_query("""
            SELECT * FROM audit_program
            ORDER BY planned_date, hotel_code, norm, area
        """, conn)
    conn.close()
    return df


def upsert_program_row(row_id: Optional[int], hotel_code, norm, area, planned_date_str, owner_name,
                      status, reminder_days, notes):
    conn = db()
    cur = conn.cursor()
    now = utc_now_iso()
    if row_id:
        cur.execute("""
            UPDATE audit_program
            SET hotel_code=?, norm=?, area=?, planned_date=?, owner_name=?, status=?, reminder_days=?, notes=?, updated_at=?
            WHERE id=?
        """, (hotel_code, norm, area, planned_date_str, owner_name, status, int(reminder_days), notes, now, int(row_id)))
    else:
        cur.execute("""
            INSERT INTO audit_program(hotel_code,norm,area,planned_date,owner_name,status,reminder_days,notes,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (hotel_code, norm, area, planned_date_str, owner_name, status, int(reminder_days), notes, now, now))
    conn.commit()
    conn.close()


def delete_program_row(row_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM audit_program WHERE id=?", (row_id,))
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
        cols = st.columns([3, 2, 2, 1, 1])
        cols[0].write(f"📎 **{r['filename']}**")
        cols[1].write(r.get("uploaded_by") or "")
        cols[2].write((r.get("uploaded_at") or "")[:19].replace("T", " "))
        try:
            with open(r["stored_path"], "rb") as f:
                data = f.read()
            cols[3].download_button("Download", data, file_name=r["filename"], mime=r.get("mime_type") or "application/octet-stream")
        except Exception:
            cols[3].write("—")
        if role_in("Admin") and cols[4].button("Löschen", key=f"del_att_{r['id']}"):
            delete_attachment(int(r["id"]))
            st.success("Anhang gelöscht.")
            st.rerun()


# ---------------------------
# PDF export
# ---------------------------
def wrap_text(text: str, max_chars: int) -> List[str]:
    words = (text or "").split()
    lines, line = [], ""
    for w in words:
        if len(line) + len(w) + 1 <= max_chars:
            line = (line + " " + w).strip()
        else:
            lines.append(line)
            line = w
    if line:
        lines.append(line)
    return lines or [""]


def make_audit_pdf(audit: Dict, dfq: pd.DataFrame, hotel_name: str) -> bytes:
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    def header(y):
        c.setFont("Helvetica-Bold", 14)
        c.drawString(20 * mm, y, "Auditbericht")
        c.setFont("Helvetica", 10)
        c.drawRightString(width - 20 * mm, y, datetime.now().strftime("%d.%m.%Y %H:%M"))
        return y - 10 * mm

    y = height - 20 * mm
    y = header(y)

    c.setFont("Helvetica-Bold", 11)
    c.drawString(20 * mm, y, f"Audit: {audit.get('audit_code','')}")
    y -= 6 * mm
    c.setFont("Helvetica", 10)
    c.drawString(20 * mm, y, f"Hotel: {audit.get('hotel_code','')} – {hotel_name}")
    y -= 6 * mm
    c.drawString(20 * mm, y, f"Norm: {audit.get('norm','')}   Bereich: {audit.get('area','')}")
    y -= 6 * mm
    c.drawString(20 * mm, y, f"Auditdatum: {fmt_date(parse_date(audit.get('audit_date')))}   Auditor: {audit.get('auditor_name') or ''}")
    y -= 6 * mm
    c.drawString(20 * mm, y, f"Status: {audit.get('status','')}   Score: {audit.get('score') if audit.get('score') is not None else '—'}")
    y -= 10 * mm

    c.setFont("Helvetica-Bold", 11)
    c.drawString(20 * mm, y, "Zusammenfassung")
    y -= 6 * mm
    c.setFont("Helvetica", 10)
    for line in wrap_text(audit.get("summary") or "", 95):
        c.drawString(20 * mm, y, line)
        y -= 5 * mm
        if y < 20 * mm:
            c.showPage()
            y = height - 20 * mm
            y = header(y)

    y -= 4 * mm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(20 * mm, y, "Checkliste & Ergebnisse (Auszug)")
    y -= 8 * mm

    c.setFont("Helvetica-Bold", 9)
    c.drawString(20 * mm, y, "Clause")
    c.drawString(40 * mm, y, "Bew.")
    c.drawString(55 * mm, y, "Typ")
    c.drawString(70 * mm, y, "Frage (gekürzt)")
    y -= 5 * mm
    c.setFont("Helvetica", 9)

    for _, row in dfq.iterrows():
        clause = (row.get("clause") or row.get("chapter") or "")
        sc = row.get("score") or ""
        dtype = (row.get("deviation_type") or "") if (row.get("deviation") == "Ja") else ""
        qtext = (row.get("question") or "")[:120]
        c.drawString(20 * mm, y, str(clause))
        c.drawString(40 * mm, y, sc)
        c.drawString(55 * mm, y, dtype)
        for line in wrap_text(qtext, 70):
            c.drawString(70 * mm, y, line)
            y -= 4.5 * mm
            if y < 20 * mm:
                c.showPage()
                y = height - 20 * mm
                y = header(y)
                c.setFont("Helvetica", 9)
        y -= 2 * mm

    c.showPage()
    c.save()
    return buf.getvalue()


# ---------------------------
# Digest notifications
# ---------------------------
_graph_token_cache = {"token": None, "expires_at": 0}


def graph_get_token() -> Optional[str]:
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


def program_digest(hotel_code: Optional[str], days_ahead: int = 30) -> Dict:
    df = list_program(hotel_code)
    td = today()
    items = []
    for _, r in df.iterrows():
        if r["status"] != "Geplant":
            continue
        pd_ = parse_date(r["planned_date"])
        if not pd_:
            continue
        delta = (pd_ - td).days
        if delta < 0 or delta <= days_ahead:
            items.append({
                "hotel": r["hotel_code"],
                "norm": r["norm"],
                "area": r["area"],
                "planned": pd_,
                "days": delta,
                "owner": r["owner_name"] or ""
            })
    items.sort(key=lambda x: (0 if x["days"] < 0 else 1, x["days"], x["hotel"], x["norm"]))
    return {"items": items, "count": len(items)}


def actions_digest(hotel_code: Optional[str]) -> Dict:
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
                "owner": r["owner_name"] or ""
            })
    items.sort(key=lambda x: (0 if x["overdue"] else 1, x["due"] or date(2999, 1, 1)))
    return {"items": items, "count": len(items)}


def send_digest(to_emails: List[str], hotel_filter: Optional[str], warn_days: int,
                days_ahead_program: int, send_mail: bool, send_teams: bool, hotels_df: pd.DataFrame) -> Dict[str, bool]:
    labels = hotel_label_map(hotels_df)
    comp = compliance_digest(hotel_filter, warn_days)
    acts = actions_digest(hotel_filter)
    prog = program_digest(hotel_filter, days_ahead_program)

    title = f"Audit/Compliance Digest – {fmt_date(today())}"
    scope = "Alle Hotels" if not hotel_filter else labels.get(hotel_filter, hotel_filter)
    link_hint = f"<p>App: <a href='{APP_BASE_URL}'>{APP_BASE_URL}</a></p>" if APP_BASE_URL else ""

    html = f"<html><body><h2>{title}</h2><p><b>{scope}</b></p>{link_hint}"
    html += f"<h3>Auditprogramm: {prog['count']}</h3><ul>"
    for it in prog["items"][:30]:
        state = "Überfällig" if it["days"] < 0 else f"in {it['days']} Tagen"
        html += f"<li><b>{labels.get(it['hotel'], it['hotel'])}</b> – {it['norm']} – {it['area']} – {fmt_date(it['planned'])} ({state})</li>"
    html += "</ul>"

    html += f"<h3>Betreiberpflichten: {comp['count']}</h3><ul>"
    for it in comp["items"][:30]:
        html += f"<li><b>{labels.get(it['hotel'], it['hotel'])}</b> – {it['asset']} / {it['task']} – {it['status']} – {fmt_date(it['next'])}</li>"
    html += "</ul>"

    html += f"<h3>Offene Maßnahmen: {acts['count']}</h3><ul>"
    for it in acts["items"][:30]:
        html += f"<li><b>{labels.get(it['hotel'], it['hotel'])}</b> – [{it['category']}] {it['title']} – {fmt_date(it['due'])}</li>"
    html += "</ul></body></html>"

    text = f"**{title}**\n\n**{scope}**\n\n"
    text += f"**Auditprogramm:** {prog['count']}\n"
    for it in prog["items"][:10]:
        state = "Überfällig" if it["days"] < 0 else f"in {it['days']} Tagen"
        text += f"- {labels.get(it['hotel'], it['hotel'])}: {it['norm']} / {it['area']} ({state})\n"
    text += f"\n**Betreiberpflichten:** {comp['count']}\n"
    for it in comp["items"][:10]:
        text += f"- {labels.get(it['hotel'], it['hotel'])}: {it['asset']} / {it['task']} ({it['status']})\n"
    text += f"\n**Maßnahmen:** {acts['count']}\n"
    for it in acts["items"][:10]:
        text += f"- {labels.get(it['hotel'], it['hotel'])}: [{it['category']}] {it['title']}\n"

    out = {"mail": False, "teams": False}
    if send_mail:
        out["mail"] = graph_send_mail(to_emails, title, html)
    if send_teams:
        out["teams"] = teams_post_message(title, text)
    return out


# ---------------------------
# UI shared
# ---------------------------
def login_ui():
    st.subheader("Login")
    with st.form("login_form", clear_on_submit=False):
        email = st.text_input("E-Mail", value=st.session_state.get("login_email", ""))
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


def header_ui(hotels_df: pd.DataFrame):
    u = st.session_state.get("user")
    labels = hotel_label_map(hotels_df)
    cols = st.columns([3, 2, 1])
    with cols[0]:
        st.title(APP_TITLE)
    with cols[1]:
        if u:
            hotel_txt = "Alle" if not u.get("hotel_code") else labels.get(u["hotel_code"], u["hotel_code"])
            st.caption(f"Angemeldet: **{u['name']}** · Rolle: **{u['role']}** · Hotel: **{hotel_txt}**")
    with cols[2]:
        if u and st.button("Logout"):
            st.session_state["user"] = None
            st.rerun()


def select_hotel_filter(hotels_df: pd.DataFrame) -> Optional[str]:
    u = st.session_state.get("user")
    labels = hotel_label_map(hotels_df)

    if u["role"] in ("Direktor", "Techniker"):
        return u["hotel_code"]

    options = ["Alle"] + hotels_df["code"].tolist()
    sel = st.selectbox("Hotel-Filter", options, index=0, format_func=lambda x: "Alle" if x == "Alle" else labels.get(x, x))
    return None if sel == "Alle" else sel


# ---------------------------
# Pages
# ---------------------------
def page_dashboard(hotels_df: pd.DataFrame):
    require_login()
    labels = hotel_label_map(hotels_df)
    st.subheader("Dashboard")

    warn_days = st.slider("Warnschwelle Betreiberpflichten (Tage bis fällig)", 7, 90, 30, 1)
    days_ahead_program = st.slider("Auditprogramm: Zeige fällige Audits in den nächsten (Tagen)", 7, 120, 30, 1)
    hotel_filter = select_hotel_filter(hotels_df)

    st.markdown("### KPI Betreiberpflichten")
    statuses, total = compliance_kpis(hotel_filter, warn_days=warn_days)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Überfällig", statuses["Überfällig"])
    c2.metric("Fällig", statuses["Fällig"])
    c3.metric("Bald fällig", statuses["Bald fällig"])
    c4.metric("OK", statuses["OK"])
    c5.metric("Gesamt", total)

    st.divider()
    st.markdown("### KPI Maßnahmen")
    acts = list_actions(hotel_filter)
    td = today()
    open_acts = acts[acts["status"] != "Erledigt"] if len(acts) else acts
    overdue = 0
    if len(open_acts):
        for dd in open_acts["due_date"].tolist():
            d = parse_date(dd)
            if d and d < td:
                overdue += 1
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Offen", int((acts["status"] == "Offen").sum()) if len(acts) else 0)
    a2.metric("In Bearbeitung", int((acts["status"] == "In Bearbeitung").sum()) if len(acts) else 0)
    a3.metric("Wirksamkeit offen", int((acts["status"] == "Wirksamkeit offen").sum()) if len(acts) else 0)
    a4.metric("Überfällig", overdue)

    st.divider()
    st.markdown("### Auditprogramm (nächste Audits)")
    prog = program_digest(hotel_filter, days_ahead_program)
    if prog["count"] == 0:
        st.info("Keine fälligen geplanten Audits im Zeitraum.")
    else:
        view = pd.DataFrame([{
            "Hotel": labels.get(it["hotel"], it["hotel"]),
            "Norm": it["norm"],
            "Bereich": it["area"],
            "Geplant": fmt_date(it["planned"]),
            "Tage": it["days"],
            "Owner": it["owner"]
        } for it in prog["items"][:50]])
        st.dataframe(view, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### Digest senden")
    send_mail = st.checkbox("Outlook E-Mail senden (Graph)", value=bool(MS_TENANT_ID and MAIL_SENDER_UPN))
    send_teams = st.checkbox("Teams Nachricht senden (Webhook)", value=bool(TEAMS_WEBHOOK_URL))
    to_emails = st.text_input("Empfänger (Komma-separiert)", value=st.session_state.get("digest_to", ""))
    if st.button("Digest jetzt senden"):
        recipients = [e.strip() for e in to_emails.split(",") if e.strip()]
        st.session_state["digest_to"] = to_emails
        if send_mail and not recipients:
            st.error("Bitte E-Mail Empfänger eintragen.")
        else:
            res = send_digest(recipients, hotel_filter, warn_days, days_ahead_program, send_mail, send_teams, hotels_df)
            st.success(f"Ergebnis: Mail={res['mail']} · Teams={res['teams']}")


def page_auditprogramm(hotels_df: pd.DataFrame):
    require_login()
    labels = hotel_label_map(hotels_df)
    st.subheader("Auditprogramm / Jahresplan")

    hotel_filter = select_hotel_filter(hotels_df)
    df = list_program(hotel_filter)

    if len(df):
        show = df.copy()
        show["Hotel"] = show["hotel_code"].apply(lambda x: labels.get(x, x))
        show["Geplant"] = show["planned_date"].apply(lambda x: fmt_date(parse_date(x)))
        show = show[["id", "Hotel", "norm", "area", "Geplant", "status", "reminder_days", "owner_name", "notes"]]
        st.dataframe(show, use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine Einträge im Auditprogramm.")

    st.divider()
    st.markdown("### Eintrag anlegen / bearbeiten")
    ids = df["id"].tolist() if len(df) else []
    sel = st.selectbox("Auswählen", options=["Neu"] + ids, index=0)

    if sel == "Neu":
        with st.form("prog_new"):
            if role_in("Direktor", "Techniker"):
                hc_opts = [st.session_state["user"]["hotel_code"]]
            else:
                hc_opts = hotels_df["code"].tolist()
            hc = st.selectbox("Hotel", hc_opts, format_func=lambda x: labels.get(x, x))
            norm = st.selectbox("Norm", ["BETREIBERPFLICHTEN", "ISO 50001", "ISO 14001", "ISO 45001", "ISO 9001"])
            area = st.text_input("Bereich/Prozess", "Technik")
            planned = st.date_input("Geplantes Datum", value=today() + timedelta(days=30))
            owner = st.text_input("Owner", "")
            status = st.selectbox("Status", PROGRAM_STATUSES, index=0)
            reminder_days = st.number_input("Reminder (Tage vorher)", 1, 90, 14, 1)
            notes = st.text_area("Notizen", "", height=90)
            ok = st.form_submit_button("Anlegen")
        if ok:
            if not can_access_hotel(hc):
                st.error("Keine Berechtigung.")
            else:
                upsert_program_row(None, hc, norm, area.strip(), planned.isoformat(), owner.strip(), status, reminder_days, notes)
                st.success("Angelegt.")
                st.rerun()
    else:
        row = df[df["id"] == sel].iloc[0].to_dict()
        if not can_access_hotel(row["hotel_code"]):
            st.error("Keine Berechtigung.")
            return
        with st.form("prog_edit"):
            st.write(f"**Hotel:** {labels.get(row['hotel_code'], row['hotel_code'])}")
            norm = st.selectbox("Norm", ["BETREIBERPFLICHTEN", "ISO 50001", "ISO 14001", "ISO 45001", "ISO 9001"],
                                index=["BETREIBERPFLICHTEN", "ISO 50001", "ISO 14001", "ISO 45001", "ISO 9001"].index(row["norm"]))
            area = st.text_input("Bereich/Prozess", row["area"])
            planned = st.date_input("Geplantes Datum", value=parse_date(row["planned_date"]) or today())
            owner = st.text_input("Owner", row["owner_name"] or "")
            status = st.selectbox("Status", PROGRAM_STATUSES, index=PROGRAM_STATUSES.index(row["status"]))
            reminder_days = st.number_input("Reminder (Tage vorher)", 1, 90, int(row["reminder_days"] or 14), 1)
            notes = st.text_area("Notizen", row["notes"] or "", height=90)
            c1, c2 = st.columns(2)
            save = c1.form_submit_button("Speichern")
            delete = c2.form_submit_button("Löschen")
        if save:
            upsert_program_row(int(sel), row["hotel_code"], norm, area.strip(), planned.isoformat(), owner.strip(), status, reminder_days, notes)
            st.success("Gespeichert.")
            st.rerun()
        if delete:
            if not role_in("Admin"):
                st.error("Löschen nur Admin.")
            else:
                delete_program_row(int(sel))
                st.success("Gelöscht.")
                st.rerun()

        st.divider()
        st.markdown("### Anhänge (Auditprogramm)")
        upload_attachment_ui(row["hotel_code"], "program", int(sel))
        attachments_list_ui(row["hotel_code"], "program", int(sel))


def page_betreiberpflichten(hotels_df: pd.DataFrame):
    require_login()
    labels = hotel_label_map(hotels_df)
    st.subheader("Betreiberpflichten / Prüfkalender")

    warn_days = st.slider("Warnschwelle (Tage bis fällig)", 7, 90, 30, 1, key="warn_ops")
    hotel_filter = select_hotel_filter(hotels_df)

    df = compliance_df(hotel_filter)
    td = today()
    rows = []
    for _, r in df.iterrows():
        nd = parse_date(r["next_date"])
        days = (nd - td).days if nd else None
        stt = status_from_days(days, warn_days=warn_days) if nd else "—"
        rows.append({
            "ID": r["id"],
            "Hotel": labels.get(r["hotel_code"], r["hotel_code"]),
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
        view = view.sort_values(["sev", "days_sort", "Hotel", "Anlage"]).drop(columns=["sev", "days_sort"])
    st.dataframe(view, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### Eintrag bearbeiten")
    ids = df["id"].tolist() if len(df) else []
    sel = st.selectbox("Betreiberpflicht auswählen", options=["—"] + ids, index=0)
    if sel != "—":
        row = df[df["id"] == sel].iloc[0].to_dict()
        if not can_access_hotel(row["hotel_code"]):
            st.error("Keine Berechtigung.")
            return
        with st.form("ops_edit"):
            st.write(f"**Hotel:** {labels.get(row['hotel_code'], row['hotel_code'])}")
            st.write(f"**Anlage:** {row['asset']} · **Aufgabe:** {row['task']}")
            interval = st.number_input("Intervall (Monate)", 1, 120, int(row["interval_months"]), 1)
            last = st.date_input("Letzte Prüfung", value=parse_date(row["last_date"]) or today())
            owner = st.text_input("Owner", row.get("owner_name") or "")
            link = st.text_input("Link / Nachweis", row.get("evidence_link") or "")
            notes = st.text_area("Notizen", row.get("notes") or "", height=90)
            c1, c2 = st.columns(2)
            save = c1.form_submit_button("Speichern")
            delete = c2.form_submit_button("Löschen")
        if save:
            update_compliance_item(int(sel), int(interval), last.isoformat(), owner, link, notes)
            st.success("Gespeichert.")
            st.rerun()
        if delete:
            if not role_in("Admin"):
                st.error("Löschen nur Admin.")
            else:
                delete_compliance_item(int(sel))
                st.success("Gelöscht.")
                st.rerun()

        st.divider()
        st.markdown("### Anhänge (Betreiberpflicht)")
        upload_attachment_ui(row["hotel_code"], "compliance", int(sel))
        attachments_list_ui(row["hotel_code"], "compliance", int(sel))

    st.divider()
    st.markdown("### Neue Betreiberpflicht anlegen")
    with st.form("ops_new"):
        if role_in("Direktor", "Techniker"):
            hc_opts = [st.session_state["user"]["hotel_code"]]
        else:
            hc_opts = hotels_df["code"].tolist()
        hc = st.selectbox("Hotel", hc_opts, format_func=lambda x: labels.get(x, x))
        asset = st.text_input("Anlage", "")
        task = st.text_input("Aufgabe", "")
        interval = st.number_input("Intervall (Monate)", 1, 120, 12, 1)
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


def page_audits(hotels_df: pd.DataFrame):
    require_login()
    labels = hotel_label_map(hotels_df)
    st.subheader("Audits")

    hotel_filter = select_hotel_filter(hotels_df)
    df = list_audits(hotel_filter)

    if len(df):
        show = df.copy()
        show["Hotel"] = show["hotel_code"].apply(lambda x: labels.get(x, x))
        show["Auditdatum"] = show["audit_date"].apply(lambda x: fmt_date(parse_date(x)))
        show = show[["id", "audit_code", "Hotel", "norm", "area", "auditor_name", "Auditdatum", "status", "score"]]
        st.dataframe(show, use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine Audits vorhanden.")

    st.divider()
    st.markdown("### Audit anlegen")
    with st.form("audit_new"):
        if role_in("Direktor", "Techniker"):
            hc_opts = [st.session_state["user"]["hotel_code"]]
        else:
            hc_opts = hotels_df["code"].tolist()
        hc = st.selectbox("Hotel", hc_opts, format_func=lambda x: labels.get(x, x))
        norm = st.selectbox("Norm", ["BETREIBERPFLICHTEN", "ISO 50001", "ISO 14001", "ISO 45001", "ISO 9001"])
        area = st.text_input("Bereich/Prozess", "Technik")
        auditor = st.text_input("Auditor", st.session_state["user"]["name"])
        adate = st.date_input("Auditdatum", value=today())
        status = st.selectbox("Status", ["Geplant", "Durchgeführt", "Abgeschlossen"], index=1)
        ok = st.form_submit_button("Audit anlegen")
    if ok:
        if not can_access_hotel(hc):
            st.error("Keine Berechtigung.")
        else:
            code = create_audit(hc, norm, area.strip(), auditor.strip(), adate.isoformat(), status)
            st.success(f"Audit angelegt: {code}")
            st.rerun()

    st.divider()
    st.markdown("### Audit bearbeiten / Auditbogen")
    ids = df["id"].tolist() if len(df) else []
    sel = st.selectbox("Audit auswählen", options=["—"] + ids, index=0)
    if sel == "—":
        return

    audit = get_audit(int(sel))
    if not can_access_hotel(audit["hotel_code"]):
        st.error("Keine Berechtigung.")
        return

    ensure_audit_answers(int(sel), audit["norm"], audit["hotel_code"])

    with st.expander("Audit Stammdaten", expanded=True):
        with st.form("audit_meta"):
            st.write(f"**Audit Code:** {audit['audit_code']}")
            st.write(f"**Hotel:** {labels.get(audit['hotel_code'], audit['hotel_code'])}")
            status = st.selectbox("Status", ["Geplant", "Durchgeführt", "Abgeschlossen"],
                                  index=["Geplant", "Durchgeführt", "Abgeschlossen"].index(audit["status"]))
            adate = st.date_input("Auditdatum", value=parse_date(audit["audit_date"]) or today())
            auditor = st.text_input("Auditor", value=audit.get("auditor_name") or "")
            summary = st.text_area("Zusammenfassung", value=audit.get("summary") or "", height=120)
            c1, c2, c3 = st.columns(3)
            save = c1.form_submit_button("Speichern")
            delete_btn = c2.form_submit_button("Audit löschen")
            score_btn = c3.form_submit_button("Score neu berechnen")
        if save:
            update_audit_meta(int(sel), status, adate.isoformat(), auditor.strip(), summary)
            st.success("Gespeichert.")
            st.rerun()
        if delete_btn:
            if not role_in("Admin"):
                st.error("Löschen nur Admin.")
            else:
                delete_audit(int(sel))
                st.success("Audit gelöscht.")
                st.rerun()
        if score_btn:
            sc = recompute_audit_score(int(sel))
            st.success(f"Score aktualisiert: {sc if sc is not None else '—'}")
            st.rerun()

    with st.expander("Audit PDF / Anhänge", expanded=False):
        dfq = audit_questions_answers(int(sel))
        hotels = hotels_df.set_index("code")
        hotel_name = hotels.loc[audit["hotel_code"], "name"] if audit["hotel_code"] in hotels.index else audit["hotel_code"]
        pdf_bytes = make_audit_pdf(audit, dfq, str(hotel_name))
        st.download_button("Auditbericht als PDF", pdf_bytes, file_name=f"{audit['audit_code']}.pdf", mime="application/pdf")

        st.markdown("#### Anhänge (Audit)")
        upload_attachment_ui(audit["hotel_code"], "audit", int(sel))
        attachments_list_ui(audit["hotel_code"], "audit", int(sel))

    st.markdown("### Auditbogen")
    dfq = audit_questions_answers(int(sel))
    chapters = sorted(dfq["chapter"].dropna().unique().tolist())
    selected_chapters = st.multiselect("Kapitel filtern", chapters, default=chapters)
    if selected_chapters:
        dfq_view = dfq[dfq["chapter"].isin(selected_chapters)].copy()
    else:
        dfq_view = dfq.copy()

    last_group = None
    for _, row in dfq_view.iterrows():
        group = row.get("topic_group") or "Allgemein"
        if group != last_group:
            st.markdown(f"## {group}")
            last_group = group

        with st.container(border=True):
            clause = row.get("clause") or row.get("chapter") or ""
            st.markdown(f"**{row['norm']} · {clause}**")
            if row.get("city_profile") and row["city_profile"] != "ALL":
                st.caption(f"Stadtprofil: {row['city_profile']}")
            st.write(row["question"])
            if row.get("evidence_hint"):
                st.caption(f"Prüfhinweise/Nachweise: {row['evidence_hint']}")

            c1, c2, c3 = st.columns(3)
            score = c1.selectbox("Bewertung", ["", "0", "1", "2"], index=["", "0", "1", "2"].index(row["score"] or ""), key=f"sc_{row['answer_id']}")
            deviation = c2.selectbox("Abweichung?", ["", "Nein", "Ja"], index=["", "Nein", "Ja"].index(row["deviation"] or ""), key=f"dev_{row['answer_id']}")
            dtype = c3.selectbox("Typ", ["", "OFI", "Minor", "Major"], index=["", "OFI", "Minor", "Major"].index(row["deviation_type"] or ""), key=f"dt_{row['answer_id']}")

            evidence = st.text_area("Objektiver Nachweis", value=row["evidence"] or "", height=70, key=f"ev_{row['answer_id']}")
            notes = st.text_area("Notizen", value=row["notes"] or "", height=70, key=f"no_{row['answer_id']}")

            b1, b2 = st.columns(2)
            if b1.button("Speichern", key=f"save_{row['answer_id']}"):
                update_audit_answer(int(row["answer_id"]), score, deviation, dtype, evidence.strip(), notes.strip())
                recompute_audit_score(int(sel))
                st.success("Gespeichert.")
                st.rerun()

            if deviation == "Ja" and dtype in ("OFI", "Minor", "Major"):
                if b2.button(f"➡️ Maßnahme aus {dtype} erzeugen", key=f"mkact_{row['answer_id']}"):
                    cat = "Verbesserung" if dtype == "OFI" else ("Minor" if dtype == "Minor" else "Major")
                    default_due_days = 7 if dtype == "Major" else 21
                    default_risk = "Hoch" if dtype == "Major" else ("Mittel" if dtype == "Minor" else "")
                    title = f"{audit['audit_code']} – {audit['norm']} {clause}: {row['question'][:80]}..."
                    create_action(
                        hotel_code=audit["hotel_code"],
                        audit_id=int(sel),
                        title=title,
                        category=cat,
                        owner_name="",
                        due_date_str=(today() + timedelta(days=default_due_days)).isoformat(),
                        status="Offen",
                        notes=f"Abweichungstyp: {dtype}\nStadtprofil: {row.get('city_profile') or 'ALL'}\n\nNachweis:\n{evidence.strip()}\n\nNotizen:\n{notes.strip()}",
                        risk_level=default_risk,
                        immediate_action="",
                        root_cause="",
                        corrective_action=""
                    )
                    st.success("Maßnahme erzeugt.")
                    st.rerun()


def page_actions(hotels_df: pd.DataFrame):
    require_login()
    labels = hotel_label_map(hotels_df)
    st.subheader("Maßnahmen")

    hotel_filter = select_hotel_filter(hotels_df)
    df = list_actions(hotel_filter)

    if len(df):
        show = df.copy()
        show["Hotel"] = show["hotel_code"].apply(lambda x: labels.get(x, x))
        show["Frist"] = show["due_date"].apply(lambda x: fmt_date(parse_date(x)))
        show["Wirksamkeit"] = show["effectiveness_date"].apply(lambda x: fmt_date(parse_date(x)))
        show = show[["id", "Hotel", "audit_code", "category", "title", "status", "Frist", "owner_name", "risk_level", "Wirksamkeit"]]
        st.dataframe(show, use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine Maßnahmen.")

    st.divider()
    st.markdown("### Neue Maßnahme anlegen")
    with st.form("act_new"):
        if role_in("Direktor", "Techniker"):
            hc_opts = [st.session_state["user"]["hotel_code"]]
        else:
            hc_opts = hotels_df["code"].tolist()
        hc = st.selectbox("Hotel", hc_opts, format_func=lambda x: labels.get(x, x))
        title = st.text_input("Titel", "")
        category = st.selectbox("Kategorie", ACTION_CATEGORIES, index=3)
        owner = st.text_input("Owner", "")
        due = st.date_input("Frist", value=today() + timedelta(days=14))
        status = st.selectbox("Status", ACTION_STATUSES, index=0)
        notes = st.text_area("Notizen", "", height=90)

        risk = st.selectbox("Risiko-Level", RISK_LEVELS, index=0)
        immediate = st.text_area("Sofortmaßnahme", "", height=70)
        root = st.text_area("Ursachenanalyse", "", height=70)
        corr = st.text_area("Korrekturmaßnahme", "", height=70)

        ok = st.form_submit_button("Anlegen")
    if ok:
        if not can_access_hotel(hc):
            st.error("Keine Berechtigung.")
        elif not title.strip():
            st.error("Bitte Titel ausfüllen.")
        else:
            if category == "Major":
                missing = []
                if not risk:
                    missing.append("Risiko-Level")
                if not immediate.strip():
                    missing.append("Sofortmaßnahme")
                if not root.strip():
                    missing.append("Ursachenanalyse")
                if not corr.strip():
                    missing.append("Korrekturmaßnahme")
                if missing:
                    st.error("Bei Major bitte ausfüllen: " + ", ".join(missing))
                    return

            create_action(hc, None, title.strip(), category, owner.strip(), due.isoformat(), status,
                          notes=notes, risk_level=risk, immediate_action=immediate, root_cause=root, corrective_action=corr)
            st.success("Maßnahme angelegt.")
            st.rerun()

    st.divider()
    st.markdown("### Maßnahme bearbeiten")
    ids = df["id"].tolist() if len(df) else []
    sel = st.selectbox("Maßnahme auswählen", options=["—"] + ids, index=0)
    if sel == "—":
        return
    row = df[df["id"] == sel].iloc[0].to_dict()
    if not can_access_hotel(row["hotel_code"]):
        st.error("Keine Berechtigung.")
        return

    with st.form("act_edit"):
        st.write(f"**Hotel:** {labels.get(row['hotel_code'], row['hotel_code'])}")
        st.write(f"**Audit:** {row.get('audit_code') or '—'}")
        title = st.text_input("Titel", row["title"])
        category = st.selectbox("Kategorie", ACTION_CATEGORIES, index=ACTION_CATEGORIES.index(row["category"]))
        owner = st.text_input("Owner", row.get("owner_name") or "")
        due = st.date_input("Frist", value=parse_date(row["due_date"]) or today())
        status = st.selectbox("Status", ACTION_STATUSES, index=ACTION_STATUSES.index(row["status"]))
        notes = st.text_area("Notizen", row.get("notes") or "", height=90)

        risk = st.selectbox("Risiko-Level", RISK_LEVELS, index=RISK_LEVELS.index(row.get("risk_level") or ""))
        immediate = st.text_area("Sofortmaßnahme", row.get("immediate_action") or "", height=70)
        root = st.text_area("Ursachenanalyse", row.get("root_cause") or "", height=70)
        corr = st.text_area("Korrekturmaßnahme", row.get("corrective_action") or "", height=70)

        eff_date = st.date_input("Wirksamkeitsdatum", value=parse_date(row["effectiveness_date"]) or today())
        eff_res = st.text_area("Wirksamkeitsergebnis", row.get("effectiveness_result") or "", height=70)

        c1, c2 = st.columns(2)
        save = c1.form_submit_button("Speichern")
        delete = c2.form_submit_button("Löschen")

    if save:
        if category == "Major":
            missing = []
            if not risk:
                missing.append("Risiko-Level")
            if not immediate.strip():
                missing.append("Sofortmaßnahme")
            if not root.strip():
                missing.append("Ursachenanalyse")
            if not corr.strip():
                missing.append("Korrekturmaßnahme")
            if missing:
                st.error("Bei Major bitte ausfüllen: " + ", ".join(missing))
                return

        if status == "Erledigt":
            if not eff_res.strip():
                st.error("Bei Status 'Erledigt' bitte Wirksamkeitsergebnis ausfüllen.")
                return
            eff_date_str = eff_date.isoformat()
            eff_res_str = eff_res.strip()
        else:
            eff_date_str = None
            eff_res_str = None

        update_action(
            int(sel),
            title.strip(),
            category,
            owner.strip(),
            due.isoformat() if due else None,
            status,
            eff_date_str,
            eff_res_str,
            risk,
            immediate.strip(),
            root.strip(),
            corr.strip(),
            notes
        )
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
    st.markdown("### Anhänge (Maßnahme)")
    upload_attachment_ui(row["hotel_code"], "action", int(sel))
    attachments_list_ui(row["hotel_code"], "action", int(sel))


def page_question_catalog(hotels_df: pd.DataFrame):
    require_login()
    if not role_in("Admin"):
        st.error("Nur Admin.")
        return

    st.subheader("Admin: Fragenkatalog")
    norm = st.selectbox("Norm", ["BETREIBERPFLICHTEN", "ISO 50001", "ISO 14001", "ISO 45001", "ISO 9001", "(Alle)"])
    df = list_audit_questions(None if norm == "(Alle)" else norm)

    if len(df):
        show = df.copy()
        show["active"] = show["is_active"].apply(lambda x: "Ja" if int(x) == 1 else "Nein")
        show = show[["id", "norm", "chapter", "clause", "topic_group", "city_profile", "active", "question"]]
        st.dataframe(show, use_container_width=True, hide_index=True)
    else:
        st.info("Keine Fragen vorhanden.")

    st.divider()
    st.markdown("### Kataloge importieren / ergänzen")
    c1, c2 = st.columns(2)
    if c1.button("BETREIBERPFLICHTEN import/update"):
        inserted = insert_questions_if_missing(build_betreiberpflichten_questions())
        st.success(f"Neu eingefügte Fragen: {inserted}")
        st.rerun()
    if c2.button("ISO-Kataloge import/update"):
        inserted = insert_questions_if_missing(build_iso_questions())
        st.success(f"Neu eingefügte Fragen: {inserted}")
        st.rerun()

    st.divider()
    st.markdown("### Neue Frage hinzufügen")
    with st.form("q_new"):
        n = st.selectbox("Norm", ["BETREIBERPFLICHTEN", "ISO 50001", "ISO 14001", "ISO 45001", "ISO 9001"])
        chapter = st.text_input("Kapitel", "")
        clause = st.text_input("Clause", "")
        topic_group = st.text_input("Themenblock", "")
        city_profile = st.selectbox("Stadtprofil", ["ALL", "Frankfurt", "München"])
        question = st.text_area("Frage", "", height=90)
        hint = st.text_area("Prüfhinweise/Nachweise", "", height=90)
        ok = st.form_submit_button("Hinzufügen")
    if ok:
        if not (chapter.strip() and question.strip()):
            st.error("Bitte mindestens Kapitel und Frage ausfüllen.")
        else:
            conn = db()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO audit_questions(norm,chapter,clause,topic_group,city_profile,question,evidence_hint,is_active)
                VALUES (?,?,?,?,?,?,?,1)
            """, (n, chapter.strip(), clause.strip(), topic_group.strip(), city_profile, question.strip(), hint.strip()))
            conn.commit()
            conn.close()
            st.success("Frage hinzugefügt.")
            st.rerun()

    st.divider()
    st.markdown("### Frage aktiv / inaktiv schalten")
    qids = df["id"].tolist() if len(df) else []
    sel = st.selectbox("Frage auswählen", options=["—"] + qids, index=0)
    if sel != "—":
        row = df[df["id"] == sel].iloc[0].to_dict()
        active = st.checkbox("Aktiv", value=bool(int(row["is_active"])))
        if st.button("Status speichern"):
            conn = db()
            cur = conn.cursor()
            cur.execute("UPDATE audit_questions SET is_active=? WHERE id=?", (1 if active else 0, int(sel)))
            conn.commit()
            conn.close()
            st.success("Gespeichert.")
            st.rerun()


def page_admin(hotels_df: pd.DataFrame):
    require_login()
    if not role_in("Admin"):
        st.error("Nur Admin.")
        return

    labels = hotel_label_map(hotels_df)
    st.subheader("Admin: Benutzerverwaltung")
    st.markdown("Standard Admin: **admin@local / admin123**")

    users = list_users()
    users_show = users.copy()
    users_show["Hotel"] = users_show["hotel_code"].apply(lambda x: labels.get(x, "") if x else "")
    users_show = users_show.drop(columns=["hotel_code"])
    st.dataframe(users_show, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### User anlegen / ändern")
    with st.form("user_upsert"):
        email = st.text_input("E-Mail", "")
        name = st.text_input("Name", "")
        role = st.selectbox("Rolle", ["Admin", "Auditor", "Direktor", "Techniker"])
        hotel = st.selectbox("Hotel (für Direktor/Techniker)", [""] + hotels_df["code"].tolist(),
                             format_func=lambda x: "" if x == "" else labels.get(x, x))
        pw = st.text_input("Neues Passwort (leer = unverändert)", type="password")
        active = st.checkbox("Aktiv", value=True)
        ok = st.form_submit_button("Speichern")
    if ok:
        if not email.strip() or not name.strip():
            st.error("Bitte E-Mail und Name ausfüllen.")
        else:
            h = hotel if role in ("Direktor", "Techniker") else None
            upsert_user(email.strip().lower(), name.strip(), role, h, pw if pw.strip() else None, active)
            st.success("User gespeichert.")
            st.rerun()


# ---------------------------
# Main
# ---------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")

    init_db()
    migrate_db()
    seed_if_empty()
    seed_city_specific_compliance_items()
    compute_and_store_next_dates()

    hotels_df = get_hotels()
    header_ui(hotels_df)

    if "user" not in st.session_state or not st.session_state["user"]:
        st.warning("Nicht eingeloggt.")
        st.info("Standard-Login: admin@local / admin123")
        login_ui()
        return

    pages = {
        "Dashboard": lambda: page_dashboard(hotels_df),
        "Auditprogramm": lambda: page_auditprogramm(hotels_df),
        "Betreiberpflichten": lambda: page_betreiberpflichten(hotels_df),
        "Audits": lambda: page_audits(hotels_df),
        "Maßnahmen": lambda: page_actions(hotels_df),
    }
    if role_in("Admin"):
        pages["Admin (User)"] = lambda: page_admin(hotels_df)
        pages["Admin (Fragenkatalog)"] = lambda: page_question_catalog(hotels_df)

    st.sidebar.radio("Navigation", list(pages.keys()), key="nav")
    st.sidebar.caption("Direktor/Techniker sehen automatisch nur ihr eigenes Hotel.")
    pages[st.session_state["nav"]]()


if __name__ == "__main__":
    main()
