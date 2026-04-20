from __future__ import annotations

import csv
import hashlib
import re
import uuid
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import pdfplumber
from fastapi import FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data.db"
UPLOADS_DIR = BASE_DIR / "uploads"
SYSTEM_ENVELOPE_NAME = "To Be Distributed"
DATE_PATTERN = re.compile(
    r"^(?:(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{1,2}|\d{1,2}/\d{1,2}/\d{2,4})\b"
)
AMOUNT_PATTERN = re.compile(r"-?(?:\d{1,3}(?:,\d{3})*)?(?:\.\d{2})")
ACCOUNT_HEADER_PATTERN = re.compile(
    r"(?P<name>[A-Za-z][A-Za-z &/.\-]*?)\s*-\s*[xX]+(?P<last4>\d{4})",
    re.IGNORECASE,
)
ACCOUNT_NUMBER_PATTERN = re.compile(r"Account Number:\s*[xX\s]*(\d{4,})", re.IGNORECASE)
ACCOUNT_ENDING_PATTERN = re.compile(
    r"(?:Account|Card)\s+Ending\s+([0-9xX\-\s]+)", re.IGNORECASE
)
ACCOUNT_ENDING_IN_PATTERN = re.compile(
    r"(?:Account|Card)\s+ending\s+in\s+(\d{4})", re.IGNORECASE
)
FOREIGN_CURRENCY_PATTERN = re.compile(
    r"\b(?:dollars|pesos|euros|pounds|yen|cad|usd|mxn|gbp|eur)\b",
    re.IGNORECASE,
)
OWNER_LINE_PATTERN = re.compile(r"^[A-Z][A-Z\s.'-]{3,}$")
OWNER_BLOCKLIST = {
    "ACCOUNT",
    "SUMMARY",
    "STATEMENT",
    "IMPORTANT",
    "BALANCE",
    "OVERDRAFT",
    "INTEREST",
    "CREDIT UNION",
    "METRO",
    "CHASE",
    "AMERICAN",
    "EXPRESS",
    "AMERICAN EXPRESS",
    "DELTA",
    "SKYMILES",
    "CARD",
    "GOLD",
}

app = FastAPI()
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def round_money(value: float) -> float:
    """Round monetary values to 2 decimal places to avoid floating-point precision issues."""
    return round(float(value), 2)


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    existing = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='envelopes'"
    ).fetchone()
    if not existing:
        init_db_with_conn(conn)
    else:
        # Table exists, but ensure migrations run
        ensure_column(conn, "transactions", "account_last4", "TEXT")
        ensure_column(conn, "transactions", "account_name", "TEXT")
        ensure_column(conn, "transactions", "account_owner", "TEXT")
        ensure_column(conn, "envelopes", "workspace_id", "INTEGER")
        ensure_column(conn, "transactions", "workspace_id", "INTEGER")
        ensure_default_workspaces(conn)
        migrate_existing_envelopes_to_workspace(conn)
        ensure_to_be_distributed(conn)
        conn.commit()


def init_db() -> None:
    with get_db() as conn:
        init_db_with_conn(conn)


def init_db_with_conn(conn: sqlite3.Connection) -> None:
    UPLOADS_DIR.mkdir(exist_ok=True)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workspaces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            date_added TEXT NOT NULL,
            date_deleted TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS envelopes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            date_added TEXT NOT NULL,
            date_deleted TEXT,
            amount_to_fund REAL NOT NULL DEFAULT 0.0,
            balance REAL NOT NULL DEFAULT 0.0,
            max_balance REAL NOT NULL DEFAULT 0.0,
            FOREIGN KEY(workspace_id) REFERENCES workspaces(id)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS statements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_hash TEXT NOT NULL UNIQUE,
            date_added TEXT NOT NULL,
            date_processed TEXT,
            all_transactions_generated INTEGER NOT NULL DEFAULT 0,
            date_deleted TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id INTEGER NOT NULL,
            date_added TEXT NOT NULL,
            transaction_date TEXT NOT NULL,
            name TEXT NOT NULL,
            amount REAL NOT NULL,
            applied INTEGER NOT NULL DEFAULT 0,
            envelope_id INTEGER,
            workspace_id INTEGER,
            date_applied TEXT,
            account_last4 TEXT,
            account_name TEXT,
            account_owner TEXT,
            FOREIGN KEY(statement_id) REFERENCES statements(id),
            FOREIGN KEY(envelope_id) REFERENCES envelopes(id),
            FOREIGN KEY(workspace_id) REFERENCES workspaces(id)
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_transactions_statement
        ON transactions(statement_id);
        """
    )
    ensure_column(conn, "transactions", "account_last4", "TEXT")
    ensure_column(conn, "transactions", "account_name", "TEXT")
    ensure_column(conn, "transactions", "account_owner", "TEXT")
    ensure_column(conn, "envelopes", "workspace_id", "INTEGER")
    ensure_column(conn, "transactions", "workspace_id", "INTEGER")
    ensure_default_workspaces(conn)
    migrate_existing_envelopes_to_workspace(conn)
    ensure_to_be_distributed(conn)


def ensure_column(
    conn: sqlite3.Connection, table: str, column: str, column_type: str
) -> None:
    existing = conn.execute(f"PRAGMA table_info({table});").fetchall()
    if any(row["name"] == column for row in existing):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type};")


def ensure_default_workspaces(conn: sqlite3.Connection) -> None:
    """Create default workspaces if they don't exist."""
    default_workspaces = ["Personal", "Real Estate", "Kwiplee", "Savings"]
    for workspace_name in default_workspaces:
        existing = conn.execute(
            "SELECT id FROM workspaces WHERE name = ? AND date_deleted IS NULL",
            (workspace_name,),
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO workspaces (name, date_added) VALUES (?, ?)",
                (workspace_name, utc_now()),
            )


def migrate_existing_envelopes_to_workspace(conn: sqlite3.Connection) -> None:
    """Assign existing envelopes without workspace_id to the first workspace."""
    try:
        # Get first workspace
        first_workspace = conn.execute(
            "SELECT id FROM workspaces WHERE date_deleted IS NULL ORDER BY id ASC LIMIT 1"
        ).fetchone()
        if not first_workspace:
            return
        workspace_id = first_workspace["id"]
        # Update envelopes without workspace_id
        conn.execute(
            """
            UPDATE envelopes 
            SET workspace_id = ? 
            WHERE workspace_id IS NULL AND date_deleted IS NULL
            """,
            (workspace_id,),
        )
        conn.commit()
    except Exception:
        # If migration fails, rollback and continue
        conn.rollback()
        pass


def ensure_to_be_distributed(conn: sqlite3.Connection) -> None:
    """Create 'To Be Distributed' envelope for each workspace if it doesn't exist.
    Also removes any duplicate 'To Be Distributed' envelopes."""
    try:
        workspaces = conn.execute(
            "SELECT id FROM workspaces WHERE date_deleted IS NULL"
        ).fetchall()
        for workspace in workspaces:
            workspace_id = workspace["id"]
            existing = conn.execute(
                """
                SELECT id FROM envelopes 
                WHERE name = ? AND workspace_id = ? AND date_deleted IS NULL
                ORDER BY id ASC
                """,
                (SYSTEM_ENVELOPE_NAME, workspace_id),
            ).fetchall()
            
            if not existing:
                # Create if doesn't exist
                conn.execute(
                    """
                    INSERT INTO envelopes (workspace_id, name, date_added, amount_to_fund, balance, max_balance)
                    VALUES (?, ?, ?, 0.0, 0.0, 0.0)
                    """,
                    (workspace_id, SYSTEM_ENVELOPE_NAME, utc_now()),
                )
            elif len(existing) > 1:
                # If duplicates exist, keep the first one and merge/delete others
                keep_id = existing[0]["id"]
                duplicate_ids = [row["id"] for row in existing[1:]]
                
                # Merge balances from duplicates into the kept envelope
                for dup_id in duplicate_ids:
                    dup_balance = conn.execute(
                        "SELECT balance FROM envelopes WHERE id = ?",
                        (dup_id,),
                    ).fetchone()
                    if dup_balance and abs(dup_balance["balance"]) > 0.001:
                        conn.execute(
                            "UPDATE envelopes SET balance = ROUND(balance + ?, 2) WHERE id = ?",
                            (dup_balance["balance"], keep_id),
                        )
                    # Move transactions from duplicate to kept envelope
                    conn.execute(
                        "UPDATE transactions SET envelope_id = ? WHERE envelope_id = ?",
                        (keep_id, dup_id),
                    )
                    # Delete duplicate
                    conn.execute(
                        "UPDATE envelopes SET date_deleted = ? WHERE id = ?",
                        (utc_now(), dup_id),
                    )
        conn.commit()
    except Exception:
        conn.rollback()
        pass


def is_system_envelope(name: str) -> bool:
    return name.strip().lower() == SYSTEM_ENVELOPE_NAME.lower()


def hash_file(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_amount(value: str) -> float:
    cleaned = value.replace("$", "").replace(",", "").strip()
    return float(cleaned)


def parse_statement_csv(file_path: Path) -> Iterable[dict[str, str]]:
    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def normalize_last4(value: str) -> Optional[str]:
    digits = re.sub(r"\D", "", value)
    if not digits:
        return None
    return digits[-4:]


def detect_account_context(line: str) -> tuple[Optional[str], Optional[str]]:
    number_match = ACCOUNT_NUMBER_PATTERN.search(line)
    if number_match:
        return None, normalize_last4(number_match.group(1))
    ending_match = ACCOUNT_ENDING_PATTERN.search(line)
    if ending_match:
        return None, normalize_last4(ending_match.group(1))
    ending_in_match = ACCOUNT_ENDING_IN_PATTERN.search(line)
    if ending_in_match:
        return None, normalize_last4(ending_in_match.group(1))
    header_match = ACCOUNT_HEADER_PATTERN.search(line)
    if header_match:
        name = header_match.group("name").strip()
        name = re.sub(r"\bcontinued\b", "", name, flags=re.IGNORECASE).strip()
        return name, header_match.group("last4")
    return None, None


def detect_owner(line: str) -> Optional[str]:
    if not OWNER_LINE_PATTERN.match(line):
        return None
    if any(word in line for word in OWNER_BLOCKLIST):
        return None
    return " ".join(part.capitalize() for part in line.split())


def is_membership_account(account_name: Optional[str]) -> bool:
    """Check if an account is a membership account that should be ignored."""
    if not account_name:
        return False
    account_lower = account_name.lower()
    membership_keywords = [
        "membership",
        "share account",
        "primary share",
        "share savings",
        "membership share",
    ]
    return any(keyword in account_lower for keyword in membership_keywords)


def parse_chase_amex_pdf(file_path: Path) -> Iterable[dict[str, str]]:
    """Parse Chase/AmEx Delta SkyMiles Gold Card statements.
    
    Extracts transactions from:
    - Payments and Credits -> Detail table (income, positive amounts)
    - New Charges -> Detail/Detail Continued tables (expenses, negative amounts)
    
    Schema: date, merchant, amount only.
    Foreign currency amounts are ignored - only USD amounts starting with $ are used.
    """
    USD_AMOUNT_PATTERN = re.compile(r"\$(\d{1,3}(?:,\d{3})*(?:\.\d{2}))")
    TRANSACTION_DATE_PATTERN = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})")
    
    with pdfplumber.open(str(file_path)) as pdf:
        in_payments_section = False
        in_charges_section = False
        in_detail_table = False
        current_account_last4: Optional[str] = None
        current_account_name: Optional[str] = None
        
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = text.splitlines()
            
            for i, raw_line in enumerate(lines):
                line = raw_line.strip()
                if not line:
                    continue
                
                # Detect account context
                account_name, account_last4 = detect_account_context(line)
                if account_name:
                    current_account_name = account_name
                if account_last4:
                    current_account_last4 = account_last4
                
                # Detect section headers
                if "Payments and Credits" in line:
                    in_payments_section = True
                    in_charges_section = False
                    in_detail_table = False
                    continue
                elif "New Charges" in line:
                    in_payments_section = False
                    in_charges_section = True
                    in_detail_table = False
                    continue
                elif "Detail" in line or "Detail Continued" in line:
                    if in_payments_section or in_charges_section:
                        in_detail_table = True
                    continue
                elif line.startswith("##") or "Fees" in line or "Interest" in line:
                    # End of transaction sections
                    in_payments_section = False
                    in_charges_section = False
                    in_detail_table = False
                    continue
                
                # Only process lines within detail tables
                if not in_detail_table:
                    continue
                
                # Extract transaction date
                date_match = TRANSACTION_DATE_PATTERN.search(line)
                if not date_match:
                    continue
                
                transaction_date = date_match.group(1)
                
                # Find USD amount (must start with $)
                usd_amounts = USD_AMOUNT_PATTERN.findall(line)
                if not usd_amounts:
                    # Skip lines without USD amounts (foreign currency only)
                    continue
                
                # Use the last USD amount found (typically the Amount column)
                amount_str = usd_amounts[-1]
                amount = parse_amount(amount_str)
                
                # Normalize amounts based on section
                if in_payments_section:
                    # Payments/Credits: always positive (convert negatives to positive)
                    amount = abs(amount)
                elif in_charges_section:
                    # New Charges: always negative (convert positives to negative)
                    amount = -abs(amount)
                else:
                    continue
                
                # Extract merchant/description
                # Remove date and amounts, keep the rest as merchant name
                merchant = line
                merchant = TRANSACTION_DATE_PATTERN.sub("", merchant, count=1).strip()
                # Remove all USD amounts
                for usd_amt in usd_amounts:
                    merchant = merchant.replace(f"${usd_amt}", "").strip()
                # Remove foreign currency patterns if present
                merchant = re.sub(r"\d+\.\d+\s+[A-Za-z]+\s+Pesos?", "", merchant, flags=re.IGNORECASE).strip()
                merchant = re.sub(r"\d+\.\d+\s+Canadian\s+Dollars?", "", merchant, flags=re.IGNORECASE).strip()
                # Clean up extra whitespace and separators
                merchant = re.sub(r"\s+", " ", merchant).strip()
                merchant = merchant.strip("|").strip()
                
                if not merchant or len(merchant) < 2:
                    continue
                
                yield {
                    "date": transaction_date,
                    "name": merchant,
                    "amount": f"{amount:.2f}",
                    "account_last4": current_account_last4 or "",
                    "account_name": current_account_name or "",
                    "account_owner": "",
                }


def parse_statement_pdf(file_path: Path) -> Iterable[dict[str, str]]:
    with pdfplumber.open(str(file_path)) as pdf:
        current_account_last4: Optional[str] = None
        current_account_name: Optional[str] = None
        current_account_owner: Optional[str] = None
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw_line in text.splitlines():
                line = raw_line.strip()
                owner = detect_owner(line)
                if owner:
                    current_account_owner = owner
                account_name, account_last4 = detect_account_context(line)
                if account_name:
                    current_account_name = account_name
                if account_last4:
                    current_account_last4 = account_last4
                # Skip membership accounts - don't parse transactions from them
                if is_membership_account(current_account_name):
                    continue
                if not line or not DATE_PATTERN.match(line):
                    continue
                if len(DATE_PATTERN.findall(line)) > 1:
                    # Skip balance summary rows that pack multiple dates into one line.
                    continue
                if "BEGINNING BALANCE" in line or "ENDING BALANCE" in line:
                    continue
                amounts = AMOUNT_PATTERN.findall(line)
                if not amounts:
                    continue
                amount_str = amounts[-1]
                if len(amounts) >= 2 and not FOREIGN_CURRENCY_PATTERN.search(line):
                    amount_str = amounts[-2]
                description = DATE_PATTERN.sub("", line, count=1).strip()
                description = re.sub(
                    r"(?:\s+\$?-?\d{1,3}(?:,\d{3})*(?:\.\d{2})){1,}$",
                    "",
                    description,
                ).strip()
                if not re.search(r"[A-Za-z]", description):
                    # Skip balance summary lines that contain only dates/amounts.
                    continue
                amount = parse_amount(amount_str)
                if not amount_str.strip().startswith("-"):
                    lowered = description.lower()
                    if "withdrawal" in lowered or "check" in lowered or "debit" in lowered:
                        amount = -abs(amount)
                yield {
                    "date": DATE_PATTERN.match(line).group(0),
                    "name": description,
                    "amount": f"{amount:.2f}",
                    "account_last4": current_account_last4 or "",
                    "account_name": current_account_name or "",
                    "account_owner": current_account_owner or "",
                }


def is_chase_amex_statement(file_path: Path) -> bool:
    """Detect if PDF is a Chase/AmEx Delta SkyMiles Gold Card statement."""
    try:
        with pdfplumber.open(str(file_path)) as pdf:
            # Check first page for keywords
            if pdf.pages:
                first_page_text = pdf.pages[0].extract_text() or ""
                return (
                    "Delta SkyMiles" in first_page_text
                    or "Delta SkyMiles® Gold Card" in first_page_text
                    or ("Chase" in first_page_text and "Delta" in first_page_text)
                )
    except Exception:
        pass
    return False


def is_chase_credit_card_statement(file_path: Path) -> bool:
    """Detect if PDF is a Chase credit card statement with PAYMENTS AND OTHER CREDITS / PURCHASE sections."""
    try:
        with pdfplumber.open(str(file_path)) as pdf:
            # Check for the specific section headers
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if "PAYMENTS AND OTHER CREDITS" in page_text and "PURCHASE" in page_text:
                    # Also check for Chase branding
                    if "Chase" in page_text or "chase.com" in page_text:
                        return True
    except Exception:
        pass
    return False


def parse_chase_credit_card_pdf(file_path: Path) -> Iterable[dict[str, str]]:
    """Parse Chase credit card statements with PAYMENTS AND OTHER CREDITS and PURCHASE sections.
    
    Extracts transactions from both sections and flips the sign:
    - Negative amounts on statement -> positive in parsed result
    - Positive amounts on statement -> negative in parsed result
    
    Uses aggressive extraction to catch all transactions.
    """
    TRANSACTION_DATE_PATTERN = re.compile(r"(\d{1,2}/\d{1,2})")
    AMOUNT_PATTERN = re.compile(r"-?(?:\d{1,3}(?:,\d{3})*)?(?:\.\d{2})")
    
    current_account_last4: Optional[str] = None
    current_account_name: Optional[str] = None
    
    # Collect all potential transactions first
    all_transactions = []
    
    with pdfplumber.open(str(file_path)) as pdf:
        # First pass: detect account info
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                account_name, account_last4 = detect_account_context(line)
                if account_name:
                    current_account_name = account_name
                if account_last4:
                    current_account_last4 = account_last4
        
        # Second pass: extract ALL lines that look like transactions
        in_transaction_area = False
        
        for page in pdf.pages:
            # Try multiple extraction methods
            text = page.extract_text(layout=True) or page.extract_text() or ""
            lines = text.splitlines()
            
            # Also try table extraction
            tables = page.extract_tables()
            table_rows = []
            for table in tables:
                if table:
                    for row in table:
                        if row:
                            row_text = " ".join(str(cell) if cell else "" for cell in row).strip()
                            if row_text:
                                table_rows.append(row_text)
            
            # Combine text lines and table rows
            all_lines = lines + table_rows
            
            for raw_line in all_lines:
                line = raw_line.strip()
                if not line:
                    continue
                
                line_upper = line.upper()
                
                # Mark when we enter transaction area
                if "ACCOUNT ACTIVITY" in line_upper or "PAYMENTS AND OTHER CREDITS" in line_upper or ("PURCHASE" in line_upper and "ACCOUNT ACTIVITY" not in line_upper and len(line.split()) <= 3):
                    in_transaction_area = True
                
                # Exit transaction area on clear end markers (but only after we've started)
                if in_transaction_area and any(marker in line_upper for marker in ["INTEREST CHARGES", "CASH ADVANCES", "BALANCE TRANSFERS", "YEAR-TO-DATE TOTALS"]):
                    # Only exit if we see these as section headers, not in transaction descriptions
                    if len(line.split()) <= 5:  # Section headers are short
                        break
                
                # Look for transaction pattern: date + amount
                date_match = TRANSACTION_DATE_PATTERN.search(line)
                amounts = AMOUNT_PATTERN.findall(line)
                
                if date_match and amounts:
                    # Skip obvious non-transactions
                    if any(skip in line_upper for skip in ["DATE OF TRANSACTION", "MERCHANT NAME", "STATEMENT DATE", "OPENING/CLOSING", "PREVIOUS BALANCE"]):
                        continue
                    
                    transaction_date = date_match.group(1)
                    amount_str = amounts[-1]
                    
                    # Extract merchant name - be more aggressive
                    merchant = line
                    # Remove date
                    merchant = TRANSACTION_DATE_PATTERN.sub("", merchant, count=1).strip()
                    # Remove amounts
                    for amt in amounts:
                        merchant = merchant.replace(amt, "").strip()
                    # Clean up
                    merchant = re.sub(r"\s+", " ", merchant).strip()
                    merchant = merchant.strip("$").strip()
                    
                    # Skip if too short or is a header
                    if not merchant or len(merchant) < 2:
                        continue
                    if merchant.upper() in ["TRANSACTION", "AMOUNT", "MERCHANT NAME OR TRANSACTION DESCRIPTION"]:
                        continue
                    
                    # Parse amount and flip sign
                    amount = parse_amount(amount_str)
                    amount = -amount  # Flip: negative->positive, positive->negative
                    
                    all_transactions.append({
                        "date": transaction_date,
                        "name": merchant,
                        "amount": round_money(amount),
                    })
    
    # Remove duplicates (same date, name, amount)
    seen = set()
    unique_transactions = []
    for tx in all_transactions:
        key = (tx["date"], tx["name"][:50], tx["amount"])
        if key not in seen:
            seen.add(key)
            unique_transactions.append(tx)
    
    # Yield all unique transactions
    for tx in unique_transactions:
        yield {
            "date": tx["date"],
            "name": tx["name"],
            "amount": f"{tx['amount']:.2f}",
            "account_last4": current_account_last4 or "",
            "account_name": current_account_name or "",
            "account_owner": "",
        }


def parse_statement(file_path: Path) -> Iterable[dict[str, str]]:
    if file_path.suffix.lower() == ".pdf":
        if is_chase_credit_card_statement(file_path):
            return parse_chase_credit_card_pdf(file_path)
        elif is_chase_amex_statement(file_path):
            return parse_chase_amex_pdf(file_path)
        return parse_statement_pdf(file_path)
    return parse_statement_csv(file_path)


def suggested_envelope_id(name: str) -> Optional[int]:
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT envelope_id
            FROM transactions
            WHERE name = ? AND envelope_id IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (name,),
        ).fetchone()
        return row["envelope_id"] if row else None


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/envelopes", status_code=302)


@app.get("/envelopes")
def envelopes_page(
    request: Request, 
    workspace_id: int | None = Query(None),
    message: str | None = Query(None)
) -> object:
    with get_db() as conn:
        # Get all workspaces
        workspaces = conn.execute(
            """
            SELECT id, name
            FROM workspaces
            WHERE date_deleted IS NULL
            ORDER BY 
                CASE name
                    WHEN 'Personal' THEN 1
                    WHEN 'Real Estate' THEN 2
                    WHEN 'Kwiplee' THEN 3
                    WHEN 'Savings' THEN 4
                    ELSE 5
                END ASC
            """
        ).fetchall()
        
        # Default to first workspace if none specified
        if not workspaces:
            return templates.TemplateResponse(
                "envelopes.html",
                {
                    "request": request,
                    "workspaces": [],
                    "current_workspace_id": None,
                    "envelopes": [],
                    "total_balance": 0.0,
                    "message": "No workspaces available",
                },
            )
        
        if workspace_id is None:
            workspace_id = int(workspaces[0]["id"])
        
        # Get envelopes for selected workspace - ensure workspace_id is an int
        workspace_id_int = int(workspace_id) if workspace_id is not None else int(workspaces[0]["id"])
        envelopes_raw = conn.execute(
            """
            SELECT *
            FROM envelopes
            WHERE workspace_id = ? AND date_deleted IS NULL
            ORDER BY date_added ASC
            """,
            (workspace_id_int,),
        ).fetchall()
        
        # Round all balances for display and fix any precision issues in database
        envelopes = []
        for row in envelopes_raw:
            rounded_balance = round_money(row["balance"])
            rounded_amount_to_fund = round_money(row["amount_to_fund"])
            rounded_max_balance = round_money(row["max_balance"])
            # Update database if values were rounded (fixes existing precision issues)
            if (abs(row["balance"] - rounded_balance) > 0.001 or
                abs(row["amount_to_fund"] - rounded_amount_to_fund) > 0.001 or
                abs(row["max_balance"] - rounded_max_balance) > 0.001):
                conn.execute(
                    """
                    UPDATE envelopes
                    SET balance = ?, amount_to_fund = ?, max_balance = ?
                    WHERE id = ?
                    """,
                    (rounded_balance, rounded_amount_to_fund, rounded_max_balance, row["id"]),
                )
            # Create a dict with rounded values for template
            try:
                workspace_id_val = row["workspace_id"]
            except (KeyError, IndexError):
                workspace_id_val = None
            envelope_dict = {
                "id": row["id"],
                "name": row["name"],
                "date_added": row["date_added"],
                "date_deleted": row["date_deleted"],
                "workspace_id": workspace_id_val,
                "amount_to_fund": rounded_amount_to_fund,
                "balance": rounded_balance,
                "max_balance": rounded_max_balance,
            }
            envelopes.append(envelope_dict)
    total_balance = round_money(sum(e["balance"] for e in envelopes))
    return templates.TemplateResponse(
        "envelopes.html",
        {
            "request": request,
            "workspaces": workspaces,
            "current_workspace_id": workspace_id,
            "envelopes": envelopes,
            "total_balance": total_balance,
            "message": message,
        },
    )


@app.post("/envelopes/create")
def create_envelope(
    workspace_id: int = Form(...),
    name: str = Form(...),
    amount_to_fund: float = Form(0.0),
    balance: float = Form(0.0),
    max_balance: float = Form(0.0),
) -> RedirectResponse:
    if is_system_envelope(name):
        return RedirectResponse(
            url=f"/envelopes?workspace_id={workspace_id}&message=System+envelope+is+managed+by+the+system",
            status_code=303,
        )
    # Round all monetary inputs
    amount_to_fund = round_money(amount_to_fund)
    balance = round_money(balance)
    max_balance = round_money(max_balance)
    
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO envelopes (workspace_id, name, date_added, amount_to_fund, balance, max_balance)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (workspace_id, name.strip(), utc_now(), amount_to_fund, balance, max_balance),
        )
        if balance > 0:
            # Deduct from "To Be Distributed" envelope in the same workspace
            conn.execute(
                """
                UPDATE envelopes
                SET balance = ROUND(balance - ?, 2)
                WHERE name = ? AND workspace_id = ? AND date_deleted IS NULL
                """,
                (balance, SYSTEM_ENVELOPE_NAME, workspace_id),
            )
    return RedirectResponse(
        url=f"/envelopes?workspace_id={workspace_id}&message=Envelope+created", 
        status_code=303
    )


@app.post("/envelopes/update/{envelope_id}")
def update_envelope(
    envelope_id: int,
    workspace_id: int = Form(...),
    name: str = Form(...),
    amount_to_fund: float = Form(0.0),
    balance: float = Form(0.0),
    max_balance: float = Form(0.0),
) -> RedirectResponse:
    # Round all monetary inputs
    amount_to_fund = round_money(amount_to_fund)
    balance = round_money(balance)
    max_balance = round_money(max_balance)
    
    with get_db() as conn:
        existing = conn.execute(
            "SELECT name, balance, workspace_id FROM envelopes WHERE id = ? AND date_deleted IS NULL",
            (envelope_id,),
        ).fetchone()
        if not existing:
            return RedirectResponse(
                url=f"/envelopes?workspace_id={workspace_id}&message=Envelope+not+found", 
                status_code=303
            )
        envelope_workspace_id = existing["workspace_id"]
        is_system = is_system_envelope(existing["name"])
        
        if is_system:
            # For system envelope, only allow balance updates
            # Keep name, amount_to_fund, and max_balance unchanged
            conn.execute(
                """
                UPDATE envelopes
                SET balance = ?
                WHERE id = ? AND date_deleted IS NULL
                """,
                (balance, envelope_id),
            )
        else:
            # For regular envelopes, update all fields and handle balance delta
            conn.execute(
                """
                UPDATE envelopes
                SET name = ?, amount_to_fund = ?, balance = ?, max_balance = ?
                WHERE id = ? AND date_deleted IS NULL
                """,
                (name.strip(), amount_to_fund, balance, max_balance, envelope_id),
            )
            delta = round_money(balance - existing["balance"])
            if abs(delta) > 0.001:  # Use epsilon for floating point comparison
                conn.execute(
                    """
                    UPDATE envelopes
                    SET balance = ROUND(balance - ?, 2)
                    WHERE name = ? AND workspace_id = ? AND date_deleted IS NULL
                    """,
                    (delta, SYSTEM_ENVELOPE_NAME, envelope_workspace_id),
                )
    return RedirectResponse(
        url=f"/envelopes?workspace_id={workspace_id}&message=Envelope+updated", 
        status_code=303
    )


@app.post("/envelopes/delete/{envelope_id}")
def delete_envelope(envelope_id: int, workspace_id: int = Form(...)) -> RedirectResponse:
    with get_db() as conn:
        existing = conn.execute(
            "SELECT name, workspace_id FROM envelopes WHERE id = ? AND date_deleted IS NULL",
            (envelope_id,),
        ).fetchone()
        if not existing:
            return RedirectResponse(
                url=f"/envelopes?workspace_id={workspace_id}&message=Envelope+not+found", 
                status_code=303
            )
        if is_system_envelope(existing["name"]):
            return RedirectResponse(
                url=f"/envelopes?workspace_id={workspace_id}&message=System+envelope+is+managed+by+the+system",
                status_code=303,
            )
        envelope_workspace_id = existing["workspace_id"]
        balance_row = conn.execute(
            "SELECT balance FROM envelopes WHERE id = ?",
            (envelope_id,),
        ).fetchone()
        conn.execute(
            """
            UPDATE envelopes
            SET date_deleted = ?
            WHERE id = ?
            """,
            (utc_now(), envelope_id),
        )
        if balance_row and abs(balance_row["balance"]) > 0.001:  # Use epsilon for floating point comparison
            conn.execute(
                """
                UPDATE envelopes
                SET balance = ROUND(balance + ?, 2)
                WHERE name = ? AND workspace_id = ? AND date_deleted IS NULL
                """,
                (balance_row["balance"], SYSTEM_ENVELOPE_NAME, envelope_workspace_id),
            )
    return RedirectResponse(
        url=f"/envelopes?workspace_id={workspace_id}&message=Envelope+deleted", 
        status_code=303
    )


@app.post("/envelopes/fill")
def fill_envelopes(workspace_id: int = Form(...)) -> RedirectResponse:
    with get_db() as conn:
        system = conn.execute(
            """
            SELECT id, balance FROM envelopes 
            WHERE name = ? AND workspace_id = ? AND date_deleted IS NULL
            """,
            (SYSTEM_ENVELOPE_NAME, workspace_id),
        ).fetchone()
        if not system:
            return RedirectResponse(
                url=f"/envelopes?workspace_id={workspace_id}&message=System+envelope+missing", 
                status_code=303
            )
        envelopes = conn.execute(
            """
            SELECT id, name, amount_to_fund, balance, max_balance
            FROM envelopes
            WHERE workspace_id = ? AND date_deleted IS NULL
            """,
            (workspace_id,),
        ).fetchall()
        total_transfer = 0.0
        for row in envelopes:
            if is_system_envelope(row["name"]):
                continue
            desired_balance = round_money(row["balance"] + row["amount_to_fund"])
            if row["max_balance"] > 0:
                desired_balance = min(desired_balance, round_money(row["max_balance"]))
            transfer = round_money(desired_balance - row["balance"])
            if transfer <= 0:
                continue
            conn.execute(
                "UPDATE envelopes SET balance = ROUND(balance + ?, 2) WHERE id = ?",
                (transfer, row["id"]),
            )
            total_transfer = round_money(total_transfer + transfer)
        if total_transfer > 0.001:  # Use epsilon for floating point comparison
            conn.execute(
                "UPDATE envelopes SET balance = ROUND(balance - ?, 2) WHERE id = ?",
                (total_transfer, system["id"]),
            )
    if total_transfer == 0:
        return RedirectResponse(
            url=f"/envelopes?workspace_id={workspace_id}&message=No+envelopes+needed+funding",
            status_code=303,
        )
    return RedirectResponse(
        url=f"/envelopes?workspace_id={workspace_id}&message=Envelopes+filled", 
        status_code=303
    )


@app.get("/statements")
def statements_page(request: Request, message: str | None = None) -> object:
    with get_db() as conn:
        statements = conn.execute(
            """
            SELECT *
            FROM statements
            WHERE date_deleted IS NULL
            ORDER BY date_added DESC
            """
        ).fetchall()
    return templates.TemplateResponse(
        "statements.html",
        {"request": request, "statements": statements, "message": message},
    )


@app.post("/statements/upload")
async def upload_statement(file: UploadFile = File(...)) -> RedirectResponse:
    data = await file.read()
    if not data:
        return RedirectResponse(
            url="/statements?message=Empty+file+upload", status_code=303
        )
    file_hash = hash_file(data)
    filename = file.filename or f"statement-{file_hash}.csv"
    with get_db() as conn:
        existing = conn.execute(
            "SELECT 1 FROM statements WHERE file_hash = ?",
            (file_hash,),
        ).fetchone()
    stored_hash = file_hash
    if existing:
        stored_hash = f"{file_hash}-{uuid.uuid4().hex[:8]}"
    destination = UPLOADS_DIR / f"{stored_hash}-{filename}"
    with get_db() as conn:
        destination.write_bytes(data)
        conn.execute(
            """
            INSERT INTO statements (name, file_path, file_hash, date_added)
            VALUES (?, ?, ?, ?)
            """,
            (filename, str(destination), stored_hash, utc_now()),
        )
    return RedirectResponse(url="/statements?message=Statement+uploaded", status_code=303)


@app.post("/statements/delete/{statement_id}")
def delete_statement(statement_id: int) -> RedirectResponse:
    with get_db() as conn:
        conn.execute(
            """
            UPDATE statements
            SET date_deleted = ?
            WHERE id = ?
            """,
            (utc_now(), statement_id),
        )
    return RedirectResponse(url="/statements?message=Statement+deleted", status_code=303)


@app.post("/statements/process/{statement_id}")
def process_statement(statement_id: int) -> RedirectResponse:
    with get_db() as conn:
        statement = conn.execute(
            """
            SELECT *
            FROM statements
            WHERE id = ? AND date_deleted IS NULL
            """,
            (statement_id,),
        ).fetchone()
        if not statement:
            return RedirectResponse(
                url="/statements?message=Statement+not+found", status_code=303
            )
        file_path = Path(statement["file_path"])
        if not file_path.exists():
            return RedirectResponse(
                url="/statements?message=Statement+file+missing", status_code=303
            )

        conn.execute(
            "DELETE FROM transactions WHERE statement_id = ?",
            (statement_id,),
        )
        for row in parse_statement(file_path):
            date_value = row.get("date") or row.get("Date") or ""
            name_value = row.get("name") or row.get("Name") or ""
            amount_value = row.get("amount") or row.get("Amount") or "0"
            account_last4 = (
                row.get("account_last4")
                or row.get("account")
                or row.get("Account")
                or ""
            )
            account_name = (
                row.get("account_name")
                or row.get("Account Name")
                or row.get("account name")
                or ""
            )
            account_owner = (
                row.get("account_owner")
                or row.get("Account Owner")
                or row.get("account owner")
                or ""
            )
            if not date_value or not name_value:
                continue
            # Note: Removed comma filter - legitimate merchants like "NETFLIX, INC." contain commas
            amount = round_money(parse_amount(amount_value))
            conn.execute(
                """
                INSERT INTO transactions
                (statement_id, date_added, transaction_date, name, amount,
                 account_last4, account_name, account_owner)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    statement_id,
                    utc_now(),
                    date_value,
                    name_value,
                    amount,
                    account_last4,
                    account_name,
                    account_owner,
                ),
            )
        conn.execute(
            """
            UPDATE statements
            SET all_transactions_generated = 1, date_processed = ?
            WHERE id = ?
            """,
            (utc_now(), statement_id),
        )
    return RedirectResponse(
        url="/statements?message=Statement+processed", status_code=303
    )


@app.get("/statements/{statement_id}/map")
def map_transactions(
    request: Request, 
    statement_id: int, 
    workspace_id: int | None = Query(None),
    message: str | None = Query(None)
) -> object:
    with get_db() as conn:
        statement = conn.execute(
            "SELECT * FROM statements WHERE id = ? AND date_deleted IS NULL",
            (statement_id,),
        ).fetchone()
        if not statement:
            return RedirectResponse(
                url="/statements?message=Statement+not+found", status_code=303
            )
        
        # Get all workspaces
        workspaces = conn.execute(
            """
            SELECT id, name
            FROM workspaces
            WHERE date_deleted IS NULL
            ORDER BY 
                CASE name
                    WHEN 'Personal' THEN 1
                    WHEN 'Real Estate' THEN 2
                    WHEN 'Kwiplee' THEN 3
                    WHEN 'Savings' THEN 4
                    ELSE 5
                END ASC
            """
        ).fetchall()
        
        # Default to first workspace if none specified
        if not workspaces:
            return RedirectResponse(
                url="/statements?message=No+workspaces+available", status_code=303
            )
        # Convert workspace_id to int if provided, otherwise use first workspace
        if workspace_id is None:
            workspace_id = int(workspaces[0]["id"])
        else:
            try:
                workspace_id = int(workspace_id)
            except (ValueError, TypeError):
                workspace_id = int(workspaces[0]["id"])
        
        transactions = conn.execute(
            """
            SELECT *
            FROM transactions
            WHERE statement_id = ? AND applied = 0
            ORDER BY transaction_date ASC, id ASC
            """,
            (statement_id,),
        ).fetchall()
        mapped = conn.execute(
            """
            SELECT transactions.*, envelopes.name AS envelope_name
            FROM transactions
            JOIN envelopes ON envelopes.id = transactions.envelope_id
            WHERE transactions.statement_id = ? AND transactions.applied = 1
            ORDER BY envelopes.name ASC, transactions.transaction_date ASC, transactions.id ASC
            """,
            (statement_id,),
        ).fetchall()
        envelopes = conn.execute(
            """
            SELECT * FROM envelopes 
            WHERE workspace_id = ? AND date_deleted IS NULL 
            ORDER BY name ASC
            """,
            (workspace_id,),
        ).fetchall()

    suggestions: dict[int, Optional[int]] = {
        row["id"]: suggested_envelope_id(row["name"]) for row in transactions
    }
    unassigned_grouped_map: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for row in transactions:
        name = row["account_name"] or "Account"
        last4 = row["account_last4"] or "Unknown"
        unassigned_grouped_map.setdefault((name, last4), []).append(row)
    unassigned_groups = [
        {"account_name": name, "account_last4": last4, "entries": items}
        for (name, last4), items in sorted(unassigned_grouped_map.items())
    ]
    mapped_by_envelope: dict[int, list[sqlite3.Row]] = {}
    mapped_counts: dict[int, int] = {row["id"]: 0 for row in envelopes}
    for row in mapped:
        mapped_by_envelope.setdefault(row["envelope_id"], []).append(row)
        mapped_counts[row["envelope_id"]] = mapped_counts.get(row["envelope_id"], 0) + 1
    mapped_by_envelope_json = {
        envelope_id: [
            {
                "transaction_date": item["transaction_date"],
                "name": item["name"],
                "amount": item["amount"],
            }
            for item in items
        ]
        for envelope_id, items in mapped_by_envelope.items()
    }
    return templates.TemplateResponse(
        "map.html",
        {
            "request": request,
            "statement": statement,
            "transactions": transactions,
            "unassigned_groups": unassigned_groups,
            "workspaces": workspaces,
            "current_workspace_id": workspace_id,
            "envelopes": envelopes,
            "mapped_by_envelope": mapped_by_envelope,
            "mapped_by_envelope_json": mapped_by_envelope_json,
            "mapped_counts": mapped_counts,
            "suggestions": suggestions,
            "message": message,
        },
    )


@app.get("/statements/{statement_id}/transactions")
def list_transactions(
    request: Request, statement_id: int, message: str | None = None
) -> object:
    with get_db() as conn:
        statement = conn.execute(
            "SELECT * FROM statements WHERE id = ? AND date_deleted IS NULL",
            (statement_id,),
        ).fetchone()
        if not statement:
            return RedirectResponse(
                url="/statements?message=Statement+not+found", status_code=303
            )
        transactions = conn.execute(
            """
            SELECT transactions.*, envelopes.name AS envelope_name
            FROM transactions
            LEFT JOIN envelopes ON envelopes.id = transactions.envelope_id
            WHERE transactions.statement_id = ?
            ORDER BY transactions.account_last4 ASC, transactions.account_name ASC,
                     transactions.transaction_date ASC, transactions.id ASC
            """,
            (statement_id,),
        ).fetchall()
    grouped_map: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for tx in transactions:
        last4 = tx["account_last4"] or "Unknown"
        name = tx["account_name"] or "Account"
        key = (name, last4)
        grouped_map.setdefault(key, []).append(tx)
    grouped = [
        {"account_name": name, "account_last4": last4, "entries": items}
        for (name, last4), items in sorted(grouped_map.items())
    ]
    return templates.TemplateResponse(
        "transactions.html",
        {
            "request": request,
            "statement": statement,
            "transactions": transactions,
            "grouped": grouped,
            "message": message,
        },
    )


def assign_transaction(
    conn: sqlite3.Connection, transaction_id: int, envelope_id: int, workspace_id: int
) -> tuple[bool, str]:
    transaction = conn.execute(
        "SELECT * FROM transactions WHERE id = ?", (transaction_id,)
    ).fetchone()
    if not transaction:
        return False, "Transaction+not+found"
    old_envelope_id = transaction["envelope_id"]
    if old_envelope_id == envelope_id:
        return True, "Transaction+already+mapped"
    # Round transaction amount to ensure precision
    amount = round_money(transaction["amount"])
    
    if old_envelope_id:
        conn.execute(
            "UPDATE envelopes SET balance = ROUND(balance - ?, 2) WHERE id = ?",
            (amount, old_envelope_id),
        )
    conn.execute(
        "UPDATE envelopes SET balance = ROUND(balance + ?, 2) WHERE id = ?",
        (amount, envelope_id),
    )
    conn.execute(
        """
        UPDATE transactions
        SET applied = 1, envelope_id = ?, workspace_id = ?, date_applied = ?
        WHERE id = ?
        """,
        (envelope_id, workspace_id, utc_now(), transaction_id),
    )
    return True, "Transaction+mapped"


@app.post("/transactions/{transaction_id}/apply")
def apply_transaction(
    transaction_id: int,
    envelope_id: int = Form(...),
    workspace_id: int = Form(...),
    statement_id: int = Form(...),
) -> RedirectResponse:
    with get_db() as conn:
        ok, message = assign_transaction(conn, transaction_id, envelope_id, workspace_id)
        if not ok:
            return RedirectResponse(
                url=f"/statements/{statement_id}/map?workspace_id={workspace_id}&message={message}",
                status_code=303,
            )
    return RedirectResponse(
        url=f"/statements/{statement_id}/map?workspace_id={workspace_id}&message={message}",
        status_code=303,
    )


@app.post("/transactions/{transaction_id}/assign")
def assign_transaction_api(
    transaction_id: int,
    envelope_id: int = Form(...),
    workspace_id: int = Form(...),
    statement_id: int = Form(...),
) -> RedirectResponse:
    with get_db() as conn:
        ok, message = assign_transaction(conn, transaction_id, envelope_id, workspace_id)
        if not ok:
            return RedirectResponse(
                url=f"/statements/{statement_id}/map?workspace_id={workspace_id}&message={message}",
                status_code=303,
            )
    return RedirectResponse(
        url=f"/statements/{statement_id}/map?workspace_id={workspace_id}&message={message}",
        status_code=303,
    )
