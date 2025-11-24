#!/usr/bin/env python3
"""
Robust Google Sheets + TCL calc script.

Place creds.json in the same directory as this script.
"""

import os
import sys
import time
import json
from functools import wraps
from typing import Any, List, Tuple, Dict

import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound

# -----------------------
# Configuration
# -----------------------
ACC_SIZE = 2000.0  # account size (float)
RATE_LIMIT_INTERVAL = 1.0  # seconds between calls

# -----------------------
# Locate credentials file
# -----------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(script_dir, "creds.json")

# -----------------------
# Globals (populated after auth)
# -----------------------
client: gspread.Client = None  # type: ignore
spreadsheet: gspread.Spreadsheet = None  # type: ignore
_last_request_time = 0.0


# -----------------------
# Credential loader with robust error handling
# -----------------------
def load_service_account_credentials(path: str, scopes: List[str]) -> Credentials:
    """
    Safely load service account credentials from JSON file.
    Raises SystemExit with explanatory message on fatal problems.
    """
    if not os.path.exists(path):
        print(f"ERROR: credentials file not found at: {path}", file=sys.stderr)
        sys.exit(1)

    try:
        # Basic sanity check for JSON structure before handing to google library
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as je:
            print(f"ERROR: creds.json is not valid JSON: {je}", file=sys.stderr)
            sys.exit(1)

        # quick sanity checks for required fields
        if not isinstance(parsed, dict) or "private_key" not in parsed or "client_email" not in parsed:
            print(
                "ERROR: creds.json looks invalid (missing 'private_key' or 'client_email'). "
                "Make sure you downloaded the Service Account JSON from Google Cloud Console.",
                file=sys.stderr,
            )
            sys.exit(1)

        # Attempt to create Credentials object (this may raise errors for malformed content)
        creds = Credentials.from_service_account_file(path, scopes=scopes)

        # Optional check: credentials object created
        if creds is None:
            print("ERROR: Failed to create credentials object from file.", file=sys.stderr)
            sys.exit(1)

        # Return credentials for use with gspread
        return creds

    except (IOError, OSError) as ioe:
        print(f"ERROR: failed to open creds file: {ioe}", file=sys.stderr)
        sys.exit(1)
    except GoogleAuthError as gae:
        # This may capture token/jwt related issues
        print(f"ERROR: authentication error when loading credentials: {gae}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: unexpected error while loading credentials: {e}", file=sys.stderr)
        sys.exit(1)


# -----------------------
# Rate limiter decorator
# -----------------------
def rate_limited(min_interval: float = RATE_LIMIT_INTERVAL):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global _last_request_time
            now = time.time()
            elapsed = now - _last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            try:
                result = func(*args, **kwargs)
            except APIError as e:
                print(f"Google Sheets APIError in {func.__name__}: {e}", file=sys.stderr)
                raise
            except Exception as e:
                print(f"Unexpected error in {func.__name__}: {e}", file=sys.stderr)
                raise
            finally:
                _last_request_time = time.time()
            return result
        return wrapper
    return decorator


# -----------------------
# Safe sheet access helpers
# -----------------------
@rate_limited()
def read_range(sheet_name: str, cell_range: str) -> List[List[Any]]:
    global spreadsheet
    if spreadsheet is None:
        raise RuntimeError("Spreadsheet not initialized")
    try:
        ws = spreadsheet.worksheet(sheet_name)
    except WorksheetNotFound:
        raise RuntimeError(f"Worksheet '{sheet_name}' not found in spreadsheet.")
    # gspread.worksheet.get returns list of rows (list of lists)
    return ws.get(cell_range)


@rate_limited()
def write_range(sheet_name: str, cell_range: str, values: List[List[Any]]) -> None:
    global spreadsheet
    if spreadsheet is None:
        raise RuntimeError("Spreadsheet not initialized")
    try:
        ws = spreadsheet.worksheet(sheet_name)
    except WorksheetNotFound:
        raise RuntimeError(f"Worksheet '{sheet_name}' not found in spreadsheet.")
    # Use the Worksheet.update(a1_range, values) form
    ws.update(cell_range, values)


def read(sheet_name: str, cell: str) -> Any:
    vals = read_range(sheet_name, cell)
    if not vals or not vals[0]:
        raise ValueError(f"Empty result reading {sheet_name}!{cell}")
    return vals[0][0]


def read1(sheet_name: str, cell: str) -> List[List[Any]]:
    return read_range(sheet_name, cell)


def write(sheet_name: str, cell: str, value: Any) -> None:
    write_range(sheet_name, cell, [[value]])
    print(f"Wrote '{value}' to {sheet_name}!{cell}")


# -----------------------
# TCL calc logic (wrapped with safe reads/writes)
# -----------------------
def tcl_calc(price1: float, price2: float, symbol: str, type_: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Calculate limits, take-profits and SLs, write intermediate values to the sheet,
    and return order and tpsl dictionaries.

    type_ is one of: 'tcl1', 'tcl2', 'tcl3', 'tcl4'
    """
    sheet = "TCL Calc (10% Risk)"   # tab name in Google Sheets

    # Basic numeric sanity checks
    try:
        price1 = float(price1)
        price2 = float(price2)
    except (TypeError, ValueError):
        raise ValueError("price1 and price2 must be numeric")

    # Interpret trend and compute levels.
    # Keep your original approach but make calculations explicit & guarded.
    if price1 > price2:
        # Uptrend (as per original code)
        diff = price1 - price2
        L1 = price2 - diff * 0.618
        L2 = price2 - diff * 0.372
        L3 = price2 - diff * 0.17
        TP1 = price2 - diff * 1.272
        SL = price2 - diff * 0.05  # SL slightly below
        side = "Buy"
        write(sheet, "B5", "LONG")
    else:
        # Downtrend
        diff = price2 - price1
        L1 = price2 + diff * 0.618
        L2 = price2 + diff * 0.372
        L3 = price2 + diff * 0.17
        TP1 = price2 + diff * 1.272
        SL = price2 + diff * 0.05  # SL slightly above
        side = "Sell"
        write(sheet, "B5", "SHORT")

    # Batch write computed levels to sheet (guard with try/except to report if it fails)
    try:
        write_range(sheet, "C6:C8", [[L1], [TP1], [SL]])
        write_range(sheet, "C13:C14", [[L2], [L3]])
    except Exception as e:
        print(f"WARNING: Failed to write computed levels to sheet: {e}", file=sys.stderr)

    # Batch read block (D6:E14) - safe parsing with clear error messages
    try:
        values = read_range(sheet, "D6:E14")
    except Exception as e:
        raise RuntimeError(f"Failed to read required input block D6:E14 from sheet '{sheet}': {e}")

    # Defensive extraction of expected cells (fill defaults if missing)
    def _safe_float_from(values_block, r, c, default=0.0):
        try:
            return float(values_block[r][c])
        except Exception:
            return float(default)

    # Map expected structure:
    # D6 -> values[0][0], D13 -> values[7][0], D14 -> values[8][0]
    # E13 -> values[7][1], E14 -> values[8][1]
    qty1 = _safe_float_from(values, 0, 0, default=0.0)
    qty2 = _safe_float_from(values, 7, 0, default=0.0)
    qty3 = _safe_float_from(values, 8, 0, default=0.0)
    tp2  = _safe_float_from(values, 7, 1, default=0.0)
    tp3  = _safe_float_from(values, 8, 1, default=0.0)

    # margin_status might be at D9 -> values[3][0]
    margin_status = None
    try:
        margin_status = str(values[3][0])
    except Exception:
        margin_status = "UNKNOWN"

    # Position sizing & leverage computation
    try:
        position_size = (qty1 * L1) + (qty2 * L2) + (qty3 * L3)
        # avoid division by zero if ACC_SIZE is zero
        if ACC_SIZE <= 0:
            raise ValueError("ACC_SIZE must be > 0")
        leverage_input = round((position_size * 1.1) / ACC_SIZE)
    except Exception as e:
        print(f"WARNING computing leverage/position size: {e}", file=sys.stderr)
        leverage_input = 1

    # Write leverage back (safe attempt)
    try:
        write(sheet, "C9", leverage_input)
    except Exception as e:
        print(f"WARNING: failed to write leverage to sheet: {e}", file=sys.stderr)

    print("Leverage computed and written (if possible).")

    order_dict = {
        "limit1": L1,
        "limit2": L2,
        "limit3": L3,
        "qty1": qty1,
        "qty2": qty2,
        "qty3": qty3,
        "coin": symbol,
        "leverage": leverage_input,
        "side": side,
        "margin_status": margin_status,
    }

    # choose tpsl mapping based on type_
    tpsl_dict: Dict[str, Any]
    if type_ == "tcl1":
        tpsl_dict = {
            "tp1": TP1,
            "sl1": SL,
            "tp2": tp2,
            "sl2": SL,
            "tp3": tp3,
            "sl3": SL,
            "symbol": symbol
        }
    elif type_ == "tcl2":
        tpsl_dict = {
            "tp1": TP1,
            "sl1": SL,
            "tp2": TP1,
            "sl2": SL,
            "tp3": tp3,
            "sl3": SL,
            "symbol": symbol
        }
    elif type_ == "tcl3":
        tpsl_dict = {
            "tp1": TP1,
            "sl1": SL,
            "tp2": TP1,
            "sl2": SL,
            "tp3": tp2,
            "sl3": SL,
            "symbol": symbol
        }
    elif type_ == "tcl4":
        tpsl_dict = {
            "tp1": TP1,
            "sl1": SL,
            "tp2": TP1,
            "sl2": SL,
            "tp3": TP1,
            "sl3": SL,
            "symbol": symbol
        }
    else:
        raise ValueError(f"Unknown type_ '{type_}' - expected one of tcl1,tcl2,tcl3,tcl4")

    print("Order dict:", order_dict)
    print("TPSL dict:", tpsl_dict)
    return order_dict, tpsl_dict


# -----------------------
# Initialize Google Sheets client and spreadsheet
# -----------------------
def init_sheets(creds_file: str, spreadsheet_name: str) -> None:
    """
    Load credentials and connect to the named spreadsheet.
    Exits the process if authentication or access fails.
    """
    global client, spreadsheet

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = load_service_account_credentials(creds_file, SCOPES)

    try:
        client = gspread.authorize(creds)
    except Exception as e:
        print(f"ERROR: failed to authorize gspread with provided credentials: {e}", file=sys.stderr)
        # Provide a helpful hint for JWT errors
        if "JWT" in str(e) or "signature" in str(e).lower():
            print("HINT: This error often means the private_key in creds.json is malformed or missing.", file=sys.stderr)
        sys.exit(1)

    try:
        spreadsheet = client.open(spreadsheet_name)
        print(f"Connected to spreadsheet '{spreadsheet_name}'.")
    except SpreadsheetNotFound:
        print(f"ERROR: spreadsheet named '{spreadsheet_name}' not found for the authenticated account.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: failed to open spreadsheet '{spreadsheet_name}': {e}", file=sys.stderr)
        sys.exit(1)



        order, tpsl = tcl_calc(price1=100.0, price2=95.0, symbol="BTCUSD", type_="tcl1")
        print("tcl_calc completed successfully.")
    except Exception as e:
        print(f"tcl_calc failed: {e}", file=sys.stderr)
        # do not re-raise; exit gracefully in a script
        sys.exit(1)
