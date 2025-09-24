import os
import json
import time
import re
import base64
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

def start_spam_email_listener_recent(sender_email, callback, poll_interval=3):
    """
    Listens for Gmail SPAM messages from a specific sender received after script start.
    Runs callback(i, f1, f2, s1, s2) for each {int,float,float,str,str} block.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, "credentials.json")

    if not os.path.exists(creds_path):
        raise FileNotFoundError(f"credentials.json not found in {script_dir}")

    # ==== Load credentials ====
    with open(creds_path, "r") as f:
        creds_data = json.load(f)["installed"]

    creds = Credentials(
        token=creds_data.get("access_token"),
        refresh_token=creds_data.get("refresh_token"),
        token_uri=creds_data.get("token_uri"),
        client_id=creds_data.get("client_id"),
        client_secret=creds_data.get("client_secret"),
        scopes=creds_data.get("scopes")
    )

    # Refresh token if expired
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        creds_data["access_token"] = creds.token
        with open(creds_path, "w") as f:
            json.dump({"installed": creds_data}, f, indent=4)
        print("Token refreshed ✅")

    service = build("gmail", "v1", credentials=creds)
    seen_ids = set()

    # ==== Helper: extract {int,float,float,str,str} ====
    def extract_values_from_text(text):
        matches = re.findall(r"\{(.*?)\}", text)
        results = []
        for match in matches:
            parts = match.split(",")
            if len(parts) != 5:
                continue
            try:
                i = int(parts[0].strip())
                f1 = float(parts[1].strip())
                f2 = float(parts[2].strip())
                s1 = parts[3].strip()
                s2 = parts[4].strip()
                results.append((i, f1, f2, s1, s2))
            except ValueError:
                continue
        return results

    # ==== Body extractor ====
    def extract_body(payload):
        if payload.get("body") and payload["body"].get("data"):
            return base64.urlsafe_b64decode(payload["body"]["data"]).decode(errors="ignore")
        if "parts" in payload:
            for part in payload["parts"]:
                mime = part.get("mimeType", "")
                if mime in ["text/plain", "text/html"] and part.get("body", {}).get("data"):
                    return base64.urlsafe_b64decode(part["body"]["data"]).decode(errors="ignore")
                if "parts" in part:
                    inner = extract_body(part)
                    if inner:
                        return inner
        return ""

    print(f"Starting Spam listener for {sender_email} (polling every {poll_interval}s)...")

    # Record script start time in milliseconds
    script_start_ms = int(time.time() * 1000)

    while True:
        try:
            results = service.users().messages().list(
                userId="me",
                labelIds=["SPAM"],
                maxResults=10
            ).execute()

            messages = results.get("messages", [])

            for msg in messages:
                if msg["id"] in seen_ids:
                    continue

                msg_data = service.users().messages().get(
                    userId="me",
                    id=msg["id"],
                    format="full"
                ).execute()

                # Filter by recent emails only
                internal_date = int(msg_data.get("internalDate", 0))
                if internal_date < script_start_ms:
                    continue  # skip old messages

                seen_ids.add(msg["id"])

                headers = msg_data["payload"].get("headers", [])
                sender = next((h["value"] for h in headers if h["name"] == "From"), "<no sender>")

                if sender_email.lower() not in sender.lower():
                    continue

                body = extract_body(msg_data["payload"]) or msg_data.get("snippet", "")

                values = extract_values_from_text(body)
                if values:
                    for i, f1, f2, s1, s2 in values:
                        callback(i, f1, f2, s1, s2)
                    print(f"Processed SPAM email from '{sender}' | Message ID: {msg['id']} ✅")
                else:
                    print("No matching {…} text found in the email.")

        except Exception as e:
            print("Error fetching emails:", e)

        time.sleep(poll_interval)


# ===== Example usage =====
if __name__ == "__main__":
    def main(i, f1, f2, s1, s2):
        print(f"int={i}, float1={f1}, float2={f2}, str1='{s1}', str2='{s2}'")

    start_spam_email_listener_recent(sender_email="hellotagvg1@gmail.com", callback=main)
