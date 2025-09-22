
import os
import json
import time
import re
import base64
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

def start_email_listener(sender_email, callback, poll_interval=3):
    """
    Listens for Gmail messages from a specific sender and runs `callback(i, f1, f2, s1, s2)`
    for each {{int,float,float,str,str}} block.
    """
    # ==== Locate credentials.json dynamically ====
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
        print("Token refreshed ���")

    # Build Gmail API service
    service = build("gmail", "v1", credentials=creds)

    # Track already seen emails
    seen_ids = set()

    # ==== Helper to extract {{int,float,float,str,str}} ====
    def extract_values_from_text(text):
        matches = re.findall(r"\{\{(.*?)\}\}", text)
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

    print(f"Starting Gmail listener for {sender_email} (polling every {poll_interval}s)...")

    # ==== Poll Gmail forever ====
    while True:
        try:
            results = service.users().messages().list(
                userId="me",
                labelIds=["INBOX"],
                maxResults=10
            ).execute()

            messages = results.get("messages", [])

            for msg in messages:
                if msg["id"] in seen_ids:
                    continue
                seen_ids.add(msg["id"])

                msg_data = service.users().messages().get(
                    userId="me",
                    id=msg["id"],
                    format="full"
                ).execute()

                headers = msg_data["payload"].get("headers", [])
                sender = next((h["value"] for h in headers if h["name"] == "From"), "")

                if sender_email not in sender:
                    continue

                # Extract plain text body
                body = ""
                if "parts" in msg_data["payload"]:
                    for part in msg_data["payload"]["parts"]:
                        if part["mimeType"] == "text/plain" and "data" in part["body"]:
                            body = base64.urlsafe_b64decode(part["body"]["data"]).decode()
                            break
                    if not body:
                        body = msg_data.get("snippet", "")
                else:
                    body = msg_data.get("snippet", "")

                # Extract values and run callback
                values = extract_values_from_text(body)
                if values:
                    for i, f1, f2, s1, s2 in values:
                        callback(i, f1, f2, s1, s2)
                else:
                    print("No matching {{���}} text found in the email.")

        except Exception as e:
            print("Error fetching emails:", e)

        time.sleep(poll_interval)

# ===== Example usage =====
if __name__ == "__main__":
    def main(i, f1, f2, s1, s2):
        print(f"int={i}, float1={f1}, float2={f2}, str1='{s1}', str2='{s2}'")

