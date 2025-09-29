#!/usr/bin/env python3
"""
Create a Google Drive OAuth token.json from a credentials.json (Desktop client).

Usage examples:
  # Use values from config.Settings
  python create_gdrive_token.py

  # Override explicitly
  python create_gdrive_token.py \
    --credentials /u/nathanj/national_ml/data/script/credentials.json \
    --token /u/nathanj/national_ml/data/script/token.json \
    --scopes https://www.googleapis.com/auth/drive \
    --verify
"""

import argparse
import os
import sys
from pathlib import Path

# Google auth / API libs
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Try to load your existing Settings (optional)
DEFAULT_CREDENTIALS = None
DEFAULT_TOKEN = None
DEFAULT_SCOPES = None
try:
    from config import Settings  # your project config
    _s = Settings()
    DEFAULT_CREDENTIALS = getattr(_s, "GDRIVE_CREDENTIALS_FILE", None)
    DEFAULT_TOKEN = getattr(_s, "GDRIVE_TOKEN_FILE", None)
    DEFAULT_SCOPES = getattr(_s, "GDRIVE_SCOPES", None)
except Exception:
    # If config import fails, we'll rely on CLI flags
    pass


def parse_args():
    p = argparse.ArgumentParser(description="Generate Google Drive token.json from credentials.json (console flow).")
    p.add_argument(
        "--credentials", "-c",
        default=DEFAULT_CREDENTIALS,
        help=f"Path to OAuth client credentials.json (Desktop app). Default from config.Settings if available: {DEFAULT_CREDENTIALS!r}"
    )
    p.add_argument(
        "--token", "-t",
        default=DEFAULT_TOKEN,
        help=f"Path to write token.json. Default from config.Settings if available: {DEFAULT_TOKEN!r}"
    )
    p.add_argument(
        "--scopes", "-s",
        default=",".join(DEFAULT_SCOPES) if isinstance(DEFAULT_SCOPES, (list, tuple)) else
                (DEFAULT_SCOPES or "https://www.googleapis.com/auth/drive"),
        help="Comma-separated OAuth scopes. Defaults to config.Settings.GDRIVE_SCOPES or Drive full scope."
    )
    p.add_argument(
        "--verify",
        action="store_true",
        help="After saving the token, call Drive API to verify it works."
    )
    p.add_argument(
        "--force-consent",
        action="store_true",
        help="Force Google consent screen (ensures a fresh refresh token)."
    )
    return p.parse_args()


def main():
    args = parse_args()

    if not args.credentials or not args.token:
        print("ERROR: Missing --credentials and/or --token. Either provide them via CLI or ensure config.Settings is available.",
              file=sys.stderr)
        sys.exit(1)

    credentials_file = Path(args.credentials).expanduser().resolve()
    token_file = Path(args.token).expanduser().resolve()
    scopes = [s.strip() for s in args.scopes.split(",") if s.strip()]

    if not credentials_file.exists():
        print(f"ERROR: credentials file not found: {credentials_file}", file=sys.stderr)
        sys.exit(1)

    # Ensure target dir exists
    token_file.parent.mkdir(parents=True, exist_ok=True)

    # Build the OAuth flow; we’ll use the console-friendly approach.
    flow = InstalledAppFlow.from_client_secrets_file(
        str(credentials_file),
        scopes=scopes,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob'
    )

    # Construct the authorization URL with offline access and (optionally) forced consent
    auth_kwargs = {
        "access_type": "offline",
        "include_granted_scopes": "true",
    }
    if args.force_consent:
        auth_kwargs["prompt"] = "consent"

    auth_url, _ = flow.authorization_url(**auth_kwargs)

    print("\n== Google Authorization ==")
    print("1) Open this URL in a browser on your laptop:")
    print(auth_url)
    print("2) Complete login and copy the authorization code.")
    code = input("\nPaste the authorization code here: ").strip()

    # Exchange code for tokens
    flow.fetch_token(code=code)
    creds = flow.credentials

    # Save token.json
    with token_file.open("w") as f:
        f.write(creds.to_json())

    print(f"\n✅ Token saved to: {token_file}")

    if args.verify:
        try:
            service = build("drive", "v3", credentials=creds, cache_discovery=False)
            about = service.about().get(fields="user/displayName, user/emailAddress").execute()
            user = about.get("user", {})
            print(f"🔎 Verified as: {user.get('displayName', 'Unknown')} <{user.get('emailAddress', 'unknown')}>")
        except HttpError as e:
            print(f"WARNING: Token saved, but Drive API verify failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
