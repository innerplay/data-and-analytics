"""
Script to generate a refresh token for Google Ads API OAuth2 authentication.
Run this script, authorize in the browser, and copy the refresh token to google_ads.yaml
"""

from google_auth_oauthlib.flow import InstalledAppFlow

# Your OAuth2 credentials from google_ads.yaml
CLIENT_ID = "552258874559-pp8krmhp7rt9ljr21c55uo49s56ekct8.apps.googleusercontent.com"
CLIENT_SECRET = "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"

# Google Ads API scope
SCOPES = ["https://www.googleapis.com/auth/adwords"]

def main():
    # Create OAuth2 flow
    client_config = {
        "installed": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)
    
    # Run the OAuth2 flow (opens browser)
    print("Opening browser for authorization...")
    print("Please log in with an account that has access to your Google Ads account.\n")
    
    credentials = flow.run_local_server(port=8080)
    
    print("\n" + "=" * 60)
    print("SUCCESS! Here is your refresh token:")
    print("=" * 60)
    print(f"\n{credentials.refresh_token}\n")
    print("=" * 60)
    print("\nCopy this token and paste it into google_ads.yaml as refresh_token")


if __name__ == "__main__":
    main()
