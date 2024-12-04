#!/usr/bin/env python
# coding: utf-8

# In[19]:


import os
import time
from datetime import datetime
import pandas as pd
from atproto import Client
import keyring

# Constants
ROOT_HANDLE = "elaval.bsky.social"
STATE_FILES = {
    "log": "followers_log.parquet",
    "master": "master.parquet",
}
RATE_LIMIT_DELAY = 0.2  # Delay between API calls to respect rate limits
EXEMPTIONS = {"ap.brid.gy"}

def get_password():
    """Retrieve the password based on the environment."""
    # Check if running in a GitHub Actions environment
    github_password = os.getenv("BLUESKY_PASSWORD")
    if github_password:
        print("Using GitHub Actions secret for password.")
        return github_password

    # Otherwise, use keyring for local development
    print("Using keyring for password.")
    password = keyring.get_password("bluesky", ROOT_HANDLE)
    if not password:
        raise ValueError(
            "Password not found in keyring. Please set it using keyring for local use or as a GitHub secret for CI/CD."
        )
    return password

# Helper Functions
def save_parquet(file_name, data):
    """Save data to a parquet file."""
    data.to_parquet(file_name, index=False)

def load_parquet(file_name, columns=None):
    """Load data from a parquet file, initializing if missing."""
    if os.path.exists(file_name):
        return pd.read_parquet(file_name)
    # Initialize with the correct schema if the file doesn't exist
    if columns:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame()

# BlueSky Functions
def fetch_profile(client, handle):
    """Fetch a user profile from the BlueSky API."""
    try:
        print(f"Fetching profile for {handle}...")
        profile = client.get_profile(actor=handle)
        record = {
            "handle": handle,
            "displayName": profile.display_name or "",
            "description": profile.description or "",
            "followers_count": profile.followers_count or 0,
            "created_at": profile.created_at,
            "timestamp": datetime.now(),
        }
        print(f"Fetched profile: {record}")
        return record
    except Exception as e:
        print(f"Error fetching profile for {handle}: {e}")
        return None

def fetch_followers(client, handle):
    """Fetch the list of followers for a given user."""
    followers = []
    cursor = None
    try:
        print(f"Fetching followers for {handle}...")
        while True:
            response = client.get_followers(actor=handle, limit=50, cursor=cursor)
            followers.extend([f.handle for f in response.followers])
            if not response.cursor:
                break
            cursor = response.cursor
            time.sleep(RATE_LIMIT_DELAY)
    except Exception as e:
        print(f"Error fetching followers for {handle}: {e}")
    return followers

# Data Update Functions
def update_master_file(profile, master_data):
    """Update the master file with the latest profile information."""
    new_row = pd.DataFrame([{
        "handle": profile["handle"],
        "displayName": profile["displayName"],
        "description": profile["description"],
        "followers_count": profile["followers_count"],
        "created_at": profile.get("created_at", ""),
        "timestamp": profile["timestamp"],
    }])
    master_data = master_data[master_data["handle"] != profile["handle"]]
    master_data = pd.concat([master_data, new_row], ignore_index=True)
    return master_data

def update_log_file(profile, log_data):
    """Update the log file with the latest follower count."""
    new_row = pd.DataFrame([{
        "handle": profile["handle"],
        "followers_count": profile["followers_count"],
        "timestamp": profile["timestamp"],
    }])
    log_data = pd.concat([log_data, new_row], ignore_index=True)
    return log_data

# Main Processing
def process_followers(client, root_handle, master_file, log_file):
    """Process followers for the root handle."""
    # Initialize column structure
    master_columns = [
        "handle", "displayName", "description", "followers_count", "created_at", "timestamp"
    ]
    log_columns = ["handle", "followers_count", "timestamp"]

    # Load existing data
    master_data = load_parquet(master_file, columns=master_columns)
    log_data = load_parquet(log_file, columns=log_columns)

    # Process the root user's profile
    root_profile = fetch_profile(client, root_handle)
    if root_profile:
        print("Updating master and log for root user...")
        master_data = update_master_file(root_profile, master_data)
        log_data = update_log_file(root_profile, log_data)

    # Fetch and process followers
    print("Requesting followers...")
    followers = fetch_followers(client, root_handle)
    print(f"Got {len(followers)} followers.")
    for i, follower in enumerate(followers, start=1):
        if follower in EXEMPTIONS:
            print(f"Skipping {follower} due to exemption.")
            continue

        print(f"Processing follower {follower} ({i}/{len(followers)})...")
        profile = fetch_profile(client, follower)
        if profile:
            try:
                master_data = update_master_file(profile, master_data)
                log_data = update_log_file(profile, log_data)
            except Exception as e:
                print(f"Error updating master/log for {follower}: {e}")

        # Save state periodically
        if i % 10 == 0 or i == len(followers):
            print(f"Saving state after processing {i} followers...")
            save_parquet(master_file, master_data)
            save_parquet(log_file, log_data)

    # Final save
    print("Final save of master and log data.")
    save_parquet(master_file, master_data)
    save_parquet(log_file, log_data)

# Main Function
def main():
    elmtest = os.getenv("ELMTEST")
    if elmtest:
        print("Got elmtest.")
        print(elmtest)
    
    password = get_password()
    client = Client()
    client.login(ROOT_HANDLE, password)
    profile = client.get_profile('elaval.bsky.social')
    print(profile)

    # Process followers and update data files
    try:
        print("Main try")
        process_followers(client, ROOT_HANDLE, STATE_FILES["master"], STATE_FILES["log"])
    except KeyboardInterrupt:
        print("Interrupted by user. Saving state...")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:




