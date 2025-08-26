import requests as rq
import pandas as pd
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
from dotenv import load_dotenv

load_dotenv()

coins = ["bitcoin", "ethereum"]
all_data = []
df_coins = pd.DataFrame()

for coin in coins:
    urlCompany = f"{os.environ.get('VARIABLE-COMPANIES-URL')}/{coin}"
    responseCompany = rq.get(urlCompany)

    if responseCompany.status_code == 200:
        data = responseCompany.json()
        df_company = pd.json_normalize(data["companies"])
        df_company["id"] = coin
        all_data.append(df_company)
    else:
        print(f"Error with {coin}: {responseCompany.status_code}")

df_companies = pd.concat(all_data, ignore_index=True)

urlCoin = os.environ.get("VARIABLE-COINS-URL")
responseCoin = rq.get(urlCoin)

if responseCoin.status_code == 200:
    df_coins = pd.json_normalize(responseCoin.json())
else:
    print(f"Error: {responseCoin.status_code}")

dataframe = pd.merge(df_companies, df_coins, on="id")

dataframe.to_csv("file.csv", index=False)

SCOPES = ['https://www.googleapis.com/auth/drive.file']

creds = None
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(os.environ.get("VARIABLE-AUTH-SECRET"), SCOPES)
        creds = flow.run_local_server(port=0)
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

drive_service = build('drive', 'v3', credentials=creds)

file_metadata = {
    'name': 'file.csv',
    'parents': [os.environ.get("VARIABLE-FOLDER-ID")]
}
media = MediaFileUpload("file.csv", mimetype='text/csv')

file = drive_service.files().create(
    body=file_metadata,
    media_body=media,
    fields='id'
).execute()

print("File uploaded to GDrive successfully")