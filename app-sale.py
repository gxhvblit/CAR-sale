import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# --- CONFIG ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/17Nq4MVLOKtdantiDayXwAgPRZKCvkI1FD4n7FJMZlJo/edit#gid=0"
COL_MAP_DEFAULT = {
    "total": "Unnamed: 29",
    "ppv_suv_base": "Unnamed: 14",
    "pickup": ["Unnamed: 20", "Unnamed: 21", "Unnamed: 22", "Unnamed: 23", "Unnamed: 24"],
    "commercial": ["Unnamed: 17", "Unnamed: 18", "Unnamed: 19", "Unnamed: 28"],
}

# --- FUNCTIONS ---
def get_gspread_client():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    # ดึงค่าจาก st.secrets แทนไฟล์ JSON
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def process_excel(file, year, month):
    # รองรับทั้ง .xls และ .xlsx
    df = pd.read_excel(file, sheet_name="Retail Sales Record by Brand")
    ttl_rows = df[df.astype(str).apply(lambda row: row.str.contains("TTL.", na=False)).any(axis=1)].copy()
    
    if len(ttl_rows) < 2:
        return None
        
    row_idx = ttl_rows.index.tolist()[1] 

    pick_up = sum(df.loc[row_idx, col] for col in COL_MAP_DEFAULT["pickup"])
    comm = sum(df.loc[row_idx, col] for col in COL_MAP_DEFAULT["commercial"])
    total_val = df.loc[row_idx, COL_MAP_DEFAULT["total"]]
    ppv = total_val - df.loc[row_idx, COL_MAP_DEFAULT["ppv_suv_base"]]
    pass_car = total_val - pick_up - comm - ppv

    return {
        "Month": month, "Year": year,
        "Passenger": int(pass_car), "Pickup": int(pick_up), 
        "Commercial": int(comm), "PPV_SUV": int(ppv), "Total": int(total_val)
    }

# --- UI ---
st.title("🚀 Sales Data Uploader")

with st.sidebar:
    st.header("⚙️ Setting")
    year = st.selectbox("Year", ["2567", "2568", "2569"])
    month = st.selectbox("Month", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    file = st.file_uploader("Upload Excel File", type=["xls", "xlsx"])

if file and st.button("Extract & Upload to Google Sheets"):
    try:
        data = process_excel(file, year, month)
        if data:
            gc = get_gspread_client()
            sh = gc.open_by_url(SHEET_URL)
            worksheet = sh.get_worksheet(0)
            
            # ดึงข้อมูลเก่า และเพิ่มบรรทัดใหม่
            existing_df = get_as_dataframe(worksheet).dropna(how='all').dropna(axis=1, how='all')
            new_row_df = pd.DataFrame([data])
            updated_df = pd.concat([existing_df, new_row_df], ignore_index=True)
            
            set_with_dataframe(worksheet, updated_df)
            st.success(f"Uploaded {month} {year} successfully!")
            st.dataframe(new_row_df)
        else:
            st.warning("Could not find 'TTL.' row in the Excel sheet.")
    except Exception as e:
        st.error(f"Error: {e}")

