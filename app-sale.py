import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# --- SETTINGS & CONFIG ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/17Nq4MVLOKtdantiDayXwAgPRZKCvkI1FD4n7FJMZlJo/edit#gid=0"
JSON_KEY_FILE = 'my-python-sale.json'
COL_MAP_DEFAULT = {
    "total": "Unnamed: 27",
    "ppv_suv_base": "Unnamed: 14",
    "pickup": ["Unnamed: 19", "Unnamed: 20", "Unnamed: 21", "Unnamed: 22"],
    "commercial": ["Unnamed: 16", "Unnamed: 17", "Unnamed: 18", "Unnamed: 26"],
}

# --- FUNCTIONS ---
def get_gspread_client():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(JSON_KEY_FILE, scopes=scopes)
    return gspread.authorize(creds)

def process_excel(file, year, month):
    """ Logic การคำนวณจาก Code เดิมของคุณ """
    df = pd.read_excel(file, sheet_name="Retail Sales Record by Brand")
    # ค้นหาแถวที่มีคำว่า TTL.
    ttl_rows = df[df.astype(str).apply(lambda row: row.str.contains("TTL.", na=False)).any(axis=1)].copy()
    
    if len(ttl_rows) < 2:
        return None
        
    row_idx = ttl_rows.index.tolist()[1] # แถวที่ต้องการตาม Logic เดิม

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

# --- UI INTERFACE ---
st.set_page_config(page_title="Excel to Google Sheet Automation", layout="wide")
st.title("🚗 ระบบจัดการข้อมูลยอดขายรถยนต์ (Routine)")

with st.sidebar:
    st.header("ตั้งค่าข้อมูล")
    selected_year = st.selectbox("เลือกปี (พ.ศ.)", ["2567", "2568", "2569"])
    selected_month = st.selectbox("เลือกเดือน", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    uploaded_file = st.file_uploader("อัปโหลดไฟล์ Excel (.xls, .xlsx)", type=["xls", "xlsx"])

if uploaded_file and st.button("เริ่มประมวลผลและอัปโหลด"):
    try:
        with st.spinner('กำลังอ่านไฟล์และคำนวณ...'):
            # 1. คำนวณข้อมูลจากไฟล์ที่อัปโหลด
            new_data = process_excel(uploaded_file, selected_year, selected_month)
            
            if new_data:
                # 2. เชื่อมต่อ Google Sheets
                gc = get_gspread_client()
                sh = gc.open_by_url(SHEET_URL)
                worksheet = sh.get_worksheet(0)
                
                # 3. ดึงข้อมูลเก่าลงมาเพื่อ Append (หรือเช็คข้อมูลซ้ำ)
                existing_df = get_as_dataframe(worksheet).dropna(how='all').dropna(axis=1, how='all')
                new_row_df = pd.DataFrame([new_data])
                
                # รวมข้อมูลเก่าและใหม่
                updated_df = pd.concat([existing_df, new_row_df], ignore_index=True)
                
                # 4. เขียนกลับลงไป
                set_with_dataframe(worksheet, updated_df)
                
                st.success(f"✅ อัปโหลดข้อมูลเดือน {selected_month} {selected_year} เรียบร้อยแล้ว!")
                st.table(new_row_df)
            else:
                st.error("ไม่พบแถวข้อมูล 'TTL.' ในไฟล์ที่อัปโหลด")
    except Exception as e:
        st.error(f"เกิดข้อผิดพลาด: {e}")

st.divider()
st.info("คำแนะนำ: เลือกเดือนและปีให้ถูกต้องก่อนอัปโหลด เพื่อให้ข้อมูลใน Google Sheets ไม่สับสน")