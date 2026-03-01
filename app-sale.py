import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# --- CONFIG ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/17Nq4MVLOKtdantiDayXwAgPRZKCvkI1FD4n7FJMZlJo/edit#gid=0"

# --- FUNCTIONS ---
def get_gspread_client():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def process_excel(file, year, month):
    # อ่านไฟล์โดยไม่กำหนด Header เพื่อสแกนหาพิกัดเอง
    df = pd.read_excel(file, sheet_name="Retail Sales Record by Brand", header=None)
    
    # 1. ค้นหาแถว "TTL." แถวแรกที่เจอ (เลี่ยงตารางสรุปด้านล่าง)
    ttl_indices = df[df.iloc[:, 0].astype(str).str.contains("TTL.", na=False)].index.tolist()
    if not ttl_indices:
        return None
    row_idx = ttl_indices[0] 

    # 2. รวมชื่อ Header จากแถวที่ 5 และ 6 (เผื่อมีการ Merge Cell)
    # เราสแกนหาชื่อคอลัมน์จากข้อความที่ปรากฏจริงในไฟล์
    header_row = df.iloc[4:7, :].astype(str).apply(lambda x: " ".join(x), axis=0).tolist()
    
    def find_cols_by_keywords(keywords):
        return [i for i, text in enumerate(header_row) if any(k.upper() in text.upper() for k in keywords)]

    # --- กำหนดตำแหน่งคอลัมน์แบบ Dynamic ---
    col_total = df.shape[1] - 1  # คอลัมน์สุดท้าย
    col_comm_subtotal = df.shape[1] - 2 # คอลัมน์รองสุดท้าย
    
    # หาคอลัมน์ Pickup (สแกนหาคำว่า PICK UP 1 TON และ DOUBLE CAB)
    pickup_cols = find_cols_by_keywords(["PICK UP 1 TON", "DOUBLE CAB"])
    
    # หาคอลัมน์ Commercial (VAN, BUS, PICK UP < 1 TON)
    comm_cols = find_cols_by_keywords(["VAN", "BUS", "PICK UP < 1 TON"])

    # หาคอลัมน์ PPV
    ppv_indices = find_cols_by_keywords(["PPV"])
    col_ppv = ppv_indices[0] if ppv_indices else None

    # 3. ดึงค่าและคำนวณตาม Logic ของคุณ
    try:
        def clean_val(c_idx):
            if c_idx is None: return 0
            val = df.iloc[row_idx, c_idx]
            return pd.to_numeric(val, errors='coerce') if pd.notnull(val) else 0

        total_val = clean_val(col_total)
        pickup_val = sum(clean_val(c) for c in pickup_cols)
        comm_val = sum(clean_val(c) for c in comm_cols)
        comm_sub_val = clean_val(col_comm_subtotal)
        
        # เลือกค่าที่มากที่สุดระหว่างผลรวมกลุ่ม หรือคอลัมน์สรุปรองสุดท้าย
        final_comm = max(comm_val, comm_sub_val)
        
        ppv_val = clean_val(col_ppv)
        
        # Passenger = Total - Pickup - Commercial - PPV
        passenger_val = total_val - pickup_val - final_comm - ppv_val

        return {
            "Month": month, "Year": year,
            "Passenger": int(passenger_val),
            "Pickup": int(pickup_val),
            "Commercial": int(final_comm),
            "PPV_SUV": int(ppv_val),
            "Total": int(total_val)
        }
    except Exception as e:
        st.error(f"Error during calculation: {e}")
        return None

# --- UI ---
st.set_page_config(page_title="Auto Sales Uploader", layout="wide")
st.title("🚗 ระบบกรองข้อมูลยอดขายรถยนต์ (Dynamic Version)")

with st.sidebar:
    st.header("⚙️ Setting")
    year = st.selectbox("Year (พ.ศ.)", ["2567", "2568", "2569"])
    month = st.selectbox("Month", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    file = st.file_uploader("Upload Excel File", type=["xls", "xlsx"])

if file and st.button("Extract & Upload"):
    try:
        with st.spinner('กำลังประมวลผล...'):
            data = process_excel(file, year, month)
            if data:
                gc = get_gspread_client()
                sh = gc.open_by_url(SHEET_URL)
                worksheet = sh.get_worksheet(0)
                
                # อ่านข้อมูลเดิมเพื่อ Append
                existing_df = get_as_dataframe(worksheet).dropna(how='all').dropna(axis=1, how='all')
                new_row_df = pd.DataFrame([data])
                
                # เช็คซ้ำ (ถ้ามี Year และ Month เดียวกันให้เอาอันใหม่แทนที่)
                if not existing_df.empty:
                    existing_df = existing_df[~((existing_df['Month'] == month) & (existing_df['Year'] == year))]
                
                updated_df = pd.concat([existing_df, new_row_df], ignore_index=True)
                
                # เขียนลง Sheet
                set_with_dataframe(worksheet, updated_df)
                st.success(f"อัปโหลดข้อมูล {month} {year} สำเร็จ!")
                st.table(new_row_df)
            else:
                st.error("ไม่สามารถระบุพิกัดข้อมูลในไฟล์ได้ กรุณาเช็คโครงสร้างไฟล์")
    except Exception as e:
        st.error(f"เกิดข้อผิดพลาด: {e}")




