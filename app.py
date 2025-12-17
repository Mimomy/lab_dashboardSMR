import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
from datetime import datetime
import time
import uuid

# --- CONFIGURAZIONE ---
SHEET_NAME = "DB_Respirometria"

# Pesi specifici delle Falcon
FALCON_DATASETS = {
    "Set Normal": [9.940, 10.108, 10.002, 9.976, 9.955, 9.967, 9.956, 9.979, 9.936, 9.919, 9.997, 9.934],
    "Set Bold":   [9.974, 9.974, 9.954, 9.924, 9.967, 9.987, 9.948, 9.972, 9.987, 9.980, 9.994, 9.982]
}

# --- CONNESSIONE GOOGLE SHEETS ---
def get_connection():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    # Su Streamlit Cloud le credenziali sono in st.secrets
    # In locale, se vuoi testare, devi gestire il file json diversamente o usare st.secrets locale
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)

# --- FUNZIONI DI SUPPORTO ---
def check_login(username, password, sh):
    try:
        ws = sh.worksheet("Users")
        users = ws.get_all_records()
        for u in users:
            if str(u['Username']) == str(username) and str(u['Password']) == str(password):
                return u['Nome_Completo']
    except:
        st.error("Errore foglio Users.")
    return None

def check_todays_experiment(sh, username):
    """Controlla se ci sono esperimenti APERTI con data di OGGI"""
    ws = sh.worksheet("DB_Respirometria")
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    if df.empty: return False, None
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    try:
        # Cerca: Utente Corretto + Data Oggi + Stato NON CHIUSO
        subset = df[ 
            (df['Operatore'] == username) & 
            (df['Data'] == today_str) &
            (df['Stato'] != 'CHIUSO')
        ]
        if not subset.empty:
            proj = subset.iloc[0]['Project_Name']
            return True, proj
    except KeyError:
        pass
    return False, None

def save_session_state(username, start_time_str, project, sh):
    ws = sh.worksheet("Active_Sessions")
    try:
        cell = ws.find(username)
        ws.update_cell(cell.row, 2, start_time_str)
        ws.update_cell(cell.row, 3, project)
    except:
        ws.append_row([username, start_time_str, project])

def load_session_state(username, sh):
    ws = sh.worksheet("Active_Sessions")
    try:
        cell = ws.find(username)
        vals = ws.row_values(cell.row)
        return vals[1], vals[2] # Time, Project
    except:
        return None, None

def clear_session_state(username, sh):
    ws = sh.worksheet("Active_Sessions")
    try:
        cell = ws.find(username)
        ws.delete_rows(cell.row)
    except:
        pass

def get_project_names(sh):
    ws = sh.worksheet("DB_Respirometria")
    try:
        df = pd.DataFrame(ws.get_all_records())
        if not df.empty:
            return sorted(list(set(df['Project_Name'].astype(str))))
    except:
        pass
    return []

# --- UI SETUP ---
st.set_page_config(page_title="Lab Dashboard", layout="wide", page_icon="🔬")
st.title("🔬 Respirometria Lab")

# Inizializzazione Session State
if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'username' not in st.session_state: st.session_state.username = ""
if 'choice_made' not in st.session_state: st.session_state.choice_made = False
if 'work_mode' not in st.session_state: st.session_state.work_mode = "NEW"

# --- 1. LOGIN ---
if not st.session_state.logged_in:
    with st.form("login"):
        user = st.text_input("Username")
        pwd = st.text_input("Password", type="password")
        if st.form_submit_button("Accedi"):
            try:
                sh = get_connection()
                real_name = check_login(user, pwd, sh)
                if real_name:
                    st.session_state.logged_in = True
                    st.session_state.username = user
                    st.session_state.real_name = real_name
                    st.rerun()
                else:
                    st.error("Credenziali Errate")
            except Exception as e:
                st.error(f"Errore connessione: {e}")
    st.stop()

# --- 2. LOGICA POST-LOGIN ---
sh = get_connection()
st.sidebar.write(f"Operatore: **{st.session_state.real_name}**")
if st.sidebar.button("Logout"):
    st.session_state.logged_in = False
    st.rerun()

menu = st.sidebar.radio("Menu", ["1. Misurazione (Day 0)", "2. Pesi (Day 3)", "3. Export Dati"])

# =============================================================================
# SEZIONE 1: MISURAZIONE (DAY 0)
# =============================================================================
if menu == "1. Misurazione (Day 0)":
    
    # Check Automatico "Smart Resume" (solo la prima volta)
    if not st.session_state.choice_made:
        has_today, proj_today = check_todays_experiment(sh, st.session_state.username)
        
        # Check se c'è un timer attivo nel cloud
        cloud_time, cloud_proj = load_session_state(st.session_state.username, sh)
        
        if cloud_time:
            st.info(f"⏱️ Hai un timer attivo su '{cloud_proj}' iniziato alle {cloud_time}.")
            st.session_state.timer_start = datetime.strptime(cloud_time, "%Y-%m-%d %H:%M:%S")
            st.session_state.work_mode = "NEW" # Continua in modalità nuovo/timer
            st.session_state.choice_made = True
            st.rerun()
            
        elif has_today:
            st.info(f"📅 Trovati dati aperti di OGGI nel progetto: **{proj_today}**")
            c1, c2 = st.columns(2)
            if c1.button("📂 APRI DATI DI OGGI (Resume)", use_container_width=True, type="primary"):
                st.session_state.work_mode = "RESUME"
                st.session_state.choice_made = True
                st.rerun()
            if c2.button("➕ NUOVO SET (Ignora)", use_container_width=True):
                st.session_state.work_mode = "NEW"
                st.session_state.choice_made = True
                st.rerun()
        else:
            st.session_state.work_mode = "NEW"
            st.session_state.choice_made = True
            st.rerun()

    # --- MODE: NUOVO SET (Timer + Falcon + Animali) ---
    if st.session_state.work_mode == "NEW":
        st.subheader("Nuovo Set Sperimentale")
        
        # Timer Logic
        col_t1, col_t2, col_t3 = st.columns([1,1,2])
        current_time_val = 0.0
        
        if 'timer_start' in st.session_state and st.session_state.timer_start:
            delta = datetime.now() - st.session_state.timer_start
            current_time_val = delta.total_seconds() / 60
            with col_t3: st.warning(f"⏳ Timer: {current_time_val:.2f} min")
            
            if col_t2.button("⏹️ STOP"):
                st.session_state.final_time = current_time_val
                clear_session_state(st.session_state.username, sh)
                del st.session_state['timer_start']
                st.rerun()
        else:
            if 'final_time' in st.session_state:
                current_time_val = st.session_state.final_time
                with col_t3: st.success(f"Tempo Registrato: {current_time_val:.2f} min")
            
            if col_t1.button("▶️ START"):
                now = datetime.now()
                st.session_state.timer_start = now
                save_session_state(st.session_state.username, now.strftime("%Y-%m-%d %H:%M:%S"), "InCorso", sh)
                if 'final_time' in st.session_state: del st.session_state['final_time']
                st.rerun()

        # Setup Variabili
        c_p1, c_p2 = st.columns(2)
        with c_p1:
            projs = get_project_names(sh)
            mode_p = st.radio("Progetto", ["Esistente", "Nuovo"], horizontal=True)
            if mode_p == "Esistente" and projs:
                proj_name = st.selectbox("Seleziona", projs)
            else:
                proj_name = st.text_input("Nome Nuovo Progetto")
        
        with c_p2:
            num_animali = st.number_input("N. Animali", 1, 20, 5)
            temp = st.number_input("Temp (°C)", value=20.0)
            press = st.number_input("Pressione (mBar)", value=1013.0)
            set_falcon = st.selectbox("Set Falcon", list(FALCON_DATASETS.keys()))

        # Tabella Falcon
        st.markdown("##### 1. Pesi Falcon")
        weights = FALCON_DATASETS[set_falcon]
        # Adatta pesi al numero animali
        current_weights = (weights * (num_animali // len(weights) + 1))[:num_animali]
        
        df_falcon = pd.DataFrame({
            "Falcon_ID": range(1, num_animali+1),
            "Peso_Vuoto": current_weights,
            "Peso_Pieno": [0.0]*num_animali,
            "Minuti": [current_time_val]*num_animali
        })
        
        edited_falcon = st.data_editor(df_falcon, hide_index=True, key="f_edit", column_config={
            "Peso_Vuoto": st.column_config.NumberColumn(disabled=True, format="%.3f"),
            "Minuti": st.column_config.NumberColumn(format="%.2f")
        })
        
        # Calcolo FR
        fr_list = []
        for i, row in edited_falcon.iterrows():
            if row['Minuti'] > 0 and row['Peso_Pieno'] > row['Peso_Vuoto']:
                fr_list.append((row['Peso_Pieno'] - row['Peso_Vuoto']) / row['Minuti'])
            else:
                fr_list.append(0.0)

        # Tabella Animali
        st.markdown("##### 2. Dati Animali")
        df_anim = pd.DataFrame({
            "ID_Animale": [""]*num_animali,
            "Siringa": range(1, num_animali+1),
            "SMR_1": [0.0]*num_animali,
            "SMR_2": [0.0]*num_animali,
            "Sex": ["M"]*num_animali,
            "Note": [""]*num_animali
        })
        # Aggiungo FR visivo
        df_anim_view = df_anim.copy()
        df_anim_view["Flow_Rate_Auto"] = fr_list
        
        edited_anim = st.data_editor(df_anim_view, hide_index=True, key="a_edit", column_config={
            "Flow_Rate_Auto": st.column_config.NumberColumn(disabled=True, format="%.4f"),
            "ID_Animale": st.column_config.TextColumn(required=True)
        })

        if st.button("💾 SALVA (Inizia Esperimento)", type="primary"):
            if not proj_name: st.error("Manca Nome Progetto"); st.stop()
            
            ws_db = sh.worksheet("DB_Respirometria")
            rows_to_add = []
            now_date = datetime.now().strftime("%Y-%m-%d")
            
            for i in range(num_animali):
                row_a = edited_anim.iloc[i]
                row_f = edited_falcon.iloc[i]
                fr = fr_list[i]
                
                # Calcolo Watt (Placeholder)
                delta = abs(row_a['SMR_1'] - row_a['SMR_2'])
                watts = (delta * fr * press) / (temp + 273.15) if fr > 0 else 0
                
                new_row = [
                    str(uuid.uuid4()), # ID Univoco
                    proj_name, now_date, st.session_state.username,
                    temp, press, "{}", 
                    row_a['ID_Animale'], row_a['Siringa'], "", "", # Elettrodo/Tubo vuoti per ora
                    set_falcon, f"F_{row_f['Falcon_ID']}", 
                    row_f['Peso_Vuoto'], row_f['Peso_Pieno'], row_f['Minuti'], fr,
                    row_a['SMR_1'], row_a['SMR_2'], delta, watts,
                    row_a['Sex'], row_a['Note'], "", # DW vuoto
                    "APERTO" # STATO
                ]
                rows_to_add.append(new_row)
            
            ws_db.append_rows(rows_to_add)
            st.success("Dati Salvati! Stato: APERTO")
            if 'final_time' in st.session_state: del st.session_state['final_time']
            time.sleep(1)
            st.session_state.choice_made = False # Resetta per permettere resume
            st.rerun()

    # --- MODE: RESUME (Aggiornamento Dati Aperti) ---
    elif st.session_state.work_mode == "RESUME":
        st.subheader("Aggiornamento Esperimenti Aperti")
        
        ws_db = sh.worksheet("DB_Respirometria")
        data = ws_db.get_all_records()
        df = pd.DataFrame(data)
        
        # Filtra solo i miei aperti di oggi
        today_str = datetime.now().strftime("%Y-%m-%d")
        df_open = df[ (df['Operatore'] == st.session_state.username) & 
                      (df['Data'] == today_str) & 
                      (df['Stato'] != 'CHIUSO') ].copy()
        
        if df_open.empty:
            st.success("Nessun esperimento aperto trovato!")
            if st.button("Torna Indietro"):
                st.session_state.work_mode = "NEW"
                st.rerun()
        else:
            # Editor Semplificato
            cols = ["ID_Univoco", "ID_Animale", "SMR_1", "SMR_2", "Note", "Stato"]
            edited_resume = st.data_editor(
                df_open[cols],
                hide_index=True,
                disabled=["ID_Univoco", "ID_Animale"],
                column_config={
                    "Stato": st.column_config.SelectboxColumn(options=["APERTO", "CHIUSO"])
                }
            )
            
            if st.button("💾 AGGIORNA DATI"):
                # Aggiornamento riga per riga
                progress = st.progress(0)
                tot = len(edited_resume)
                for idx, row in edited_resume.iterrows():
                    # Trova la cella dell'ID univoco
                    cell = ws_db.find(str(row['ID_Univoco']))
                    r = cell.row
                    # Aggiorna colonne specifiche (hardcoded per semplicità, attento all'ordine colonne!)
                    # SMR_1=18, SMR_2=19, Note=23, Stato=25 (basato su struttura foglio punto 1)
                    ws_db.update_cell(r, 18, row['SMR_1'])
                    ws_db.update_cell(r, 19, row['SMR_2'])
                    ws_db.update_cell(r, 23, row['Note'])
                    ws_db.update_cell(r, 25, row['Stato'])
                    progress.progress((idx+1)/tot)
                
                st.success("Aggiornamento completato!")
                time.sleep(1)
                st.rerun()

# =============================================================================
# SEZIONE 2: PESI (DAY 3)
# =============================================================================
elif menu == "2. Pesi (Day 3)":
    st.header("Inserimento Dry Weight")
    ws_db = sh.worksheet("DB_Respirometria")
    df = pd.DataFrame(ws_db.get_all_records())
    
    # Filtra righe con DW vuoto
    # gspread può tornare stringa vuota "" o None
    to_update = df[ (df['Dry_Weight'] == "") ].copy()
    
    if to_update.empty:
        st.info("Tutti i pesi sono stati inseriti.")
    else:
        # Selettore Progetto
        projs = sorted(list(set(to_update['Project_Name'].astype(str))))
        sel_proj = st.selectbox("Filtra Progetto", ["Tutti"] + projs)
        if sel_proj != "Tutti":
            to_update = to_update[to_update['Project_Name'] == sel_proj]
            
        edited_dw = st.data_editor(
            to_update[["ID_Univoco", "ID_Animale", "Data", "Dry_Weight"]],
            hide_index=True,
            disabled=["ID_Univoco", "ID_Animale", "Data"],
            column_config={"Dry_Weight": st.column_config.NumberColumn(required=True)}
        )
        
        if st.button("💾 Salva Pesi"):
            prog = st.progress(0)
            cnt = 0
            for i, row in edited_dw.iterrows():
                if row['Dry_Weight'] != "":
                    cell = ws_db.find(str(row['ID_Univoco']))
                    # Colonna 24 è Dry_Weight
                    ws_db.update_cell(cell.row, 24, float(row['Dry_Weight']))
                    cnt += 1
                prog.progress((i+1)/len(edited_dw))
            st.success(f"Aggiornati {cnt} pesi.")
            time.sleep(1)
            st.rerun()

# =============================================================================
# SEZIONE 3: EXPORT
# =============================================================================
elif menu == "3. Export Dati":
    if st.button("🔄 Ricarica"): st.rerun()
    ws_db = sh.worksheet("DB_Respirometria")
    df = pd.DataFrame(ws_db.get_all_records())
    st.dataframe(df)