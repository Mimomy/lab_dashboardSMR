import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
from datetime import datetime
import time
import uuid
import numpy as np # Necessario per il fix dell'errore JSON

# --- CONFIGURAZIONE ---
SHEET_NAME = "DB_Respirometria"

FALCON_DATASETS = {
    "Set Normal": [9.940, 10.108, 10.002, 9.976, 9.955, 9.967, 9.956, 9.979, 9.936, 9.919, 9.997, 9.934],
    "Set Bold":   [9.974, 9.974, 9.954, 9.924, 9.967, 9.987, 9.948, 9.972, 9.987, 9.980, 9.994, 9.982]
}

# --- CONNESSIONE ---
def get_connection():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)

# --- FIX ERRORE JSON (Sanitizzazione Dati) ---
def clean_for_json(value):
    """Converte tipi NumPy (int64, float64) in tipi Python nativi per evitare errore JSON"""
    if isinstance(value, (np.integer, np.int64)):
        return int(value)
    elif isinstance(value, (np.floating, np.float64)):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif pd.isna(value):
        return "" # Converte NaN in stringa vuota per Google Sheets
    return value

# --- FUNZIONI DI SUPPORTO ---
def check_login(username, password, sh):
    try:
        ws = sh.worksheet("Users")
        users = ws.get_all_records()
        for u in users:
            if str(u['Username']).strip() == str(username).strip() and str(u['Password']).strip() == str(password).strip():
                return u['Nome_Completo']
    except:
        st.error("Errore foglio Users.")
    return None

def get_project_names(sh):
    ws = sh.worksheet("DB_Respirometria")
    try:
        col_vals = ws.col_values(2) 
        if len(col_vals) > 1:
            return sorted(list(set(col_vals[1:])))
    except: pass
    return []

def get_all_unique_tags(sh):
    ws = sh.worksheet("DB_Respirometria")
    try:
        raw_data = ws.col_values(7) # Colonna 7 = JSON Tags
        unique_tags = set()
        for item in raw_data[1:]:
            if item and item != "{}":
                try:
                    js = json.loads(item)
                    unique_tags.update(js.keys())
                except: pass
        return sorted(list(unique_tags))
    except: return []

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
        if len(vals) >= 3: return vals[1], vals[2]
    except: pass
    return None, None

def clear_session_state(username, sh):
    ws = sh.worksheet("Active_Sessions")
    try:
        cell = ws.find(username)
        ws.delete_rows(cell.row)
    except: pass

# --- UI SETUP ---
st.set_page_config(page_title="Lab Manager V3", layout="wide", page_icon="🔬")
st.title("🔬 Respirometria Lab Manager")

if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'username' not in st.session_state: st.session_state.username = ""
if 'active_tags' not in st.session_state: st.session_state.active_tags = []
if 'all_possible_tags' not in st.session_state: st.session_state.all_possible_tags = []

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
                    st.session_state.all_possible_tags = get_all_unique_tags(sh)
                    st.rerun()
                else: st.error("Credenziali Errate")
            except Exception as e: st.error(f"Errore: {e}")
    st.stop()

# --- 2. LOGICA POST-LOGIN ---
try: sh = get_connection()
except: st.error("Connessione persa."); st.stop()

st.sidebar.write(f"Op: **{st.session_state.real_name}**")
if st.sidebar.button("Logout"):
    st.session_state.logged_in = False
    st.rerun()

menu = st.sidebar.radio("Navigazione", ["1. Gestione Esperimenti (Flow/SMR)", "2. Pesi (Day 3)", "3. Export"])

# =============================================================================
# SEZIONE 1: GESTIONE ESPERIMENTI (CREAZIONE + SVOLGIMENTO)
# =============================================================================
if menu == "1. Gestione Esperimenti (Flow/SMR)":
    
    # Check Timer Globale (per ripristino sessione)
    cloud_time, cloud_proj = load_session_state(st.session_state.username, sh)
    if cloud_time and 'timer_start' not in st.session_state:
        st.toast(f"Timer sincronizzato dal cloud: {cloud_time}")
        st.session_state.timer_start = datetime.strptime(cloud_time, "%Y-%m-%d %H:%M:%S")

    tab_new, tab_run = st.tabs(["🆕 Crea Nuovo Set", "▶️ Svolgi / Aggiorna Set"])

    # --- TAB A: CREAZIONE SCHELETRO (SETUP INIZIALE) ---
    with tab_new:
        st.caption("Usa questo tab SOLO per creare la struttura iniziale (Nomi, Parametri). Poi passa al tab 'Svolgi'.")
        
        c1, c2 = st.columns(2)
        with c1:
            projs = get_project_names(sh)
            mode_p = st.radio("Cartella", ["Esistente", "Nuova"], horizontal=True)
            if mode_p == "Esistente" and projs:
                proj_name = st.selectbox("Seleziona Cartella", projs)
            else:
                proj_name = st.text_input("Nome Nuova Cartella", placeholder="Es. Trota_2024")
        
        with c2:
            num_animali = st.number_input("Numero Animali", 1, 30, 5)

        # Parametri e Tag
        st.markdown("##### Parametri Ambientali & Filtri")
        col_env1, col_env2 = st.columns(2)
        with col_env1:
            temp = st.number_input("Temp (°C)", value=20.0)
            press = st.number_input("Pressione (mBar)", value=1013.0)
        
        with col_env2:
            # Gestione Tag (Multiselect + Crea)
            with st.form("add_tag", clear_on_submit=True):
                new_t = st.text_input("Crea Nuovo Parametro", placeholder="Scrivi e Invio")
                if st.form_submit_button("Aggiungi") and new_t:
                    if new_t not in st.session_state.all_possible_tags:
                        st.session_state.all_possible_tags.append(new_t)
                    if new_t not in st.session_state.active_tags:
                        st.session_state.active_tags.append(new_t)
                        st.rerun()

            sel_tags = st.multiselect("Parametri Attivi:", st.session_state.all_possible_tags, default=st.session_state.active_tags)
            st.session_state.active_tags = sel_tags

        # Input Valori Dinamici
        dyn_vals = {}
        if st.session_state.active_tags:
            cols_d = st.columns(4)
            for i, t in enumerate(st.session_state.active_tags):
                with cols_d[i%4]: dyn_vals[t] = st.text_input(t, key=f"create_{t}")
        
        # Nomi Animali (Scheletro)
        st.markdown("##### Definizione Animali")
        df_skeleton = pd.DataFrame({"ID_Animale": [f"Ind_{i+1}" for i in range(num_animali)], "Siringa": range(1, num_animali+1)})
        edited_skel = st.data_editor(df_skeleton, hide_index=True, num_rows="fixed")

        if st.button("💾 CREA STRUTTURA (Salva ed Esci)", type="primary"):
            if not proj_name: st.error("Nome progetto mancante"); st.stop()
            
            ws_db = sh.worksheet("DB_Respirometria")
            rows = []
            now_str = datetime.now().strftime("%Y-%m-%d")
            json_tags = json.dumps(dyn_vals)
            
            for i, row in edited_skel.iterrows():
                # Creiamo la riga vuota pronta per essere riempita dopo
                new_row = [
                    str(uuid.uuid4()), proj_name, now_str, st.session_state.username,
                    clean_for_json(temp), clean_for_json(press), json_tags,
                    str(row['ID_Animale']), int(row['Siringa']), "", "", # Elettrodo/Tubo
                    "", "", 0.0, 0.0, 0.0, 0.0, # Falcon Data (Vuoti)
                    0.0, 0.0, 0.0, 0.0, # SMR Data (Vuoti)
                    "", 0.0, 0.0, "", # Bio Data (Vuoti)
                    "", "SETUP" # DW e Stato
                ]
                rows.append(new_row)
            
            ws_db.append_rows(rows)
            st.success(f"Creati {num_animali} slot in '{proj_name}'. Ora vai al tab 'Svolgi'!")

    # --- TAB B: SVOLGIMENTO (IL CUORE DEL LAVORO) ---
    with tab_run:
        ws_db = sh.worksheet("DB_Respirometria")
        all_data = ws_db.get_all_records()
        df = pd.DataFrame(all_data)

        # 1. Filtro Selezione Esperimento
        my_open = df[ (df['Operatore'] == st.session_state.username) & (df['Stato'] != 'ARCHIVIATO') ].copy()
        
        if my_open.empty:
            st.info("Nessun esperimento attivo. Creane uno nel tab 'Crea Nuovo'.")
        else:
            # Crea una lista leggibile per selezionare
            # Es: "Trota_2024 (Data: 2023-10-27) - SETUP"
            my_open['Label'] = my_open['Project_Name'] + " (" + my_open['Data'] + ") - " + my_open['Stato']
            # Prendiamo gli univoci per la selectbox (per raggruppare gli animali dello stesso set)
            unique_sets = my_open[['Project_Name', 'Data', 'Stato']].drop_duplicates()
            unique_sets['Label'] = unique_sets['Project_Name'] + " (" + unique_sets['Data'] + ") - " + unique_sets['Stato']
            
            selected_label = st.selectbox("Seleziona Esperimento su cui lavorare:", unique_sets['Label'].tolist())
            
            # Filtra il DataFrame solo per quel set
            current_set_info = unique_sets[unique_sets['Label'] == selected_label].iloc[0]
            current_df = my_open[ 
                (my_open['Project_Name'] == current_set_info['Project_Name']) & 
                (my_open['Data'] == current_set_info['Data']) 
            ].copy()
            
            st.divider()
            
            # --- ZONA OPERATIVA ---
            
            # A. FLOW RATE CONTROL
            with st.expander("💧 1. Flow Rate (Timer & Falcon)", expanded=True):
                col_t1, col_t2, col_t3 = st.columns([1,1,2])
                
                # Timer Logic
                current_timer = 0.0
                if 'timer_start' in st.session_state:
                    delta = datetime.now() - st.session_state.timer_start
                    current_timer = delta.total_seconds() / 60
                    col_t3.warning(f"⏱️ IN CORSO: {current_timer:.2f} min")
                    if col_t2.button("⏹️ STOP TIMER"):
                        # Aggiorna il tempo nel dataframe locale per mostrarlo subito
                        current_df['Durata_Min'] = current_timer
                        clear_session_state(st.session_state.username, sh)
                        del st.session_state['timer_start']
                        st.rerun()
                else:
                    # Se il timer è fermo, prendiamo il tempo salvato nel DB (se c'è), altrimenti 0
                    # Usiamo il max per prendere il tempo se è stato salvato
                    saved_time = pd.to_numeric(current_df['Durata_Min']).max()
                    if saved_time > 0: current_timer = saved_time
                    
                    col_t3.info(f"Ultimo Tempo: {current_timer:.2f} min")
                    if col_t1.button("▶️ START TIMER"):
                        now = datetime.now()
                        st.session_state.timer_start = now
                        save_session_state(st.session_state.username, now.strftime("%Y-%m-%d %H:%M:%S"), current_set_info['Project_Name'], sh)
                        st.rerun()

                # Selezione Set Falcon
                c_fal1, c_fal2 = st.columns(2)
                with c_fal1: 
                    # Recupera il set salvato o usa default
                    saved_set = current_df.iloc[0]['Falcon_Set'] if current_df.iloc[0]['Falcon_Set'] else "Set Normal"
                    set_falcon = st.selectbox("Set Falcon", list(FALCON_DATASETS.keys()), index=list(FALCON_DATASETS.keys()).index(saved_set) if saved_set in FALCON_DATASETS else 0)
                
                # Tabella Falcon Editabile
                weights = FALCON_DATASETS[set_falcon]
                needed = len(current_df)
                tara_weights = (weights * (needed // len(weights) + 1))[:needed]
                
                # Prepariamo i dati per l'editor
                df_falcon_edit = current_df[['ID_Univoco', 'ID_Animale', 'Falcon_ID', 'Peso_Pieno']].copy()
                df_falcon_edit['Minuti'] = current_timer # Applica il timer a tutti
                df_falcon_edit['Tara'] = tara_weights # Mostra la tara corretta
                
                edited_falcon = st.data_editor(
                    df_falcon_edit, 
                    hide_index=True,
                    disabled=["ID_Univoco", "ID_Animale", "Tara"],
                    column_config={
                        "Minuti": st.column_config.NumberColumn(format="%.2f"),
                        "Peso_Pieno": st.column_config.NumberColumn(format="%.3f"),
                        "Tara": st.column_config.NumberColumn(format="%.3f"),
                        "Falcon_ID": st.column_config.TextColumn(help="Scrivi F_1, F_2 etc")
                    }
                )

            # B. MISURE CORP & SMR
            with st.expander("📏 2. SMR & Biometria", expanded=True):
                # Prepariamo colonne da editare
                cols_bio = ['ID_Univoco', 'ID_Animale', 'SMR_1', 'SMR_2', 'Sex', 'Body_Length', 'Head_Length', 'Note']
                df_bio_edit = current_df[cols_bio].copy()
                
                edited_bio = st.data_editor(
                    df_bio_edit,
                    hide_index=True,
                    disabled=['ID_Univoco', 'ID_Animale'],
                    column_config={
                        "SMR_1": st.column_config.NumberColumn(format="%.2f"),
                        "SMR_2": st.column_config.NumberColumn(format="%.2f"),
                        "Body_Length": st.column_config.NumberColumn(label="BL (mm)"),
                        "Head_Length": st.column_config.NumberColumn(label="HL (mm)"),
                        "Sex": st.column_config.SelectboxColumn(options=["M", "F", "ND"])
                    }
                )

            # C. SALVATAGGIO UNIVERSALE (UPDATE)
            col_act1, col_act2 = st.columns(2)
            
            if col_act1.button("💾 AGGIORNA DATI (Salva & Esci)", type="primary"):
                progress = st.progress(0)
                tot_rows = len(current_df)
                
                # Mapping per trovare le righe velocemente
                # (gspread update by cell è lento, meglio ricostruire la riga o usare batch, 
                # ma per sicurezza usiamo update_cell mirato sui campi che cambiano)
                
                for i in range(tot_rows):
                    # Dati dalle tabelle editate
                    row_f = edited_falcon.iloc[i]
                    row_b = edited_bio.iloc[i]
                    uid = str(row_f['ID_Univoco'])
                    
                    # Calcolo Flow Rate
                    fr = 0.0
                    try:
                        p_pieno = float(row_f['Peso_Pieno']) if row_f['Peso_Pieno'] else 0.0
                        tara = float(row_f['Tara'])
                        mins = float(row_f['Minuti'])
                        if mins > 0 and p_pieno > tara:
                            fr = (p_pieno - tara) / mins
                    except: pass
                    
                    # Calcolo Watts (semplice placeholder)
                    watts = 0.0
                    try:
                        temp = float(current_df.iloc[0]['Temperatura'])
                        press = float(current_df.iloc[0]['Pressione'])
                        smr1 = float(row_b['SMR_1']) if row_b['SMR_1'] else 0.0
                        smr2 = float(row_b['SMR_2']) if row_b['SMR_2'] else 0.0
                        delta = abs(smr1 - smr2)
                        if fr > 0:
                            watts = (delta * fr * press) / (temp + 273.15)
                    except: pass

                    # TROVA LA RIGA NEL FOGLIO
                    cell = ws_db.find(uid)
                    r = cell.row
                    
                    # AGGIORNA CELLE (Usa numeri colonna fissi basati sull'header v3.0)
                    # 11: Tubo (skip), 12: Falcon_Set, 13: Falcon_ID, 14: Peso_Vuoto, 15: Peso_Pieno, 16: Durata, 17: FR
                    # 18: SMR1, 19: SMR2, 20: Delta, 21: Watts, 22: Sex, 23: BL, 24: HL, 25: Note, 26: DW, 27: Stato
                    
                    # Nota: gspread usa indici base 1.
                    
                    # Aggiorna Falcon Data
                    ws_db.update_cell(r, 12, set_falcon)
                    ws_db.update_cell(r, 13, str(row_f['Falcon_ID'])) # Falcon ID può essere vuoto se non settato
                    ws_db.update_cell(r, 14, clean_for_json(row_f['Tara']))
                    ws_db.update_cell(r, 15, clean_for_json(row_f['Peso_Pieno']))
                    ws_db.update_cell(r, 16, clean_for_json(row_f['Minuti']))
                    ws_db.update_cell(r, 17, clean_for_json(fr))
                    
                    # Aggiorna Bio Data
                    ws_db.update_cell(r, 18, clean_for_json(row_b['SMR_1']))
                    ws_db.update_cell(r, 19, clean_for_json(row_b['SMR_2']))
                    ws_db.update_cell(r, 20, clean_for_json(abs(clean_for_json(row_b['SMR_1']) - clean_for_json(row_b['SMR_2'])))) # Delta
                    ws_db.update_cell(r, 21, clean_for_json(watts))
                    ws_db.update_cell(r, 22, str(row_b['Sex']))
                    ws_db.update_cell(r, 23, clean_for_json(row_b['Body_Length']))
                    ws_db.update_cell(r, 24, clean_for_json(row_b['Head_Length']))
                    ws_db.update_cell(r, 25, str(row_b['Note']))
                    
                    # Aggiorna Stato a "IN_CORSO" se era SETUP
                    ws_db.update_cell(r, 27, "IN_CORSO")

                    progress.progress((i+1)/tot_rows)
                
                st.success("Tutti i dati salvati correttamente!")
                time.sleep(1)
                st.rerun()

            if col_act2.button("✅ ARCHIVIA (Fine Esperimento)"):
                # Mette stato ARCHIVIATO così non appare più qui (ma resta per DW)
                for i, row in current_df.iterrows():
                    cell = ws_db.find(str(row['ID_Univoco']))
                    ws_db.update_cell(cell.row, 27, "ARCHIVIATO")
                st.success("Esperimento Archiviato (Disponibile per pesi Day 3)")
                st.rerun()


# =============================================================================
# SEZIONE 2: PESI (DAY 3)
# =============================================================================
elif menu == "2. Pesi (Day 3)":
    st.header("Inserimento Dry Weight")
    ws_db = sh.worksheet("DB_Respirometria")
    df = pd.DataFrame(ws_db.get_all_records())
    
    if not df.empty:
        # Mostra tutto tranne ciò che ha già il peso
        to_update = df[ pd.to_numeric(df['Dry_Weight'], errors='coerce').isna() ].copy()
        
        if to_update.empty:
            st.info("Nessun peso mancante.")
        else:
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
                    if row['Dry_Weight'] != "" and row['Dry_Weight'] is not None:
                        try:
                            cell = ws_db.find(str(row['ID_Univoco']))
                            # Colonna 26 è Dry_Weight nel nuovo layout
                            ws_db.update_cell(cell.row, 26, float(row['Dry_Weight']))
                            cnt += 1
                        except: pass
                    prog.progress((i+1)/len(edited_dw))
                st.success(f"Aggiornati {cnt} pesi.")
                time.sleep(1)
                st.rerun()
    else:
        st.info("DB vuoto.")

# =============================================================================
# SEZIONE 3: EXPORT
# =============================================================================
elif menu == "3. Export":
    if st.button("🔄 Ricarica"): st.rerun()
    ws_db = sh.worksheet("DB_Respirometria")
    df = pd.DataFrame(ws_db.get_all_records())
    
    if not df.empty and 'Custom_Tags_JSON' in df.columns:
        st.write("Anteprima:")
        tags_list = []
        for x in df['Custom_Tags_JSON']:
            try: tags_list.append(json.loads(x))
            except: tags_list.append({})
        tags_df = pd.json_normalize(tags_list)
        df_final = pd.concat([df.drop('Custom_Tags_JSON', axis=1), tags_df], axis=1)
        st.dataframe(df_final)
    else:
        st.dataframe(df)
