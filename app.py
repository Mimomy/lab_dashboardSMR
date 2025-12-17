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
        col_vals = ws.col_values(2) # Colonna B è Project_Name
        if len(col_vals) > 1:
            return sorted(list(set(col_vals[1:])))
    except:
        pass
    return []

def get_all_unique_tags(sh):
    """Recupera TUTTI i tag mai usati in tutto il database per suggerirli"""
    ws = sh.worksheet("DB_Respirometria")
    try:
        # Colonna G (7) è Custom_Tags_JSON
        raw_data = ws.col_values(7)
        unique_tags = set()
        for item in raw_data[1:]: # Salta header
            if item and item != "{}":
                try:
                    js = json.loads(item)
                    unique_tags.update(js.keys())
                except: pass
        return sorted(list(unique_tags))
    except:
        return []

def get_tags_for_specific_project(sh, project_name):
    """Recupera i tag usati SOLO in un progetto specifico"""
    ws = sh.worksheet("DB_Respirometria")
    try:
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        if df.empty: return []
        subset = df[df['Project_Name'] == project_name]
        unique_tags = set()
        for val in subset['Custom_Tags_JSON']:
            if val and val != "{}":
                try:
                    data_json = json.loads(val)
                    unique_tags.update(data_json.keys())
                except: pass
        return list(unique_tags)
    except:
        return []

def check_todays_data_for_project(sh, project_name, username):
    ws = sh.worksheet("DB_Respirometria")
    try:
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        if df.empty: return False
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        subset = df[ 
            (df['Operatore'] == username) & 
            (df['Project_Name'] == project_name) &
            (df['Data'] == today_str) &
            (df['Stato'] != 'CHIUSO')
        ]
        return not subset.empty
    except:
        return False

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
        if len(vals) >= 3:
            return vals[1], vals[2]
    except:
        pass
    return None, None

def clear_session_state(username, sh):
    ws = sh.worksheet("Active_Sessions")
    try:
        cell = ws.find(username)
        ws.delete_rows(cell.row)
    except:
        pass

# --- UI SETUP ---
st.set_page_config(page_title="Lab Dashboard V2.1", layout="wide", page_icon="🔬")
st.title("🔬 Respirometria Lab")

if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'username' not in st.session_state: st.session_state.username = ""
if 'choice_made' not in st.session_state: st.session_state.choice_made = False
if 'work_mode' not in st.session_state: st.session_state.work_mode = "NEW"
if 'active_tags' not in st.session_state: st.session_state.active_tags = []
if 'selected_project_memory' not in st.session_state: st.session_state.selected_project_memory = ""
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
                    # Pre-carica tutti i tag possibili all'avvio per velocità
                    st.session_state.all_possible_tags = get_all_unique_tags(sh)
                    st.rerun()
                else:
                    st.error("Credenziali Errate")
            except Exception as e:
                st.error(f"Errore: {e}")
    st.stop()

# --- 2. LOGICA POST-LOGIN ---
try:
    sh = get_connection()
except:
    st.error("Connessione persa.")
    st.stop()

st.sidebar.write(f"Operatore: **{st.session_state.real_name}**")
if st.sidebar.button("Logout"):
    st.session_state.logged_in = False
    st.session_state.choice_made = False
    st.rerun()

menu = st.sidebar.radio("Menu", ["1. Misurazione (Day 0)", "2. Pesi (Day 3)", "3. Export Dati"])

# =============================================================================
# SEZIONE 1: MISURAZIONE (DAY 0)
# =============================================================================
if menu == "1. Misurazione (Day 0)":
    
    # Check Iniziale Timer
    if not st.session_state.choice_made:
        cloud_time, cloud_proj = load_session_state(st.session_state.username, sh)
        if cloud_time:
            st.info(f"⏱️ **Timer Attivo!** Stavi lavorando su '{cloud_proj}' dalle {cloud_time}.")
            st.session_state.timer_start = datetime.strptime(cloud_time, "%Y-%m-%d %H:%M:%S")
            st.session_state.work_mode = "NEW" 
            st.session_state.choice_made = True
            st.rerun()
        else:
            st.session_state.work_mode = "NEW"
            st.session_state.choice_made = True

    # --- MODALITÀ NUOVO SET ---
    if st.session_state.work_mode == "NEW":
        st.subheader("Setup Esperimento")
        
        # A. Progetto
        c_p1, c_p2 = st.columns(2)
        with c_p1:
            projs = get_project_names(sh)
            mode_p = st.radio("Cartella / Progetto", ["Esistente", "Nuova"], horizontal=True)
            
            proj_name = ""
            if mode_p == "Esistente":
                if projs:
                    proj_name = st.selectbox("Seleziona Cartella:", projs)
                    
                    if proj_name != st.session_state.selected_project_memory:
                        tags = get_tags_for_specific_project(sh, proj_name)
                        st.session_state.active_tags = tags # Imposta i default per questo progetto
                        st.session_state.selected_project_memory = proj_name
                        
                        if check_todays_data_for_project(sh, proj_name, st.session_state.username):
                            st.session_state.alert_today = True
                        else:
                            st.session_state.alert_today = False
                else:
                    st.warning("Crea una nuova cartella.")
            else:
                proj_name = st.text_input("Nome Nuova Cartella", placeholder="Es. Trota_2024")
                # Non resettiamo active_tags qui, lasciamo che l'utente scelga

        # Avviso Dati Oggi
        if st.session_state.get('alert_today', False) and mode_p == "Esistente":
            st.warning(f"⚠️ Ci sono dati aperti di **OGGI** in **{proj_name}**.")
            col_warn1, col_warn2 = st.columns(2)
            if col_warn1.button("📂 APRI QUELLI DI OGGI", use_container_width=True, type="primary"):
                st.session_state.work_mode = "RESUME"
                st.rerun()
            if col_warn2.button("➕ NO, NUOVI ANIMALI", use_container_width=True):
                st.session_state.alert_today = False
                st.rerun()

        with c_p2:
            num_animali = st.number_input("N. Animali", 1, 30, 5)

        st.markdown("---")
        
        # B. Timer
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
                with col_t3: st.success(f"Tempo: {current_time_val:.2f} min")
            if col_t1.button("▶️ START TIMER"):
                now = datetime.now()
                st.session_state.timer_start = now
                save_session_state(st.session_state.username, now.strftime("%Y-%m-%d %H:%M:%S"), proj_name or "Nuovo", sh)
                if 'final_time' in st.session_state: del st.session_state['final_time']
                st.rerun()

        # C. Parametri Ambientali (Con Multi-Select)
        st.markdown("##### 🌡️ Parametri")
        c_fix1, c_fix2 = st.columns(2)
        with c_fix1: temp = st.number_input("Temp (°C)", value=20.0)
        with c_fix2: press = st.number_input("Pressione (mBar)", value=1013.0)
        
        st.write("---")
        st.write("###### 🏷️ Filtri / Categorie Extra")
        
        # GESTIONE FILTRI DINAMICA (MULTISELECT)
        # Permette di aggiungere, rimuovere e selezionare da lista esistente
        selected_tags = st.multiselect(
            "Aggiungi o Rimuovi Parametri:",
            options=st.session_state.all_possible_tags, # Suggerisce tutti quelli usati nel DB
            default=st.session_state.active_tags,       # Pre-seleziona quelli del progetto
            placeholder="Seleziona o scrivi e premi Invio..."
        )
        
        # Aggiorniamo lo stato con la selezione corrente
        st.session_state.active_tags = selected_tags
        
        # Renderizza Input Dinamici
        dynamic_values = {}
        if st.session_state.active_tags:
            cols_dyn = st.columns(4)
            for i, tag in enumerate(st.session_state.active_tags):
                with cols_dyn[i % 4]:
                    val = st.text_input(f"{tag}", key=f"dyn_{tag}")
                    dynamic_values[tag] = val

        st.markdown("---")

        # D. Tabelle
        c_set, _ = st.columns(2)
        with c_set:
            set_falcon = st.selectbox("Set Falcon", list(FALCON_DATASETS.keys()))
        
        # Tabella Falcon
        st.markdown("##### 🧪 1. Pesi Falcon")
        weights = FALCON_DATASETS[set_falcon]
        needed = num_animali
        current_weights = []
        while len(current_weights) < needed: current_weights.extend(weights)
        current_weights = current_weights[:needed]
        
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
        
        fr_list = []
        for i, row in edited_falcon.iterrows():
            if row['Minuti'] > 0 and row['Peso_Pieno'] > row['Peso_Vuoto']:
                fr_list.append((row['Peso_Pieno'] - row['Peso_Vuoto']) / row['Minuti'])
            else:
                fr_list.append(0.0)

        # Tabella Animali
        st.markdown("##### 🐟 2. Dati Animali")
        df_anim = pd.DataFrame({
            "ID_Animale": [""]*num_animali,
            "Siringa": range(1, num_animali+1),
            "SMR_1": [0.0]*num_animali,
            "SMR_2": [0.0]*num_animali,
            "Sex": ["M"]*num_animali,
            "Note": [""]*num_animali
        })
        df_anim_view = df_anim.copy()
        df_anim_view.insert(2, "Flow_Rate", fr_list)
        
        edited_anim = st.data_editor(df_anim_view, hide_index=True, key="a_edit", column_config={
            "Flow_Rate": st.column_config.NumberColumn(disabled=True, format="%.4f"),
            "ID_Animale": st.column_config.TextColumn(required=True)
        })

        if st.button("💾 SALVA (Inizia Esperimento)", type="primary"):
            if not proj_name: st.error("Manca Nome Progetto"); st.stop()
            if edited_anim['ID_Animale'].eq("").any(): st.error("Mancano ID Animali"); st.stop()
            
            ws_db = sh.worksheet("DB_Respirometria")
            rows_to_add = []
            now_date = datetime.now().strftime("%Y-%m-%d")
            json_tags = json.dumps(dynamic_values)
            
            for i in range(num_animali):
                row_a = edited_anim.iloc[i]
                row_f = edited_falcon.iloc[i]
                fr = fr_list[i]
                delta = abs(row_a['SMR_1'] - row_a['SMR_2'])
                watts = (delta * fr * press) / (temp + 273.15) if fr > 0 else 0
                
                new_row = [
                    str(uuid.uuid4()), proj_name, now_date, st.session_state.username,
                    temp, press, json_tags,
                    row_a['ID_Animale'], row_a['Siringa'], "", "",
                    set_falcon, f"F_{row_f['Falcon_ID']}", 
                    row_f['Peso_Vuoto'], row_f['Peso_Pieno'], row_f['Minuti'], fr,
                    row_a['SMR_1'], row_a['SMR_2'], delta, watts,
                    row_a['Sex'], row_a['Note'], "", "APERTO"
                ]
                rows_to_add.append(new_row)
            
            ws_db.append_rows(rows_to_add)
            st.success("Dati Salvati!")
            if 'final_time' in st.session_state: del st.session_state['final_time']
            time.sleep(1)
            st.session_state.choice_made = False 
            st.rerun()

    # --- MODE: RESUME ---
    elif st.session_state.work_mode == "RESUME":
        st.subheader("📝 Aggiornamento Esperimenti Aperti")
        
        ws_db = sh.worksheet("DB_Respirometria")
        data = ws_db.get_all_records()
        df = pd.DataFrame(data)
        
        df_open = df[ (df['Operatore'] == st.session_state.username) & (df['Stato'] != 'CHIUSO') ].copy()
        
        if df_open.empty:
            st.success("Tutto chiuso!")
            if st.button("Torna al Menu"):
                st.session_state.work_mode = "NEW"
                st.rerun()
        else:
            progetti_aperti = df_open['Project_Name'].unique()
            st.info(f"Progetti Aperti: {', '.join(progetti_aperti)}")

            cols = ["ID_Univoco", "ID_Animale", "SMR_1", "SMR_2", "Note", "Stato"]
            edited_resume = st.data_editor(
                df_open[cols],
                hide_index=True,
                disabled=["ID_Univoco", "ID_Animale"],
                column_config={
                    "SMR_1": st.column_config.NumberColumn(format="%.2f"),
                    "SMR_2": st.column_config.NumberColumn(format="%.2f"),
                    "Stato": st.column_config.SelectboxColumn(options=["APERTO", "CHIUSO"])
                }
            )
            
            c_s1, c_s2 = st.columns(2)
            if c_s1.button("💾 AGGIORNA PARZIALE"):
                prog = st.progress(0)
                tot = len(edited_resume)
                for idx, row in edited_resume.iterrows():
                    cell = ws_db.find(str(row['ID_Univoco']))
                    r = cell.row
                    ws_db.update_cell(r, 18, row['SMR_1'])
                    ws_db.update_cell(r, 19, row['SMR_2'])
                    ws_db.update_cell(r, 23, row['Note'])
                    ws_db.update_cell(r, 25, row['Stato'])
                    prog.progress((idx+1)/tot)
                st.success("Aggiornato!")
                time.sleep(1)
                st.rerun()
                
            if c_s2.button("✅ CHIUDI TUTTO", type="primary"):
                prog = st.progress(0)
                tot = len(edited_resume)
                for idx, row in edited_resume.iterrows():
                    cell = ws_db.find(str(row['ID_Univoco']))
                    r = cell.row
                    ws_db.update_cell(r, 18, row['SMR_1'])
                    ws_db.update_cell(r, 19, row['SMR_2'])
                    ws_db.update_cell(r, 23, row['Note'])
                    ws_db.update_cell(r, 25, "CHIUSO")
                    prog.progress((idx+1)/tot)
                st.balloons()
                st.success("Chiuso!")
                time.sleep(1.5)
                st.session_state.work_mode = "NEW"
                st.rerun()

# =============================================================================
# SEZIONE 2: PESI
# =============================================================================
elif menu == "2. Pesi (Day 3)":
    st.header("Inserimento Dry Weight")
    ws_db = sh.worksheet("DB_Respirometria")
    df = pd.DataFrame(ws_db.get_all_records())
    
    if not df.empty:
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
                            ws_db.update_cell(cell.row, 24, float(row['Dry_Weight']))
                            cnt += 1
                        except: pass
                    prog.progress((i+1)/len(edited_dw))
                st.success(f"Fatto ({cnt} pesi).")
                time.sleep(1)
                st.rerun()
    else:
        st.info("DB vuoto.")

# =============================================================================
# SEZIONE 3: EXPORT
# =============================================================================
elif menu == "3. Export Dati":
    if st.button("🔄 Ricarica"): st.rerun()
    ws_db = sh.worksheet("DB_Respirometria")
    df = pd.DataFrame(ws_db.get_all_records())
    
    if not df.empty and 'Custom_Tags_JSON' in df.columns:
        st.write("Anteprima (Tags Espansi):")
        tags_list = []
        for x in df['Custom_Tags_JSON']:
            try: tags_list.append(json.loads(x))
            except: tags_list.append({})
        tags_df = pd.json_normalize(tags_list)
        df_final = pd.concat([df.drop('Custom_Tags_JSON', axis=1), tags_df], axis=1)
        st.dataframe(df_final)
    else:
        st.dataframe(df)
