"""
Sistema de Gestión Telcosur CR — v3.0 (Google Sheets + Nube)
Requiere: pip install streamlit pandas plotly reportlab gspread google-auth
Ejecutar: streamlit run app.py

v3.0: Soporte dual Google Sheets (nube) / CSV (local)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, datetime
import os
import re
import io
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import HexColor
from reportlab.pdfgen import canvas as pdf_canvas

# Intentar importar gspread para Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Telcosur CR – Gestión",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENTES_PATH  = os.path.join(DATA_DIR, "clientes.csv")
FACTURAS_PATH  = os.path.join(DATA_DIR, "Facturacion_Proyectada_Telcosur.csv")
PAGOS_PATH     = os.path.join(DATA_DIR, "pagos.csv")

# ─────────────────────────────────────────────
# GOOGLE SHEETS — Conexión
# ─────────────────────────────────────────────
USE_GSHEETS = False
_gs_client = None
_gs_spreadsheet = None

SPREADSHEET_NAME = "Telcosur_DB"

def _get_gsheets_client():
    """Conecta a Google Sheets usando secrets de Streamlit."""
    global _gs_client, _gs_spreadsheet, USE_GSHEETS
    if _gs_client is not None:
        return _gs_client, _gs_spreadsheet
    try:
        if not GSPREAD_AVAILABLE:
            return None, None
        if "gcp_service_account" not in st.secrets:
            return None, None
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        _gs_client = gspread.authorize(creds)
        _gs_spreadsheet = _gs_client.open(SPREADSHEET_NAME)
        USE_GSHEETS = True
        return _gs_client, _gs_spreadsheet
    except Exception as e:
        st.sidebar.warning(f"⚠️ Google Sheets no disponible: {e}")
        return None, None

# Intentar conectar al inicio
_get_gsheets_client()

def _gs_read(sheet_name: str) -> pd.DataFrame:
    """Lee una hoja de Google Sheets y retorna DataFrame."""
    _, ss = _get_gsheets_client()
    if ss is None:
        return pd.DataFrame()
    ws = ss.worksheet(sheet_name)
    data = ws.get_all_records(default_blank="")
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data).astype(str)
    return df

def _gs_write(sheet_name: str, df: pd.DataFrame):
    """Escribe un DataFrame completo a una hoja de Google Sheets."""
    _, ss = _get_gsheets_client()
    if ss is None:
        return
    ws = ss.worksheet(sheet_name)
    ws.clear()
    if df.empty:
        ws.update([df.columns.tolist()], value_input_option="RAW")
        return
    # Convertir todo a string para evitar problemas
    df_str = df.fillna("").astype(str)
    data = [df_str.columns.tolist()] + df_str.values.tolist()
    ws.update(data, value_input_option="RAW")


# ─────────────────────────────────────────────
# GOOGLE DRIVE — Carpetas y Archivos
# ─────────────────────────────────────────────
DRIVE_ROOT_FOLDER_ID = "1UQocuFxpKZZfrnbuKBbwhL8tDBx-NtXz"

def _get_drive_service():
    """Retorna el servicio de Google Drive autenticado."""
    try:
        if "gcp_service_account" not in st.secrets:
            return None
        from googleapiclient.discovery import build
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        return build("drive", "v3", credentials=creds)
    except Exception:
        return None

def _drive_find_folder(service, name, parent_id):
    """Busca una carpeta por nombre dentro de un parent. Retorna ID o None."""
    try:
        query = f"name='{name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = service.files().list(q=query, fields="files(id,name)", pageSize=1).execute()
        files = results.get("files", [])
        return files[0]["id"] if files else None
    except Exception:
        return None

def _drive_create_folder(service, name, parent_id):
    """Crea una carpeta en Drive. Retorna el ID."""
    try:
        metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = service.files().create(body=metadata, fields="id").execute()
        return folder.get("id")
    except Exception:
        return None

def _drive_get_or_create_folder(service, name, parent_id):
    """Busca carpeta, si no existe la crea."""
    folder_id = _drive_find_folder(service, name, parent_id)
    if folder_id:
        return folder_id
    return _drive_create_folder(service, name, parent_id)

def drive_get_client_folder(cliente_id, nombre_cliente, tipo_red):
    """
    Obtiene o crea la estructura de carpetas para un cliente:
    Telcosur_Clientes / {Tipo de Red} / {ID - Nombre} / (Contratos, Comprobantes, Documentos)
    Retorna dict con IDs de las subcarpetas.
    """
    service = _get_drive_service()
    if service is None:
        return None

    # Carpeta tipo de red
    red_folder = _drive_get_or_create_folder(service, tipo_red, DRIVE_ROOT_FOLDER_ID)
    if not red_folder:
        return None

    # Carpeta del cliente
    nombre_limpio = nombre_cliente[:40].strip().replace("/", "-")
    folder_name = f"{cliente_id} - {nombre_limpio}"
    client_folder = _drive_get_or_create_folder(service, folder_name, red_folder)
    if not client_folder:
        return None

    # Subcarpetas
    contratos_id = _drive_get_or_create_folder(service, "Contratos", client_folder)
    comprobantes_id = _drive_get_or_create_folder(service, "Comprobantes", client_folder)
    documentos_id = _drive_get_or_create_folder(service, "Documentos", client_folder)

    return {
        "cliente": client_folder,
        "contratos": contratos_id,
        "comprobantes": comprobantes_id,
        "documentos": documentos_id,
    }

def drive_upload_file(file_data, filename, folder_id, mime_type=None):
    """Sube un archivo a una carpeta de Drive. Retorna el ID del archivo."""
    service = _get_drive_service()
    if service is None:
        return None
    try:
        from googleapiclient.http import MediaIoBaseUpload
        metadata = {"name": filename, "parents": [folder_id]}
        if mime_type is None:
            if filename.lower().endswith(".pdf"):
                mime_type = "application/pdf"
            elif filename.lower().endswith((".jpg", ".jpeg")):
                mime_type = "image/jpeg"
            elif filename.lower().endswith(".png"):
                mime_type = "image/png"
            else:
                mime_type = "application/octet-stream"
        media = MediaIoBaseUpload(io.BytesIO(file_data), mimetype=mime_type)
        uploaded = service.files().create(body=metadata, media_body=media, fields="id,webViewLink").execute()
        return uploaded
    except Exception as e:
        st.error(f"Error subiendo archivo: {e}")
        return None

# Constantes de columnas (nombres limpios, sin espacios trailing)
COL_ID_CLIENTE = "ID Cliente"
COL_NOMBRE     = "Nombre del cliente"
COL_MONTO      = "Monto Mensual de Facturacion"
COL_ESTADO     = "Estado"
COL_TELEFONO   = "Telefono"
COL_CELULAR    = "Celular"
COL_CEDULA     = "Numero de Cedula"
COL_MEGAS      = "Megas"
COL_TV         = "TV"
COL_CORREO     = "Correo"
COL_NODO       = "Nodo"
COL_VENDEDOR   = "Vendedor"
COL_NOTAS      = "Notas"
COL_FECHA_DX   = "Fecha Desconexion"
COL_FECHA_1FAC = "Fecha Primer Factura"

# Estado activo normalizado
ESTADO_ACTIVO = "activo"

# Reglas de vencimiento de facturas
DIAS_EN_COBRO = 7     # Días de gracia para pagar desde emisión
DIAS_PARA_BAJA = 10   # Días desde emisión para generar correo de baja (7+3)


def calcular_estado_factura(row):
    """
    Calcula el estado dinámico de una factura según reglas de negocio:
    - Pagada: ya fue pagada
    - En Cobro: dentro de los 7 días de gracia
    - Vencida: pasaron más de 7 días sin pago
    - Requiere Baja: pasaron más de 10 días sin pago (candidata a desconexión)
    """
    estado = str(row.get("estado_factura", "")).strip().lower()
    if estado == "pagada":
        return "Pagada"
    if estado == "anulada":
        return "Anulada"
    
    fecha_fac = pd.to_datetime(row.get("fecha_factura", ""), errors="coerce")
    if pd.isna(fecha_fac):
        return "Pendiente"
    
    hoy = pd.Timestamp.now().normalize()
    dias_transcurridos = (hoy - fecha_fac).days
    
    if dias_transcurridos <= DIAS_EN_COBRO:
        return "En Cobro"
    elif dias_transcurridos <= DIAS_PARA_BAJA:
        return "Vencida"
    else:
        return "Vencida"  # Más de 10 días


def facturas_requieren_baja(df_f):
    """Retorna facturas que llevan más de DIAS_PARA_BAJA sin pagar."""
    hoy = pd.Timestamp.now().normalize()
    pendientes = df_f[df_f["estado_factura"].str.lower().isin(["pendiente", "en cobro", "vencida"])].copy()
    if pendientes.empty:
        return pd.DataFrame()
    pendientes["_fecha_dt"] = pd.to_datetime(pendientes["fecha_factura"], errors="coerce")
    pendientes["_dias"] = (hoy - pendientes["_fecha_dt"]).dt.days
    return pendientes[pendientes["_dias"] > DIAS_PARA_BAJA]


# ─────────────────────────────────────────────
# HELPERS DE MONTO
# ─────────────────────────────────────────────
def parse_monto(val) -> float:
    """
    Convierte formatos costarricenses de moneda a float.
    Ejemplos:
        '₡21.000'   → 21000.0
        '₡24.500'   → 24500.0
        '59000'      → 59000.0
        '₡1.250.000' → 1250000.0
        '21000'      → 21000.0
        ''           → 0.0
    
    Regla: En formato CR, el punto es separador de miles.
    Solo se trata como decimal si la parte después del último punto
    tiene 1 o 2 dígitos (ej: '21.5' o '21.50').
    """
    if pd.isna(val) or str(val).strip() == "":
        return 0.0
    s = str(val).strip()
    # Quitar símbolo de colón y espacios
    s = re.sub(r"[₡\s]", "", s)
    
    if not s:
        return 0.0
    
    # Si tiene coma Y punto: formato europeo/CR extendido → "1.250.000,50"
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        # Coma como decimal: "21000,50" → "21000.50"
        s = s.replace(",", ".")
    elif "." in s:
        # Solo puntos. Determinar si es separador de miles o decimal.
        # Si la parte después del ÚLTIMO punto tiene exactamente 3 dígitos,
        # es separador de miles (ej: 21.000, 1.250.000)
        parts = s.split(".")
        ultima_parte = parts[-1]
        if len(ultima_parte) == 3:
            # Separador de miles → quitar todos los puntos
            s = s.replace(".", "")
        # Si tiene 1 o 2 dígitos después del punto → es decimal real
        # Si tiene más de 3 → probablemente error, dejamos como está
    
    try:
        return float(s)
    except ValueError:
        return 0.0


def fmt_colones(val) -> str:
    """Formatea número a colones costarricenses: ₡21.000"""
    try:
        return f"₡{int(val):,}".replace(",", ".")
    except (ValueError, TypeError):
        return str(val)


# ─────────────────────────────────────────────
# NORMALIZACIÓN DE DATAFRAMES
# ─────────────────────────────────────────────
def normalizar_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalización estándar para todos los DataFrames cargados:
    1. Strip en nombres de columnas
    2. Strip en todos los valores string
    3. Reemplazar strings 'nan'/'NaN' por ''
    """
    df.columns = df.columns.str.strip()
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()
        # Limpiar 'nan' que quedaron al convertir NaN → str
        df[col] = df[col].replace({"nan": "", "NaN": "", "None": ""})
    return df


def normalizar_estado(df: pd.DataFrame, col_estado: str = COL_ESTADO) -> pd.DataFrame:
    """Normaliza la columna Estado eliminando espacios trailing."""
    if col_estado in df.columns:
        df[col_estado] = df[col_estado].str.strip()
    return df


def es_fila_vacia(df: pd.DataFrame, col_clave: str) -> pd.Series:
    """Retorna máscara de filas donde la columna clave está vacía."""
    return (df[col_clave] == "") | (df[col_clave].isna())


def col_segura(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    """Acceso seguro a columna, retorna default si no existe."""
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


# ─────────────────────────────────────────────
# CARGA / GUARDADO — Dual (Google Sheets o CSV)
# ─────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_clientes() -> pd.DataFrame:
    if USE_GSHEETS:
        df = _gs_read("clientes")
        if not df.empty:
            df = normalizar_df(df)
            df = normalizar_estado(df)
            df = df[~es_fila_vacia(df, COL_ID_CLIENTE)]
            return df
    # Fallback CSV
    df = pd.read_csv(CLIENTES_PATH, sep=";", encoding="utf-8-sig", dtype=str)
    df = normalizar_df(df)
    df = normalizar_estado(df)
    df = df[~es_fila_vacia(df, COL_ID_CLIENTE)]
    return df


@st.cache_data(ttl=30)
def load_facturas() -> pd.DataFrame:
    if USE_GSHEETS:
        df = _gs_read("facturas")
        if not df.empty:
            df = normalizar_df(df)
            df = normalizar_estado(df)
            df = df[~es_fila_vacia(df, "factura_id")]
            # Calcular estado dinámico para facturas no pagadas
            df["estado_factura"] = df.apply(
                lambda r: calcular_estado_factura(r) if r["estado_factura"].lower() not in ("pagada", "anulada") else r["estado_factura"],
                axis=1,
            )
            return df
    # Fallback CSV
    df = pd.read_csv(FACTURAS_PATH, encoding="utf-8-sig", dtype=str)
    df = normalizar_df(df)
    df = normalizar_estado(df)
    df = df[~es_fila_vacia(df, "factura_id")]
    # Calcular estado dinámico
    df["estado_factura"] = df.apply(
        lambda r: calcular_estado_factura(r) if r["estado_factura"].lower() not in ("pagada", "anulada") else r["estado_factura"],
        axis=1,
    )
    return df


@st.cache_data(ttl=30)
def load_pagos() -> pd.DataFrame:
    if USE_GSHEETS:
        df = _gs_read("pagos")
        if not df.empty:
            df = normalizar_df(df)
            if "numero_pago_cliente" not in df.columns:
                df["numero_pago_cliente"] = ""
            return df
    # Fallback CSV
    if not os.path.exists(PAGOS_PATH):
        df = pd.DataFrame(columns=[
            "pago_id", "factura_id", "cliente_id", "nombre_cliente",
            "monto", "fecha_pago", "mes_facturado", "numero_pago_cliente"
        ])
        df.to_csv(PAGOS_PATH, index=False, encoding="utf-8-sig")
        return df
    df = pd.read_csv(PAGOS_PATH, dtype=str, encoding="utf-8-sig")
    df = normalizar_df(df)
    if "numero_pago_cliente" not in df.columns:
        df["numero_pago_cliente"] = ""
    return df


def save_clientes(df: pd.DataFrame):
    if USE_GSHEETS:
        _gs_write("clientes", df)
    else:
        df.to_csv(CLIENTES_PATH, sep=";", index=False, encoding="utf-8-sig")
    st.cache_data.clear()


def save_facturas(df: pd.DataFrame):
    if USE_GSHEETS:
        _gs_write("facturas", df)
    else:
        df.to_csv(FACTURAS_PATH, index=False, encoding="utf-8-sig")
    st.cache_data.clear()


def save_pagos(df: pd.DataFrame):
    if USE_GSHEETS:
        _gs_write("pagos", df)
    else:
        df.to_csv(PAGOS_PATH, index=False, encoding="utf-8-sig")
    st.cache_data.clear()


def next_factura_id(df: pd.DataFrame) -> int:
    if df.empty or "factura_id" not in df.columns:
        return 170000001
    vals = pd.to_numeric(df["factura_id"], errors="coerce").dropna()
    return int(vals.max()) + 1 if not vals.empty else 170000001


def next_pago_id(df: pd.DataFrame) -> int:
    if df.empty or "pago_id" not in df.columns:
        return 180000001
    vals = pd.to_numeric(df["pago_id"], errors="coerce").dropna()
    return int(vals.max()) + 1 if not vals.empty else 180000001


# ─────────────────────────────────────────────
# ESTILOS
# ─────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stSidebar"] { background: #0d1b2a; }
    [data-testid="stSidebar"] * { color: #e0f0ff !important; }
    .metric-card {
        background: #1e2d3e;
        border-radius: 10px;
        padding: 16px 20px;
        text-align: center;
    }
    .metric-value { font-size: 2rem; font-weight: 700; color: #38bdf8; }
    .metric-label { font-size: .85rem; color: #94a3b8; }
    .stDataFrame { width: 100% !important; }
    div[data-testid="stHorizontalBlock"] { gap: 12px; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# SIDEBAR NAV
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📡 Telcosur CR")
    if USE_GSHEETS:
        st.caption("☁️ Conectado a Google Sheets")
    else:
        st.caption("💻 Modo local (CSV)")
    st.markdown("---")
    pagina = st.radio("Navegación", [
        "📊 Dashboard",
        "🗓️ Generar Facturas",
        "📄 Facturas",
        "👤 Clientes",
        "💰 Pagos",
    ])
    st.markdown("---")
    st.caption(f"Hoy: {datetime.now().strftime('%d/%m/%Y')}")


# ─────────────────────────────────────────────
# HELPER: Filtrar activos/inactivos
# ─────────────────────────────────────────────
def filtrar_activos(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna solo clientes con Estado == 'Activo' (normalizado)."""
    return df[df[COL_ESTADO].str.lower().str.strip() == ESTADO_ACTIVO]


def filtrar_inactivos(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna clientes con Estado != 'Activo'."""
    return df[df[COL_ESTADO].str.lower().str.strip() != ESTADO_ACTIVO]


# ─────────────────────────────────────────────
# MIGRACIÓN INICIAL: CSV → Google Sheets
# ─────────────────────────────────────────────
if USE_GSHEETS:
    # Si la hoja de clientes está vacía, migrar desde CSV
    _test = _gs_read("clientes")
    if _test.empty or len(_test) == 0:
        with st.spinner("☁️ Migrando datos a Google Sheets por primera vez..."):
            try:
                # Cargar desde CSV local o de GitHub
                if os.path.exists(CLIENTES_PATH):
                    df_mig_c = pd.read_csv(CLIENTES_PATH, sep=";", encoding="utf-8-sig", dtype=str)
                    df_mig_c = normalizar_df(df_mig_c)
                    df_mig_c = normalizar_estado(df_mig_c)
                    df_mig_c = df_mig_c[~es_fila_vacia(df_mig_c, COL_ID_CLIENTE)]
                    _gs_write("clientes", df_mig_c)
                if os.path.exists(FACTURAS_PATH):
                    df_mig_f = pd.read_csv(FACTURAS_PATH, encoding="utf-8-sig", dtype=str)
                    df_mig_f = normalizar_df(df_mig_f)
                    _gs_write("facturas", df_mig_f)
                if os.path.exists(PAGOS_PATH):
                    df_mig_p = pd.read_csv(PAGOS_PATH, dtype=str, encoding="utf-8-sig")
                    df_mig_p = normalizar_df(df_mig_p)
                    _gs_write("pagos", df_mig_p)
                st.success("✅ Datos migrados a Google Sheets exitosamente.")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error migrando datos: {e}")


# ═════════════════════════════════════════════
#                 DASHBOARD
# ═════════════════════════════════════════════
if pagina == "📊 Dashboard":
    st.title("📊 Dashboard de Control – Telcosur CR")

    df_c = load_clientes()
    df_f = load_facturas()
    df_p = load_pagos()

    # Filtro por tipo de red
    tipos_red = ["Todas"]
    if "Tipo de Red" in df_c.columns:
        tipos_disponibles = sorted(df_c["Tipo de Red"].unique().tolist())
        tipos_disponibles = [t for t in tipos_disponibles if t and t != ""]
        tipos_red += tipos_disponibles
    filtro_red = st.selectbox("🌐 Filtrar por Tipo de Red", tipos_red)

    if filtro_red != "Todas" and "Tipo de Red" in df_c.columns:
        df_c = df_c[df_c["Tipo de Red"] == filtro_red]
        clientes_ids = set(df_c[COL_ID_CLIENTE].tolist())
        df_f = df_f[df_f["cliente_id"].isin(clientes_ids)]
        df_p = df_p[df_p["cliente_id"].isin(clientes_ids)]

    # KPIs principales
    activos   = filtrar_activos(df_c)
    inactivos = filtrar_inactivos(df_c)

    total_proyectado = activos[COL_MONTO].apply(parse_monto).sum()
    total_cobrado    = df_p["monto"].apply(parse_monto).sum() if (not df_p.empty and "monto" in df_p.columns) else 0.0
    pendientes_total = df_f[df_f["estado_factura"].str.lower() == "pendiente"][COL_MONTO].apply(parse_monto).sum()
    perdida_bajas    = inactivos[COL_MONTO].apply(parse_monto).sum()

    c1, c2, c3, c4 = st.columns(4)
    for col, label, val, color in [
        (c1, "Clientes Activos",       str(len(activos)),           "#38bdf8"),
        (c2, "Ingreso Proyectado/mes", fmt_colones(total_proyectado), "#4ade80"),
        (c3, "Total Pendiente",        fmt_colones(pendientes_total), "#fb923c"),
        (c4, "Pérdida por Bajas/mes",  fmt_colones(perdida_bajas),    "#f87171"),
    ]:
        col.markdown(f"""
        <div class='metric-card'>
            <div class='metric-value' style='color:{color}'>{val}</div>
            <div class='metric-label'>{label}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Distribución por Tipo de Red ──
    st.subheader("🌐 Clientes por Tipo de Red")

    if "Tipo de Red" in activos.columns:
        activos_red = activos.copy()
        activos_red["monto_n"] = activos_red[COL_MONTO].apply(parse_monto)
        por_red = (
            activos_red.groupby("Tipo de Red")
            .agg(clientes=(COL_ID_CLIENTE, "count"), ingreso=("monto_n", "sum"))
            .reset_index()
            .sort_values("clientes", ascending=False)
        )
        por_red = por_red[por_red["Tipo de Red"] != ""]

        if not por_red.empty:
            colores_red = {"FTTH": "#38bdf8", "Inalambrico": "#4ade80",
                           "TerraNetwork": "#a78bfa", "Empresarial": "#fb923c"}

            # Tarjetas
            red_cols = st.columns(len(por_red) if len(por_red) <= 4 else 4)
            for i, (_, rr) in enumerate(por_red.iterrows()):
                tipo = rr["Tipo de Red"]
                cli = int(rr["clientes"])
                ing = fmt_colones(rr["ingreso"])
                c_red = colores_red.get(tipo, "#94a3b8")
                red_cols[i % len(red_cols)].markdown(f"""
                <div class='metric-card'>
                    <div class='metric-value' style='color:{c_red}; font-size:1.4rem;'>{tipo}</div>
                    <div class='metric-label'>{cli} clientes</div>
                    <div class='metric-label'>Ingreso: {ing}/mes</div>
                </div>""", unsafe_allow_html=True)

            # Gráfico doble: barras clientes + barras ingreso
            fig_red = go.Figure()
            fig_red.add_bar(
                x=por_red["Tipo de Red"], y=por_red["clientes"],
                name="Clientes", marker_color=[colores_red.get(t, "#94a3b8") for t in por_red["Tipo de Red"]],
                text=por_red["clientes"], textposition="outside",
                yaxis="y",
            )
            fig_red.add_bar(
                x=por_red["Tipo de Red"], y=por_red["ingreso"],
                name="Ingreso Mensual (₡)", marker_color=[colores_red.get(t, "#94a3b8") for t in por_red["Tipo de Red"]],
                text=por_red["ingreso"].apply(fmt_colones), textposition="outside",
                yaxis="y2", opacity=0.5,
            )
            fig_red.update_layout(
                barmode="group",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="#cbd5e1",
                yaxis=dict(title="Clientes", side="left"),
                yaxis2=dict(title="Ingreso (₡)", side="right", overlaying="y"),
                legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=-0.15),
                height=350, margin=dict(l=10, r=10, t=10, b=10),
            )
            st.plotly_chart(fig_red, use_container_width=True)

    st.markdown("---")

    # ── KPI de TV ──
    st.subheader("📺 Clientes con Servicio de TV")

    activos_tv = activos.copy()
    activos_tv["tv_n"] = pd.to_numeric(col_segura(activos_tv, COL_TV), errors="coerce").fillna(0).astype(int)
    con_tv = activos_tv[activos_tv["tv_n"] > 0]
    sin_tv = activos_tv[activos_tv["tv_n"] == 0]

    tv1, tv2, tv3 = st.columns(3)
    tv1.markdown(f"""
    <div class='metric-card'>
        <div class='metric-value' style='color:#a78bfa; font-size:1.8rem;'>{len(con_tv)}</div>
        <div class='metric-label'>Con TV</div>
    </div>""", unsafe_allow_html=True)
    tv2.markdown(f"""
    <div class='metric-card'>
        <div class='metric-value' style='color:#94a3b8; font-size:1.8rem;'>{len(sin_tv)}</div>
        <div class='metric-label'>Sin TV</div>
    </div>""", unsafe_allow_html=True)
    pct_tv = (len(con_tv) / len(activos) * 100) if len(activos) > 0 else 0
    tv3.markdown(f"""
    <div class='metric-card'>
        <div class='metric-value' style='color:#4ade80; font-size:1.8rem;'>{pct_tv:.1f}%</div>
        <div class='metric-label'>Penetración TV</div>
    </div>""", unsafe_allow_html=True)

    # Gráfico dona TV
    if len(activos) > 0:
        fig_tv = go.Figure(data=[go.Pie(
            labels=["Con TV", "Sin TV"],
            values=[len(con_tv), len(sin_tv)],
            hole=0.5,
            marker_colors=["#a78bfa", "#334155"],
            textinfo="label+percent+value",
            textfont_size=13,
        )])
        fig_tv.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#cbd5e1", showlegend=False, height=280,
            margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig_tv, use_container_width=True)

    st.markdown("---")

    # ── Distribución por Paquete (Megas) ──
    st.subheader("📶 Clientes Activos por Paquete")

    activos_megas = activos.copy()
    activos_megas["megas_n"] = pd.to_numeric(col_segura(activos_megas, COL_MEGAS), errors="coerce").fillna(0).astype(int)
    activos_megas["monto_n"] = activos_megas[COL_MONTO].apply(parse_monto)

    paquetes = (
        activos_megas.groupby("megas_n")
        .agg(clientes=(COL_ID_CLIENTE, "count"), monto_prom=("monto_n", "mean"), ingreso_total=("monto_n", "sum"))
        .reset_index()
        .sort_values("megas_n")
    )
    paquetes = paquetes[paquetes["megas_n"] > 0]  # excluir 0 Mbps

    if not paquetes.empty:
        # Tarjetas por paquete
        paq_cols = st.columns(len(paquetes) if len(paquetes) <= 5 else 5)
        colores_paq = ["#38bdf8", "#4ade80", "#a78bfa", "#fb923c", "#f87171"]
        for i, (_, rp) in enumerate(paquetes.iterrows()):
            megas = int(rp["megas_n"])
            cli = int(rp["clientes"])
            precio = fmt_colones(rp["monto_prom"])
            total = fmt_colones(rp["ingreso_total"])
            c_paq = colores_paq[i % len(colores_paq)]
            col_idx = i % len(paq_cols)
            paq_cols[col_idx].markdown(f"""
            <div class='metric-card'>
                <div class='metric-value' style='color:{c_paq}; font-size:1.5rem;'>{megas} Mbps</div>
                <div class='metric-label'>{cli} clientes — {precio}/mes prom.</div>
                <div class='metric-label'>Ingreso: {total}/mes</div>
            </div>""", unsafe_allow_html=True)

        # Gráfico de barras
        fig_paq = go.Figure()
        fig_paq.add_bar(
            x=[f"{int(m)} Mbps" for m in paquetes["megas_n"]],
            y=paquetes["clientes"],
            marker_color=colores_paq[:len(paquetes)],
            text=paquetes["clientes"],
            textposition="outside",
        )
        fig_paq.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#cbd5e1",
            xaxis_title="Paquete",
            yaxis_title="Clientes",
            height=300,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_paq, use_container_width=True)

    st.markdown("---")

    # ── Resumen de Estado de Facturas ──
    st.subheader("🧾 Estado de Facturas")

    mes_actual = datetime.now().strftime("%Y-%m")
    df_f_stats = df_f.copy()
    df_f_stats["monto_num"] = df_f_stats[COL_MONTO].apply(parse_monto)
    df_f_stats["_mes"] = pd.to_datetime(df_f_stats["fecha_factura"], errors="coerce").dt.to_period("M").astype(str)

    # Clasificar con nuevos estados dinámicos
    pagadas_mes = df_f_stats[(df_f_stats["estado_factura"] == "Pagada") & (df_f_stats["_mes"] == mes_actual)]
    en_cobro = df_f_stats[df_f_stats["estado_factura"] == "En Cobro"]
    vencidas = df_f_stats[df_f_stats["estado_factura"] == "Vencida"]
    anuladas = df_f_stats[df_f_stats["estado_factura"] == "Anulada"]

    # Clientes candidatos a baja (>10 días)
    requieren_baja = facturas_requieren_baja(df_f)

    ef1, ef2, ef3, ef4 = st.columns(4)
    ef1.markdown(f"""
    <div class='metric-card'>
        <div class='metric-value' style='color:#4ade80; font-size:1.5rem;'>{len(pagadas_mes)}</div>
        <div class='metric-label'>Pagadas {mes_actual}</div>
        <div class='metric-label'>{fmt_colones(pagadas_mes["monto_num"].sum())}</div>
    </div>""", unsafe_allow_html=True)
    ef2.markdown(f"""
    <div class='metric-card'>
        <div class='metric-value' style='color:#38bdf8; font-size:1.5rem;'>{len(en_cobro)}</div>
        <div class='metric-label'>En Cobro (≤7 días)</div>
        <div class='metric-label'>{fmt_colones(en_cobro["monto_num"].sum())}</div>
    </div>""", unsafe_allow_html=True)
    ef3.markdown(f"""
    <div class='metric-card'>
        <div class='metric-value' style='color:#f87171; font-size:1.5rem;'>{len(vencidas)}</div>
        <div class='metric-label'>Vencidas (&gt;7 días)</div>
        <div class='metric-label'>{fmt_colones(vencidas["monto_num"].sum())}</div>
    </div>""", unsafe_allow_html=True)
    ef4.markdown(f"""
    <div class='metric-card'>
        <div class='metric-value' style='color:#94a3b8; font-size:1.5rem;'>{len(anuladas)}</div>
        <div class='metric-label'>Anuladas</div>
        <div class='metric-label'>{fmt_colones(anuladas["monto_num"].sum())}</div>
    </div>""", unsafe_allow_html=True)

    # Gráfico de dona
    labels_ef = ["Pagadas (mes actual)", "En Cobro", "Vencidas"]
    values_ef = [len(pagadas_mes), len(en_cobro), len(vencidas)]
    colors_ef = ["#4ade80", "#38bdf8", "#f87171"]
    if len(anuladas) > 0:
        labels_ef.append("Anuladas")
        values_ef.append(len(anuladas))
        colors_ef.append("#94a3b8")

    fig_dona = go.Figure(data=[go.Pie(
        labels=labels_ef, values=values_ef, hole=0.5,
        marker_colors=colors_ef,
        textinfo="label+percent+value", textfont_size=12,
    )])
    fig_dona.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color="#cbd5e1", showlegend=False, height=300,
        margin=dict(l=20, r=20, t=20, b=20),
    )
    st.plotly_chart(fig_dona, use_container_width=True)

    # ── Alerta de clientes para baja ──
    if not requieren_baja.empty:
        st.markdown("---")
        st.subheader("⚠️ Clientes Candidatos a Desconexión (+10 días sin pagar)")
        baja_resumen = requieren_baja.groupby(["cliente_id", COL_NOMBRE]).agg(
            facturas_vencidas=("factura_id", "count"),
            monto_total=("Monto Mensual de Facturacion", lambda x: sum(parse_monto(v) for v in x)),
            dias_max=("_dias", "max"),
        ).reset_index().sort_values("dias_max", ascending=False)
        baja_resumen["monto_fmt"] = baja_resumen["monto_total"].apply(fmt_colones)

        st.dataframe(
            baja_resumen[["cliente_id", COL_NOMBRE, "facturas_vencidas", "monto_fmt", "dias_max"]]
            .rename(columns={"cliente_id": "ID", COL_NOMBRE: "Cliente",
                             "facturas_vencidas": "Fact. Vencidas", "monto_fmt": "Monto",
                             "dias_max": "Días sin Pagar"}),
            use_container_width=True, hide_index=True,
        )

        # Generar correo para TI
        with st.expander("📧 Generar Correo de Baja para TI"):
            clientes_baja_list = baja_resumen.apply(
                lambda r: f"• {r['cliente_id']} – {r[COL_NOMBRE]} ({int(r['facturas_vencidas'])} facturas, {r['monto_fmt']}, {int(r['dias_max'])} días)",
                axis=1,
            ).tolist()
            correo_body = f"""Asunto: Solicitud de Desconexión – Clientes con Facturación Vencida

Estimado equipo de TI,

Los siguientes clientes tienen facturas vencidas con más de {DIAS_PARA_BAJA} días sin pagar. Se solicita proceder con la desconexión del servicio según protocolo:

{chr(10).join(clientes_baja_list)}

Total clientes a desconectar: {len(baja_resumen)}
Monto total en mora: {fmt_colones(baja_resumen["monto_total"].sum())}

Fecha de generación: {datetime.now().strftime('%d/%m/%Y %H:%M')}

Saludos,
Departamento de Cobros – Telcosur CR"""

            st.text_area("Correo generado (copiar y enviar)", value=correo_body, height=300)
            st.caption("Copiá este texto y envialo a contacto@telcosur.net")

    st.markdown("---")

    # ── Mora ──
    st.subheader("🔴 Mora – Clientes con Facturas Vencidas")

    df_mora = df_f[df_f["estado_factura"].isin(["Vencida", "En Cobro"])].copy()
    df_mora["monto_num"] = df_mora[COL_MONTO].apply(parse_monto)
    df_mora["_fecha_dt"] = pd.to_datetime(df_mora["fecha_factura"], errors="coerce")
    df_mora["dias_sin_pago"] = (pd.Timestamp.now().normalize() - df_mora["_fecha_dt"]).dt.days

    mora = (
        df_mora.groupby(["cliente_id", COL_NOMBRE])
        .agg(
            facturas_vencidas=("factura_id", "count"),
            monto_total=("monto_num", "sum"),
            dias_max=("dias_sin_pago", "max"),
            en_cobro=("estado_factura", lambda x: (x == "En Cobro").sum()),
            vencidas=("estado_factura", lambda x: (x == "Vencida").sum()),
        )
        .reset_index()
        .sort_values("dias_max", ascending=False)
    )

    if not mora.empty:
        mora["monto_fmt"] = mora["monto_total"].apply(fmt_colones)
        mora["alerta"] = mora.apply(
            lambda r: "🔴 VENCIDA" if r["vencidas"] > 0 else "🟡 EN COBRO", axis=1
        )
        mora_display = (
            mora[["cliente_id", COL_NOMBRE, "en_cobro", "vencidas", "monto_fmt", "dias_max", "alerta"]]
            .rename(columns={
                "cliente_id": "ID",
                COL_NOMBRE: "Cliente",
                "en_cobro": "En Cobro",
                "vencidas": "Vencidas",
                "monto_fmt": "Monto Total",
                "dias_max": "Días sin Pago",
                "alerta": "Estado",
            })
            .reset_index(drop=True)
        )

        def highlight_mora(row):
            if row["Vencidas"] > 0:
                return ["background-color:#7f1d1d; color:white"] * len(row)
            return [""] * len(row)

        st.dataframe(
            mora_display.style.apply(highlight_mora, axis=1),
            use_container_width=True, hide_index=True,
        )
        st.caption("🔴 Rojo = tiene facturas vencidas (+7 días) | 🟡 = en período de cobro (≤7 días)")
    else:
        st.success("¡Sin clientes en mora!")

    st.markdown("---")

    # ── Proyectado vs Cobrado por mes ──
    st.subheader("📈 Ingreso Proyectado vs. Cobrado Real por Mes")

    df_f2 = df_f.copy()
    df_f2["_fecha_dt"] = pd.to_datetime(df_f2["fecha_factura"], errors="coerce")
    df_f2["mes"] = df_f2["_fecha_dt"].dt.to_period("M").astype(str)
    df_f2["monto_num"] = df_f2[COL_MONTO].apply(parse_monto)
    proyectado_mes = df_f2.groupby("mes")["monto_num"].sum().reset_index()
    proyectado_mes.columns = ["mes", "proyectado"]

    if not df_p.empty and "fecha_pago" in df_p.columns:
        df_p2 = df_p.copy()
        df_p2["_fecha_dt"] = pd.to_datetime(df_p2["fecha_pago"], errors="coerce")
        df_p2["mes"] = df_p2["_fecha_dt"].dt.to_period("M").astype(str)
        df_p2["monto_num"] = df_p2["monto"].apply(parse_monto)
        cobrado_mes = df_p2.groupby("mes")["monto_num"].sum().reset_index()
        cobrado_mes.columns = ["mes", "cobrado"]
        chart_df = proyectado_mes.merge(cobrado_mes, on="mes", how="left").fillna(0)
    else:
        chart_df = proyectado_mes.copy()
        chart_df["cobrado"] = 0

    chart_df = chart_df[chart_df["mes"] != "NaT"].sort_values("mes").tail(12)

    fig = go.Figure()
    fig.add_bar(x=chart_df["mes"], y=chart_df["proyectado"], name="Proyectado", marker_color="#38bdf8")
    fig.add_bar(x=chart_df["mes"], y=chart_df["cobrado"],    name="Cobrado",    marker_color="#4ade80")
    fig.update_layout(
        barmode="group",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#cbd5e1",
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        xaxis_title="Mes",
        yaxis_title="Monto (₡)",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Pérdida por Bajas con desglose mensual ──
    st.subheader("📉 Pérdida Económica por Clientes Inactivos")

    # Tabla de clientes inactivos con fecha de desconexión
    inactivos2 = inactivos[[COL_ID_CLIENTE, COL_NOMBRE, COL_ESTADO, COL_MONTO, COL_FECHA_DX]].copy()
    inactivos2["monto_num"] = inactivos2[COL_MONTO].apply(parse_monto)
    inactivos2["Monto Mensual"] = inactivos2["monto_num"].apply(fmt_colones)
    inactivos2["_fecha_dx_dt"] = pd.to_datetime(inactivos2[COL_FECHA_DX], dayfirst=True, errors="coerce")
    inactivos2["Fecha DX"] = inactivos2["_fecha_dx_dt"].dt.strftime("%d/%m/%Y").fillna("Sin fecha")
    inactivos2 = inactivos2.sort_values("_fecha_dx_dt", ascending=False, na_position="last")

    st.dataframe(
        inactivos2[[COL_ID_CLIENTE, COL_NOMBRE, COL_ESTADO, "Monto Mensual", "Fecha DX"]]
        .rename(columns={COL_ID_CLIENTE: "ID", COL_NOMBRE: "Cliente", COL_ESTADO: "Estado"}),
        use_container_width=True, hide_index=True
    )

    # KPIs de pérdida
    pk1, pk2 = st.columns(2)
    pk1.metric("Pérdida Mensual Acumulada", fmt_colones(perdida_bajas))
    pk2.metric("Pérdida Anual Estimada", fmt_colones(perdida_bajas * 12))

    # Gráfico: Pérdida acumulada por mes de desconexión
    inactivos_con_fecha = inactivos2[inactivos2["_fecha_dx_dt"].notna()].copy()
    if not inactivos_con_fecha.empty:
        st.markdown("#### Impacto de Bajas por Mes de Desconexión")

        inactivos_con_fecha["mes_dx"] = inactivos_con_fecha["_fecha_dx_dt"].dt.to_period("M").astype(str)
        perdida_por_mes = (
            inactivos_con_fecha.groupby("mes_dx")
            .agg(
                clientes_baja=("monto_num", "count"),
                perdida_mes=("monto_num", "sum"),
            )
            .reset_index()
            .sort_values("mes_dx")
        )
        # Pérdida acumulada: cada baja sigue afectando todos los meses siguientes
        perdida_por_mes["perdida_acumulada"] = perdida_por_mes["perdida_mes"].cumsum()

        fig_perdida = go.Figure()
        fig_perdida.add_bar(
            x=perdida_por_mes["mes_dx"],
            y=perdida_por_mes["perdida_mes"],
            name="Pérdida Nuevas Bajas",
            marker_color="#f87171",
            text=perdida_por_mes["clientes_baja"].apply(lambda x: f"{int(x)} cli"),
            textposition="outside",
        )
        fig_perdida.add_scatter(
            x=perdida_por_mes["mes_dx"],
            y=perdida_por_mes["perdida_acumulada"],
            name="Pérdida Acumulada Mensual",
            mode="lines+markers",
            line=dict(color="#fbbf24", width=3),
            marker=dict(size=8),
        )
        fig_perdida.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#cbd5e1",
            legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=-0.15),
            xaxis_title="Mes de Desconexión",
            yaxis_title="Monto (₡)",
            yaxis_tickformat=",.0f",
            height=400,
        )
        st.plotly_chart(fig_perdida, use_container_width=True)

        st.caption(
            "Las barras muestran la pérdida mensual por nuevas bajas en ese mes. "
            "La línea muestra la pérdida acumulada (cada baja sigue impactando mes a mes)."
        )


# ═════════════════════════════════════════════
#          GENERAR FACTURAS DEL MES
# ═════════════════════════════════════════════
elif pagina == "🗓️ Generar Facturas":
    st.title("🗓️ Generar Facturas del Mes")

    hoy = date.today()
    col_mes, col_anio = st.columns(2)
    mes_sel  = col_mes.selectbox(
        "Mes", list(range(1, 13)), index=hoy.month - 1,
        format_func=lambda m: date(2000, m, 1).strftime("%B")
    )
    anio_sel = col_anio.number_input("Año", min_value=2024, max_value=2030, value=hoy.year)

    mes_sel = int(mes_sel)
    anio_sel = int(anio_sel)

    fecha_fact = date(anio_sel, mes_sel, 1)
    # Calcular fecha vencimiento (primer día del mes siguiente)
    if mes_sel == 12:
        fecha_venc = date(anio_sel + 1, 1, 1)
    else:
        fecha_venc = date(anio_sel, mes_sel + 1, 1)
    periodo_str = fecha_fact.strftime("%Y-%m")

    st.info(f"Período a generar: **{periodo_str}** | Vencimiento: **{fecha_venc}**")

    if st.button("⚡ Generar Facturas del Mes", type="primary"):
        df_c = load_clientes()
        df_f = load_facturas()

        activos = filtrar_activos(df_c).copy()

        # Facturas ya existentes para ese mes
        df_f["_fecha_dt"] = pd.to_datetime(df_f["fecha_factura"], errors="coerce")
        df_f["_mes_fact"] = df_f["_fecha_dt"].dt.to_period("M").astype(str)
        ya_facturados = set(df_f[df_f["_mes_fact"] == periodo_str]["cliente_id"].tolist())

        nuevas = []
        saltados = []
        errores = []
        next_id = next_factura_id(df_f)

        for _, row in activos.iterrows():
            cid = str(row[COL_ID_CLIENTE]).strip()
            if cid in ya_facturados:
                saltados.append(cid)
                continue

            monto_num = parse_monto(row[COL_MONTO])
            if monto_num <= 0:
                errores.append(f"{cid}: monto no válido ({row[COL_MONTO]})")
                continue

            nueva = {
                "factura_id":        str(next_id),
                "cliente_id":        cid,
                "fecha_factura":     str(fecha_fact),
                "fecha_vencimiento": str(fecha_venc),
                "estado_factura":    "Pendiente",
                COL_FECHA_1FAC:      str(row.get(COL_FECHA_1FAC, "")),
                COL_NOMBRE:          str(row.get(COL_NOMBRE, "")),
                COL_MEGAS:           str(row.get(COL_MEGAS, "")),
                COL_TV:              str(row.get(COL_TV, "")),
                COL_MONTO:           str(int(monto_num)),
                COL_CORREO:          str(row.get(COL_CORREO, "")),
                COL_ESTADO:          str(row.get(COL_ESTADO, "")),
                COL_FECHA_DX:        str(row.get(COL_FECHA_DX, "")),
                "Monto de Primer Factura": "",
            }
            nuevas.append(nueva)
            next_id += 1

        if nuevas:
            df_nuevas = pd.DataFrame(nuevas)
            # Limpiar columnas temporales antes de guardar
            cols_temp = [c for c in df_f.columns if c.startswith("_")]
            df_f = df_f.drop(columns=cols_temp, errors="ignore")
            df_f = pd.concat([df_f, df_nuevas], ignore_index=True)
            save_facturas(df_f)
            st.success(f"✅ Se generaron **{len(nuevas)}** facturas para {periodo_str}.")
        else:
            st.warning("⚠️ No se generaron facturas nuevas.")

        if saltados:
            st.info(f"ℹ️ Se omitieron **{len(saltados)}** clientes (ya tenían factura este mes).")
        if errores:
            with st.expander("❌ Ver errores"):
                for e in errores:
                    st.write(f"- {e}")


# ═════════════════════════════════════════════
#                  FACTURAS
# ═════════════════════════════════════════════
elif pagina == "📄 Facturas":
    st.title("📄 Gestión de Facturas")

    df_f = load_facturas()
    df_p = load_pagos()

    # Filtros
    fc1, fc2, fc3 = st.columns(3)
    filt_estado = fc1.selectbox("Estado", ["Todos", "En Cobro", "Vencida", "Pagada", "Anulada"])
    # Generar lista de meses disponibles
    df_f["_fecha_dt"] = pd.to_datetime(df_f["fecha_factura"], errors="coerce")
    meses_disponibles = sorted(df_f["_fecha_dt"].dt.to_period("M").dropna().astype(str).unique().tolist(), reverse=True)
    filt_mes = fc2.selectbox("Mes", ["Todos"] + meses_disponibles)
    filt_nombre = fc3.text_input("Buscar cliente")

    df_view = df_f.copy()
    if filt_estado != "Todos":
        df_view = df_view[df_view["estado_factura"].str.lower() == filt_estado.lower()]
    if filt_mes != "Todos":
        df_view["_mes_calc"] = pd.to_datetime(df_view["fecha_factura"], errors="coerce").dt.to_period("M").astype(str)
        df_view = df_view[df_view["_mes_calc"] == filt_mes]
        df_view = df_view.drop(columns=["_mes_calc"], errors="ignore")
    if filt_nombre.strip():
        df_view = df_view[df_view[COL_NOMBRE].str.lower().str.contains(filt_nombre.lower(), na=False)]

    cols_mostrar = ["factura_id", "cliente_id", COL_NOMBRE, "fecha_factura",
                    "fecha_vencimiento", "estado_factura", COL_MONTO]
    cols_mostrar = [c for c in cols_mostrar if c in df_view.columns]

    st.dataframe(
        df_view[cols_mostrar].rename(columns={
            "factura_id": "ID Factura",
            "cliente_id": "ID Cliente",
            COL_NOMBRE: "Cliente",
            "fecha_factura": "Fecha",
            "fecha_vencimiento": "Vencimiento",
            "estado_factura": "Estado",
            COL_MONTO: "Monto",
        }),
        use_container_width=True, hide_index=True
    )
    st.caption(f"Mostrando {len(df_view)} de {len(df_f)} facturas")

    st.markdown("---")
    st.subheader("✏️ Editar / Registrar Pago de Factura")

    # Selector con búsqueda
    buscar_fac_ed = st.text_input("Buscar factura por ID o nombre de cliente", key="buscar_fac_edit")
    df_ed = df_f.copy()
    if buscar_fac_ed.strip():
        mask_ed = (
            df_ed["factura_id"].str.contains(buscar_fac_ed, case=False, na=False) |
            df_ed[COL_NOMBRE].str.contains(buscar_fac_ed, case=False, na=False)
        )
        df_ed = df_ed[mask_ed]

    if df_ed.empty:
        st.info("No se encontraron facturas con ese criterio.")
    else:
        opciones_fac = df_ed.apply(
            lambda r: f"{r['factura_id']} | {r[COL_NOMBRE]} | {r['fecha_factura']} | {r['estado_factura']}",
            axis=1
        ).tolist()
        sel_fac = st.selectbox("Seleccionar Factura", opciones_fac)
        fac_id_sel = sel_fac.split(" | ")[0].strip()

        idx = df_f.index[df_f["factura_id"] == fac_id_sel][0]
        row = df_f.loc[idx]

        st.write(
            f"**Cliente:** {row[COL_NOMBRE]} &nbsp;|&nbsp; "
            f"**Fecha:** {row['fecha_factura']} &nbsp;|&nbsp; "
            f"**Estado actual:** {row['estado_factura']}"
        )

        ecol1, ecol2, ecol3 = st.columns(3)
        estados_fac = ["En Cobro", "Vencida", "Pagada", "Anulada"]
        estado_actual = row["estado_factura"].strip()
        idx_estado = estados_fac.index(estado_actual) if estado_actual in estados_fac else 0
        nuevo_estado = ecol1.selectbox("Nuevo Estado", estados_fac, index=idx_estado)
        nuevo_monto  = ecol2.text_input("Monto", value=row[COL_MONTO])
        fecha_pago   = ecol3.date_input("Fecha de Pago", value=date.today())

        # Campo de número de comprobante (visible siempre, requerido si se marca Pagada)
        num_pago_cliente = st.text_input(
            "Número de Pago / Comprobante del Cliente",
            placeholder="Ej: 202603…1234 o número de SINPE",
        )

        # Upload de comprobante
        archivo_comprobante = st.file_uploader(
            "📎 Adjuntar comprobante de pago (opcional)",
            type=["pdf", "jpg", "jpeg", "png"],
            key="comprobante_factura",
        )

        if st.button("💾 Guardar Cambios", type="primary"):
            df_f.at[idx, "estado_factura"] = nuevo_estado
            df_f.at[idx, COL_MONTO] = nuevo_monto.strip()

            # Si se marca como Pagada y antes no lo era → registrar pago
            if nuevo_estado == "Pagada" and estado_actual != "Pagada":
                monto_pago = parse_monto(nuevo_monto)
                fecha_fac_str = row["fecha_factura"]
                fecha_fac_dt = pd.to_datetime(fecha_fac_str, errors="coerce")
                mes_facturado = fecha_fac_dt.strftime("%Y-%m") if pd.notna(fecha_fac_dt) else ""

                nuevo_pago = {
                    "pago_id":              str(next_pago_id(df_p)),
                    "factura_id":           str(fac_id_sel),
                    "cliente_id":           str(row["cliente_id"]),
                    "nombre_cliente":       str(row[COL_NOMBRE]),
                    "monto":                str(monto_pago),
                    "fecha_pago":           str(fecha_pago),
                    "mes_facturado":        mes_facturado,
                    "numero_pago_cliente":  num_pago_cliente.strip(),
                }
                df_p = pd.concat([df_p, pd.DataFrame([nuevo_pago])], ignore_index=True)
                save_pagos(df_p)
                st.success(f"💰 Pago de {fmt_colones(monto_pago)} registrado.")

                # Subir comprobante al Drive si se adjuntó
                if archivo_comprobante is not None:
                    tipo_red = str(row.get("Estado", "FTTH"))
                    # Buscar tipo de red del cliente
                    df_cli_temp = load_clientes()
                    cli_match = df_cli_temp[df_cli_temp[COL_ID_CLIENTE] == str(row["cliente_id"])]
                    tipo_red_cli = cli_match.iloc[0].get("Tipo de Red", "FTTH") if not cli_match.empty else "FTTH"
                    
                    folders = drive_get_client_folder(str(row["cliente_id"]), str(row[COL_NOMBRE]), tipo_red_cli)
                    if folders and folders.get("comprobantes"):
                        ext = archivo_comprobante.name.split(".")[-1]
                        nombre_archivo = f"Comprobante_{mes_facturado}_{num_pago_cliente.strip()[:20]}.{ext}"
                        result = drive_upload_file(archivo_comprobante.read(), nombre_archivo, folders["comprobantes"])
                        if result:
                            st.success(f"📎 Comprobante subido a Drive: {nombre_archivo}")

            save_facturas(df_f)
            st.success(f"✅ Factura {fac_id_sel} actualizada a '{nuevo_estado}'.")
            st.rerun()


# ═════════════════════════════════════════════
#                  CLIENTES
# ═════════════════════════════════════════════
elif pagina == "👤 Clientes":
    st.title("👤 Gestión de Clientes")

    df_c = load_clientes()

    # Filtros
    filt_c1, filt_c2, filt_c3 = st.columns([3, 1, 1])
    filt = filt_c1.text_input("🔍 Buscar por nombre o ID")
    filtro_red_cli = filt_c2.selectbox("Tipo de Red", ["Todas"] + sorted([t for t in df_c.get("Tipo de Red", pd.Series()).unique() if t and t != ""]), key="filt_red_cli")
    filtro_est_cli = filt_c3.selectbox("Estado", ["Todos", "Activo", "DX - Al Dia", "DX - No Pago", "DX - Cortesia", "DX - Cambio Proveedor", "DX - Mudanza", "DX - Problemas Técnicos", "DX - Otro"], key="filt_est_cli")

    df_view = df_c.copy()
    if filt.strip():
        mask = (
            df_view[COL_NOMBRE].str.lower().str.contains(filt.lower(), na=False) |
            df_view[COL_ID_CLIENTE].str.lower().str.contains(filt.lower(), na=False)
        )
        df_view = df_view[mask]
    if filtro_red_cli != "Todas" and "Tipo de Red" in df_view.columns:
        df_view = df_view[df_view["Tipo de Red"] == filtro_red_cli]
    if filtro_est_cli != "Todos":
        df_view = df_view[df_view[COL_ESTADO] == filtro_est_cli]

    cols_show = [COL_ID_CLIENTE, COL_NOMBRE, "Tipo de Red", COL_TELEFONO, COL_CELULAR,
                 COL_MEGAS, COL_TV, COL_MONTO, COL_ESTADO, COL_VENDEDOR, COL_NODO, COL_CORREO]
    cols_show = [c for c in cols_show if c in df_view.columns]
    st.dataframe(df_view[cols_show], use_container_width=True, hide_index=True)
    st.caption(f"Total: {len(df_view)} clientes")

    # Detalle de un cliente
    with st.expander("📋 Ver detalle de un cliente"):
        opciones_det = df_view.apply(lambda r: f"{r[COL_ID_CLIENTE]} – {r[COL_NOMBRE]}", axis=1).tolist()
        if opciones_det:
            sel_det = st.selectbox("Seleccionar cliente", opciones_det, key="detalle_sel")
            det_id = sel_det.split(" – ")[0].strip()
            det_row = df_c[df_c[COL_ID_CLIENTE] == det_id].iloc[0]

            # ── Info personal ──
            st.markdown("#### 👤 Información Personal")
            d1, d2 = st.columns(2)
            d1.markdown(f"""
**ID:** {det_row.get(COL_ID_CLIENTE, '')}  
**Nombre:** {det_row.get(COL_NOMBRE, '')}  
**Cédula:** {det_row.get(COL_CEDULA, '')}  
**Fecha Nacimiento:** {det_row.get('Fecha de nacimiento', '')}  
**Teléfono:** {det_row.get(COL_TELEFONO, '')}  
**Celular:** {det_row.get(COL_CELULAR, '')}  
**Correo:** {det_row.get(COL_CORREO, '')}  
**Dirección:** {det_row.get('Dirrecion', '')}
            """)
            d2.markdown(f"""
**Tipo de Red:** {det_row.get('Tipo de Red', '')}  
**Megas:** {det_row.get(COL_MEGAS, '')} Mbps  
**TV:** {det_row.get(COL_TV, '')}  
**Promo:** {det_row.get('Promo', '')}  
**Monto Mensual:** ₡{det_row.get(COL_MONTO, '')}  
**Estado:** {det_row.get(COL_ESTADO, '')}  
**Vendedor:** {det_row.get(COL_VENDEDOR, '')}  
**Nodo:** {det_row.get(COL_NODO, '')}
            """)

            # ── Info de equipo ──
            st.markdown("#### 📡 Equipo e Infraestructura")
            e1, e2 = st.columns(2)
            e1.markdown(f"""
**Número de Ufinet:** {det_row.get('Numero de Ufinet', '')}  
**Medidor:** {det_row.get('Medidor', '')}  
**SN (Serie):** {det_row.get('SN', '')}  
**Marca ONU:** {det_row.get('Marca', '')}  
**Modelo:** {det_row.get('Modelo', '')}
            """)
            e2.markdown(f"""
**MAC:** {det_row.get('MAC', '')}  
**IP:** {det_row.get('IP', '')}  
**Postes:** {det_row.get('Postes', '')}
            """)

            # ── Fechas ──
            st.markdown("#### 📅 Fechas")
            d3, d4 = st.columns(2)
            d3.markdown(f"""
**Fecha Contrato:** {det_row.get('Fecha Contrato', '')}  
**Fecha Instalación:** {det_row.get('Fecha Instalacion', '')}  
**Fecha 1ra Factura:** {det_row.get(COL_FECHA_1FAC, '')}
            """)
            d4.markdown(f"""
**Fecha Desconexión:** {det_row.get(COL_FECHA_DX, '')}  
**Notas:** {det_row.get(COL_NOTAS, '')}
            """)

            # ── Historial de Facturas ──
            st.markdown("#### 🧾 Historial de Facturas")
            df_f_det = load_facturas()
            fac_cliente = df_f_det[df_f_det["cliente_id"] == det_id].copy()
            if fac_cliente.empty:
                st.info("Sin facturas registradas.")
            else:
                fac_cliente["monto_fmt"] = fac_cliente[COL_MONTO].apply(lambda x: fmt_colones(parse_monto(x)))
                color_map = {"Pagada": "🟢", "En Cobro": "🔵", "Vencida": "🔴", "Anulada": "⚫"}
                fac_cliente["🔘"] = fac_cliente["estado_factura"].map(lambda x: color_map.get(x, "⚪"))
                cols_fac_det = ["🔘", "factura_id", "fecha_factura", "estado_factura", "monto_fmt"]
                cols_fac_det = [c for c in cols_fac_det if c in fac_cliente.columns]
                st.dataframe(
                    fac_cliente[cols_fac_det].rename(columns={
                        "factura_id": "ID Factura", "fecha_factura": "Fecha",
                        "estado_factura": "Estado", "monto_fmt": "Monto",
                    }).sort_values("Fecha", ascending=False),
                    use_container_width=True, hide_index=True,
                )
                # Resumen
                n_pagadas = len(fac_cliente[fac_cliente["estado_factura"] == "Pagada"])
                n_cobro = len(fac_cliente[fac_cliente["estado_factura"] == "En Cobro"])
                n_vencidas = len(fac_cliente[fac_cliente["estado_factura"] == "Vencida"])
                st.caption(f"Total: {len(fac_cliente)} facturas | 🟢 {n_pagadas} pagadas | 🔵 {n_cobro} en cobro | 🔴 {n_vencidas} vencidas")

            # ── Historial de Pagos ──
            st.markdown("#### 💰 Historial de Pagos")
            df_p_det = load_pagos()
            pag_cliente = df_p_det[df_p_det["cliente_id"] == det_id].copy()
            if pag_cliente.empty:
                st.info("Sin pagos registrados.")
            else:
                pag_cliente["monto_fmt"] = pag_cliente["monto"].apply(lambda x: fmt_colones(parse_monto(x)))
                cols_pag_det = ["pago_id", "fecha_pago", "mes_facturado", "monto_fmt", "numero_pago_cliente"]
                cols_pag_det = [c for c in cols_pag_det if c in pag_cliente.columns]
                st.dataframe(
                    pag_cliente[cols_pag_det].rename(columns={
                        "pago_id": "ID Pago", "fecha_pago": "Fecha Pago",
                        "mes_facturado": "Mes", "monto_fmt": "Monto",
                        "numero_pago_cliente": "Comprobante",
                    }).sort_values("Fecha Pago", ascending=False),
                    use_container_width=True, hide_index=True,
                )
                total_pagado = pag_cliente["monto"].apply(parse_monto).sum()
                st.caption(f"Total pagado: {fmt_colones(total_pagado)} en {len(pag_cliente)} pagos")

    st.markdown("---")
    tab_edit, tab_alta, tab_baja, tab_docs = st.tabs(["✏️ Editar Cliente", "➕ Alta de Cliente", "🚫 Dar de Baja", "📂 Documentos"])

    # ── Editar ──
    with tab_edit:
        st.subheader("Editar Cliente Existente")
        cliente_ids = df_c[COL_ID_CLIENTE].tolist()
        sel_id = st.selectbox("Seleccionar ID de Cliente", cliente_ids, key="edit_sel")
        if sel_id:
            idx = df_c.index[df_c[COL_ID_CLIENTE] == sel_id][0]
            row = df_c.loc[idx]
            ec1, ec2 = st.columns(2)
            nuevo_nombre = ec1.text_input("Nombre", value=row.get(COL_NOMBRE, ""))
            nuevo_monto  = ec2.text_input("Monto Mensual", value=row.get(COL_MONTO, ""))
            ec3, ec4 = st.columns(2)
            estados_disponibles = ["Activo", "DX - Al Dia", "DX - No Pago", "DX", "DX - Cortesia"]
            estado_actual = row.get(COL_ESTADO, "Activo").strip()
            idx_estado = estados_disponibles.index(estado_actual) if estado_actual in estados_disponibles else 0
            nuevo_estado = ec3.selectbox("Estado", estados_disponibles, index=idx_estado)
            nueva_nota   = ec4.text_input("Notas", value=row.get(COL_NOTAS, ""))

            if st.button("💾 Guardar Edición", key="btn_edit"):
                df_c.at[idx, COL_NOMBRE]  = nuevo_nombre.strip()
                df_c.at[idx, COL_MONTO]   = nuevo_monto.strip()
                df_c.at[idx, COL_ESTADO]  = nuevo_estado
                df_c.at[idx, COL_NOTAS]   = nueva_nota.strip()
                save_clientes(df_c)
                st.success(f"✅ Cliente {sel_id} actualizado.")
                st.rerun()

    # ── Alta ──
    with tab_alta:
        st.subheader("Registrar Nuevo Cliente")

        # Tipo de Red y ID automático
        ac_r1, ac_r2 = st.columns(2)
        a_tipo_red = ac_r1.selectbox("Tipo de Red *", ["FTTH", "Inalambrico", "TerraNetwork", "Empresarial"])
        
        # Generar ID según tipo de red
        prefijo_red = {"FTTH": "TCS-FTTH", "Inalambrico": "TCS-INAL", "TerraNetwork": "TRN", "Empresarial": "TCS-EMP"}
        prefijo = prefijo_red[a_tipo_red]
        ids_tipo = df_c[df_c[COL_ID_CLIENTE].str.startswith(prefijo)][COL_ID_CLIENTE]
        ids_nums = ids_tipo.str.extract(r"(\d+)$")[0].dropna()
        ids_nums = pd.to_numeric(ids_nums, errors="coerce").dropna()
        nuevo_id = f"{prefijo}-{int(ids_nums.max()) + 1:06d}" if not ids_nums.empty else f"{prefijo}-000001"
        ac_r2.info(f"ID: **{nuevo_id}**")

        ac1, ac2 = st.columns(2)
        a_nombre   = ac1.text_input("Nombre Completo *")
        a_cedula   = ac2.text_input("Número de Cédula")
        ac3, ac4 = st.columns(2)
        montos_comunes = ["21000", "24500", "19000", "20000", "25000", "27000", "27500", "29000", "30000", "35000", "37500", "40000", "59000", "Otro"]
        a_monto_sel = ac3.selectbox("Monto Mensual (₡) *", montos_comunes)
        if a_monto_sel == "Otro":
            a_monto = ac4.text_input("Monto personalizado (₡)")
        else:
            a_monto = a_monto_sel
            ac4.info(f"Monto: **₡{int(a_monto_sel):,}**".replace(",", "."))
        ac5, ac6 = st.columns(2)
        megas_opciones = ["200", "100", "50", "500", "25", "30", "21", "23", "Otro"]
        a_megas = ac5.selectbox("Megas", megas_opciones)
        if a_megas == "Otro":
            a_megas = ac6.text_input("Megas personalizado")
        else:
            a_tv = ac6.selectbox("TV", ["0", "1", "2"])
        ac7, ac8 = st.columns(2)
        vendedores = sorted(set(df_c[COL_VENDEDOR].unique()) - {""})
        vendedores = vendedores + ["Otro"]
        a_vendedor = ac7.selectbox("Vendedor", [""] + vendedores)
        if a_vendedor == "Otro":
            a_vendedor = ac8.text_input("Nombre del vendedor")
        nodos = sorted(set(df_c[COL_NODO].unique()) - {""})
        a_nodo = ac8.selectbox("Nodo", [""] + nodos) if a_vendedor != "Otro" else ""
        ac9, ac10 = st.columns(2)
        a_tel      = ac9.text_input("Teléfono")
        a_celular  = ac10.text_input("Celular")
        a_correo   = st.text_input("Correo electrónico")
        a_dir      = st.text_area("Dirección")
        ac_f1, ac_f2, ac_f3 = st.columns(3)
        a_fecha_contrato    = ac_f1.date_input("Fecha Contrato", value=date.today())
        a_fecha_instalacion = ac_f2.date_input("Fecha Instalación", value=date.today())
        a_fecha_primer_fact = ac_f3.date_input("Fecha Primer Factura", value=date.today())

        st.markdown("**📎 Documentos del cliente (opcional)**")
        doc_c1, doc_c2 = st.columns(2)
        a_contrato = doc_c1.file_uploader("Contrato firmado", type=["pdf", "jpg", "jpeg", "png"], key="alta_contrato")
        a_cedula_doc = doc_c2.file_uploader("Cédula / Identificación", type=["pdf", "jpg", "jpeg", "png"], key="alta_cedula")

        if st.button("➕ Registrar Cliente", type="primary"):
            if not a_nombre.strip():
                st.error("El Nombre es obligatorio.")
            elif not a_monto or parse_monto(a_monto) <= 0:
                st.error("El Monto debe ser un número válido mayor a 0.")
            else:
                nueva_fila = {col: "" for col in df_c.columns}
                nueva_fila[COL_ID_CLIENTE]   = nuevo_id
                nueva_fila[COL_NOMBRE]       = a_nombre.strip()
                nueva_fila[COL_CEDULA]       = a_cedula.strip()
                nueva_fila[COL_MONTO]        = str(int(parse_monto(a_monto)))
                nueva_fila[COL_VENDEDOR]     = a_vendedor.strip() if a_vendedor else ""
                nueva_fila[COL_TELEFONO]     = a_tel.strip()
                nueva_fila[COL_CELULAR]      = a_celular.strip()
                nueva_fila[COL_CORREO]       = a_correo.strip()
                nueva_fila[COL_MEGAS]        = a_megas if a_megas != "Otro" else a_megas
                nueva_fila[COL_TV]           = a_tv if 'a_tv' in dir() else "0"
                nueva_fila["Dirrecion"]      = a_dir.strip()
                nueva_fila["Fecha Contrato"]     = str(a_fecha_contrato)
                nueva_fila["Fecha Instalacion"]  = str(a_fecha_instalacion)
                nueva_fila[COL_FECHA_1FAC]       = str(a_fecha_primer_fact)
                nueva_fila[COL_ESTADO]       = "Activo"
                nueva_fila["Tipo de Red"]    = a_tipo_red
                nueva_fila[COL_NODO]         = a_nodo if 'a_nodo' in dir() else ""
                df_c = pd.concat([df_c, pd.DataFrame([nueva_fila])], ignore_index=True)
                save_clientes(df_c)
                st.success(f"✅ Cliente {nuevo_id} – {a_nombre} registrado como {a_tipo_red}.")

                # Subir documentos al Drive si se adjuntaron
                if a_contrato is not None or a_cedula_doc is not None:
                    folders = drive_get_client_folder(nuevo_id, a_nombre.strip(), a_tipo_red)
                    if folders:
                        if a_contrato is not None:
                            ext = a_contrato.name.split(".")[-1]
                            nombre_contrato = f"Contrato_{date.today().strftime('%Y%m%d')}.{ext}"
                            result = drive_upload_file(a_contrato.read(), nombre_contrato, folders["contratos"])
                            if result:
                                st.success(f"📎 Contrato subido: {nombre_contrato}")
                        if a_cedula_doc is not None:
                            ext = a_cedula_doc.name.split(".")[-1]
                            nombre_ced = f"Cedula_{date.today().strftime('%Y%m%d')}.{ext}"
                            result = drive_upload_file(a_cedula_doc.read(), nombre_ced, folders["documentos"])
                            if result:
                                st.success(f"📎 Cédula subida: {nombre_ced}")

                st.rerun()

    # ── Baja ──
    with tab_baja:
        st.subheader("Dar de Baja a un Cliente")
        # Solo mostrar activos
        activos_baja = filtrar_activos(df_c)
        if activos_baja.empty:
            st.info("No hay clientes activos para dar de baja.")
        else:
            baja_opciones = activos_baja.apply(
                lambda r: f"{r[COL_ID_CLIENTE]} – {r[COL_NOMBRE]}", axis=1
            ).tolist()
            baja_sel = st.selectbox("Cliente Activo", baja_opciones, key="baja_sel")
            baja_id  = baja_sel.split(" – ")[0].strip()
            motivo   = st.selectbox("Motivo", ["DX - No Pago", "DX - Al Dia", "DX - Cortesia", "DX - Cambio Proveedor", "DX - Mudanza", "DX - Problemas Técnicos", "DX - Otro"])
            fecha_desc = st.date_input("Fecha Desconexión", value=date.today())

            if st.button("🚫 Confirmar Baja", type="primary"):
                idx = df_c.index[df_c[COL_ID_CLIENTE] == baja_id][0]
                df_c.at[idx, COL_ESTADO]   = motivo
                df_c.at[idx, COL_FECHA_DX] = str(fecha_desc)
                save_clientes(df_c)
                st.success(f"✅ Cliente {baja_id} dado de baja con estado '{motivo}'.")
                st.rerun()

    # ── Documentos ──
    with tab_docs:
        st.subheader("📂 Documentos del Cliente")

        opciones_docs = df_c.apply(
            lambda r: f"{r[COL_ID_CLIENTE]} – {r[COL_NOMBRE]}", axis=1
        ).tolist()
        sel_docs = st.selectbox("Seleccionar Cliente", opciones_docs, key="docs_sel")
        doc_cid = sel_docs.split(" – ")[0].strip()
        doc_row = df_c[df_c[COL_ID_CLIENTE] == doc_cid].iloc[0]
        doc_nombre = doc_row[COL_NOMBRE]
        doc_tipo_red = doc_row.get("Tipo de Red", "FTTH")

        st.write(f"**{doc_cid}** — {doc_nombre} ({doc_tipo_red})")

        tipo_doc = st.selectbox("Tipo de Documento", [
            "Contrato firmado",
            "Comprobante de pago",
            "Cédula / Identificación",
            "Foto de instalación",
            "Otro documento",
        ], key="tipo_doc_sel")

        # Mapear tipo a subcarpeta
        subcarpeta_map = {
            "Contrato firmado": "contratos",
            "Comprobante de pago": "comprobantes",
            "Cédula / Identificación": "documentos",
            "Foto de instalación": "documentos",
            "Otro documento": "documentos",
        }

        archivo = st.file_uploader(
            "Subir archivo (PDF, imagen JPG/PNG)",
            type=["pdf", "jpg", "jpeg", "png"],
            key="doc_uploader",
        )

        if archivo and st.button("☁️ Subir a Google Drive", type="primary", key="btn_upload_doc"):
            with st.spinner("Creando carpeta y subiendo archivo..."):
                folders = drive_get_client_folder(doc_cid, doc_nombre, doc_tipo_red)
                if folders is None:
                    st.error("No se pudo conectar a Google Drive. Verificá que la cuenta de servicio tenga acceso.")
                else:
                    target_folder = folders[subcarpeta_map[tipo_doc]]
                    # Nombre del archivo: tipo_fecha_original
                    fecha_str = datetime.now().strftime("%Y%m%d")
                    ext = archivo.name.split(".")[-1]
                    nuevo_nombre = f"{tipo_doc.replace(' ', '_').replace('/', '_')}_{fecha_str}.{ext}"
                    
                    result = drive_upload_file(archivo.read(), nuevo_nombre, target_folder)
                    if result:
                        link = result.get("webViewLink", "")
                        st.success(f"✅ Archivo subido: **{nuevo_nombre}**")
                        if link:
                            st.markdown(f"[📄 Ver en Drive]({link})")
                    else:
                        st.error("Error al subir el archivo.")


# ═════════════════════════════════════════════
#                   PAGOS
# ═════════════════════════════════════════════
elif pagina == "💰 Pagos":
    st.title("💰 Registro de Pagos")

    df_p = load_pagos()

    if df_p.empty or len(df_p) == 0:
        st.info("No hay pagos registrados aún. Marcá facturas como 'Pagada' desde la sección Facturas.")
    else:
        # Filtro por mes y cliente
        pm1, pm2 = st.columns(2)
        meses_pago = sorted(df_p["mes_facturado"].dropna().unique().tolist(), reverse=True)
        meses_pago = [m for m in meses_pago if m and m != "" and m != "nan"]
        filt_mes_p = pm1.selectbox("Filtrar por mes", ["Todos"] + meses_pago, key="filt_mes_pagos")
        filt_cli_p = pm2.text_input("Buscar por cliente")

        df_pv = df_p.copy()
        if filt_mes_p != "Todos":
            df_pv = df_pv[df_pv["mes_facturado"] == filt_mes_p]
        if filt_cli_p.strip():
            df_pv = df_pv[df_pv["nombre_cliente"].str.lower().str.contains(filt_cli_p.lower(), na=False)]

        df_pv["monto_fmt"] = df_pv["monto"].apply(lambda x: fmt_colones(parse_monto(x)))

        cols_pagos = ["pago_id", "factura_id", "cliente_id", "nombre_cliente",
                      "monto_fmt", "fecha_pago", "mes_facturado", "numero_pago_cliente"]
        cols_pagos = [c for c in cols_pagos if c in df_pv.columns]

        st.dataframe(
            df_pv[cols_pagos].rename(columns={
                "pago_id": "ID Pago",
                "factura_id": "ID Factura",
                "cliente_id": "ID Cliente",
                "nombre_cliente": "Cliente",
                "monto_fmt": "Monto",
                "fecha_pago": "Fecha Pago",
                "mes_facturado": "Mes Facturado",
                "numero_pago_cliente": "# Comprobante Cliente",
            }),
            use_container_width=True, hide_index=True
        )

        total_cobrado = df_pv["monto"].apply(parse_monto).sum()
        st.metric("Total cobrado (filtro actual)", fmt_colones(total_cobrado))

        st.markdown("---")

        # ── Generar Comprobante PDF ──
        st.subheader("🧾 Generar Comprobante de Pago")

        opciones_pago = df_pv.apply(
            lambda r: f"{r['pago_id']} | {r['nombre_cliente']} | {r['mes_facturado']} | {fmt_colones(parse_monto(r['monto']))}",
            axis=1
        ).tolist()
        sel_pago = st.selectbox("Seleccionar Pago", opciones_pago)
        pago_id_sel = sel_pago.split(" | ")[0].strip()
        row_pago = df_p[df_p["pago_id"] == pago_id_sel].iloc[0]

        if st.button("📄 Generar Comprobante PDF", type="primary"):
            comprobantes_dir = os.path.join(DATA_DIR, "comprobantes")
            os.makedirs(comprobantes_dir, exist_ok=True)
            pdf_path = os.path.join(comprobantes_dir, f"comprobante_{pago_id_sel}.pdf")

            # ── Generar PDF ──
            w, h = letter
            c = pdf_canvas.Canvas(pdf_path, pagesize=letter)

            azul_oscuro = HexColor('#0d1b2a')
            azul_medio  = HexColor('#1e3a5f')
            azul_claro  = HexColor('#38bdf8')
            gris_claro  = HexColor('#94a3b8')
            verde       = HexColor('#4ade80')
            blanco      = HexColor('#ffffff')

            # Header
            c.setFillColor(azul_oscuro)
            c.rect(0, h - 120, w, 120, fill=True, stroke=False)
            c.setFillColor(blanco)
            c.setFont('Helvetica-Bold', 24)
            c.drawString(50, h - 55, 'TELCOSUR CR')
            c.setFont('Helvetica', 11)
            c.setFillColor(azul_claro)
            c.drawString(50, h - 75, 'Servicios de Telecomunicaciones')
            c.setFillColor(verde)
            c.setFont('Helvetica-Bold', 16)
            c.drawRightString(w - 50, h - 55, 'COMPROBANTE DE PAGO')
            c.setFillColor(blanco)
            c.setFont('Helvetica', 11)
            c.drawRightString(w - 50, h - 75, f'N\u00b0 {pago_id_sel}')

            # Línea decorativa
            c.setStrokeColor(azul_claro)
            c.setLineWidth(3)
            c.line(50, h - 135, w - 50, h - 135)

            # Campos
            _y = [h - 170]

            def dibujar_campo(label, valor, x=50):
                c.setFillColor(gris_claro)
                c.setFont('Helvetica', 9)
                c.drawString(x, _y[0] + 12, label)
                c.setFillColor(azul_oscuro)
                c.setFont('Helvetica-Bold', 13)
                c.drawString(x, _y[0] - 4, str(valor))
                _y[0] -= 45

            def dibujar_campo_doble(l1, v1, l2, v2):
                c.setFillColor(gris_claro)
                c.setFont('Helvetica', 9)
                c.drawString(50, _y[0] + 12, l1)
                c.drawString(320, _y[0] + 12, l2)
                c.setFillColor(azul_oscuro)
                c.setFont('Helvetica', 13)
                c.drawString(50, _y[0] - 4, str(v1))
                c.drawString(320, _y[0] - 4, str(v2))
                _y[0] -= 45

            dibujar_campo('CLIENTE', row_pago.get('nombre_cliente', ''))
            dibujar_campo_doble('ID CLIENTE', row_pago.get('cliente_id', ''),
                                'ID FACTURA', row_pago.get('factura_id', ''))
            dibujar_campo_doble('FECHA DE PAGO', row_pago.get('fecha_pago', ''),
                                'MES FACTURADO', row_pago.get('mes_facturado', ''))

            num_comp = row_pago.get('numero_pago_cliente', '')
            if num_comp:
                dibujar_campo('NUMERO DE COMPROBANTE DEL CLIENTE', num_comp)

            # Monto destacado
            y = _y[0] - 10
            c.setFillColor(azul_medio)
            c.roundRect(50, y - 30, w - 100, 70, 10, fill=True, stroke=False)
            c.setFillColor(blanco)
            c.setFont('Helvetica', 11)
            c.drawString(70, y + 22, 'MONTO PAGADO')
            monto_val = int(parse_monto(row_pago.get('monto', '0')))
            c.setFont('Helvetica-Bold', 28)
            c.drawRightString(w - 70, y + 10, fmt_colones(monto_val))

            # Estado
            y -= 60
            c.setFillColor(verde)
            c.setFont('Helvetica-Bold', 14)
            c.drawCentredString(w / 2, y, 'PAGO REGISTRADO')

            # Footer
            c.setFillColor(gris_claro)
            c.setFont('Helvetica', 8)
            c.drawCentredString(w / 2, 50, 'Telcosur CR - Comprobante generado automaticamente')
            c.drawCentredString(w / 2, 38, f'Pago ID: {pago_id_sel} | Factura ID: {row_pago.get("factura_id", "")}')

            c.save()

            # Mostrar descarga
            with open(pdf_path, "rb") as f:
                st.download_button(
                    label="⬇️ Descargar Comprobante PDF",
                    data=f.read(),
                    file_name=f"comprobante_{pago_id_sel}.pdf",
                    mime="application/pdf",
                )
            st.success(f"✅ Comprobante generado: comprobante_{pago_id_sel}.pdf")

        # Gráfico de cobros por mes
        if not df_p.empty:
            df_p2 = df_p.copy()
            df_p2["_fecha_dt"] = pd.to_datetime(df_p2["fecha_pago"], errors="coerce")
            df_p2["mes"] = df_p2["_fecha_dt"].dt.to_period("M").astype(str)
            df_p2["monto_n"] = df_p2["monto"].apply(parse_monto)
            resumen = (
                df_p2[df_p2["mes"] != "NaT"]
                .groupby("mes")["monto_n"].sum()
                .reset_index()
                .sort_values("mes")
                .tail(12)
            )
            if not resumen.empty:
                fig2 = px.bar(
                    resumen, x="mes", y="monto_n",
                    labels={"mes": "Mes", "monto_n": "Cobrado (₡)"},
                    title="Cobros por Mes",
                    color_discrete_sequence=["#4ade80"],
                )
                fig2.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="#cbd5e1",
                )
                st.plotly_chart(fig2, use_container_width=True)

        st.markdown("---")

        # ── Editar / Anular Pagos ──
        st.subheader("✏️ Editar o Anular Pago")

        tab_editar_pago, tab_anular_pago = st.tabs(["✏️ Editar Pago", "🗑️ Anular Pago"])

        with tab_editar_pago:
            opciones_edit = df_p.apply(
                lambda r: f"{r['pago_id']} | {r['nombre_cliente']} | {r['mes_facturado']} | {fmt_colones(parse_monto(r['monto']))}",
                axis=1
            ).tolist()
            sel_edit = st.selectbox("Seleccionar Pago a Editar", opciones_edit, key="edit_pago_sel")
            edit_pago_id = sel_edit.split(" | ")[0].strip()
            idx_edit = df_p.index[df_p["pago_id"] == edit_pago_id][0]
            row_edit = df_p.loc[idx_edit]

            ep1, ep2 = st.columns(2)
            edit_monto = ep1.text_input("Monto (₡)", value=row_edit.get("monto", ""), key="edit_monto")
            edit_fecha = ep2.text_input("Fecha de Pago", value=row_edit.get("fecha_pago", ""), key="edit_fecha")
            ep3, ep4 = st.columns(2)
            edit_comprobante = ep3.text_input("# Comprobante Cliente",
                                              value=row_edit.get("numero_pago_cliente", ""), key="edit_comp")
            edit_mes = ep4.text_input("Mes Facturado", value=row_edit.get("mes_facturado", ""), key="edit_mes")

            if st.button("💾 Guardar Cambios del Pago", key="btn_save_edit_pago"):
                df_p.at[idx_edit, "monto"] = str(int(parse_monto(edit_monto))) if parse_monto(edit_monto) > 0 else edit_monto.strip()
                df_p.at[idx_edit, "fecha_pago"] = edit_fecha.strip()
                df_p.at[idx_edit, "numero_pago_cliente"] = edit_comprobante.strip()
                df_p.at[idx_edit, "mes_facturado"] = edit_mes.strip()
                save_pagos(df_p)
                st.success(f"✅ Pago {edit_pago_id} actualizado.")
                st.rerun()

        with tab_anular_pago:
            st.warning("⚠️ Al anular un pago, la factura asociada vuelve a estado **Pendiente**.")
            opciones_anular = df_p.apply(
                lambda r: f"{r['pago_id']} | {r['nombre_cliente']} | {r['mes_facturado']} | {fmt_colones(parse_monto(r['monto']))}",
                axis=1
            ).tolist()
            sel_anular = st.selectbox("Seleccionar Pago a Anular", opciones_anular, key="anular_pago_sel")
            anular_pago_id = sel_anular.split(" | ")[0].strip()
            row_anular = df_p[df_p["pago_id"] == anular_pago_id].iloc[0]

            st.write(
                f"**Cliente:** {row_anular['nombre_cliente']} &nbsp;|&nbsp; "
                f"**Factura:** {row_anular['factura_id']} &nbsp;|&nbsp; "
                f"**Monto:** {fmt_colones(parse_monto(row_anular['monto']))} &nbsp;|&nbsp; "
                f"**Mes:** {row_anular['mes_facturado']}"
            )

            confirmar = st.checkbox("Confirmo que deseo anular este pago", key="confirmar_anular")
            if st.button("🗑️ Anular Pago", type="primary", key="btn_anular_pago"):
                if not confirmar:
                    st.error("Debés confirmar marcando la casilla antes de anular.")
                else:
                    # Revertir factura a Pendiente
                    df_f = load_facturas()
                    fac_id_anular = row_anular["factura_id"]
                    mask_fac = df_f["factura_id"] == fac_id_anular
                    if mask_fac.any():
                        df_f.loc[mask_fac, "estado_factura"] = "Pendiente"
                        save_facturas(df_f)

                    # Eliminar el pago
                    df_p = df_p[df_p["pago_id"] != anular_pago_id].reset_index(drop=True)
                    save_pagos(df_p)

                    st.success(f"✅ Pago {anular_pago_id} anulado. Factura {fac_id_anular} revertida a Pendiente.")
                    st.rerun()
