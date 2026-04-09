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
            return df
    # Fallback CSV
    df = pd.read_csv(FACTURAS_PATH, encoding="utf-8-sig", dtype=str)
    df = normalizar_df(df)
    df = normalizar_estado(df)
    df = df[~es_fila_vacia(df, "factura_id")]
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

    # Conteo y monto por estado
    df_f_stats = df_f.copy()
    df_f_stats["monto_num"] = df_f_stats[COL_MONTO].apply(parse_monto)
    estado_resumen = (
        df_f_stats.groupby("estado_factura")
        .agg(cantidad=("factura_id", "count"), monto_total=("monto_num", "sum"))
        .reset_index()
        .sort_values("cantidad", ascending=False)
    )

    # Mostrar tarjetas por estado
    estados_cols = st.columns(len(estado_resumen) if len(estado_resumen) <= 5 else 5)
    color_estado = {
        "pendiente": "#fb923c",
        "pagada": "#4ade80",
        "anulada": "#94a3b8",
    }
    for i, (_, row_e) in enumerate(estado_resumen.iterrows()):
        est = row_e["estado_factura"]
        cant = int(row_e["cantidad"])
        monto = row_e["monto_total"]
        c_est = color_estado.get(est.lower(), "#38bdf8")
        col_idx = i % len(estados_cols)
        estados_cols[col_idx].markdown(f"""
        <div class='metric-card'>
            <div class='metric-value' style='color:{c_est}; font-size:1.6rem;'>{cant}</div>
            <div class='metric-label'>{est} — {fmt_colones(monto)}</div>
        </div>""", unsafe_allow_html=True)

    # Gráfico de dona con estados
    fig_dona = go.Figure(data=[go.Pie(
        labels=estado_resumen["estado_factura"],
        values=estado_resumen["cantidad"],
        hole=0.5,
        marker_colors=[color_estado.get(e.lower(), "#38bdf8") for e in estado_resumen["estado_factura"]],
        textinfo="label+percent+value",
        textfont_size=13,
    )])
    fig_dona.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#cbd5e1",
        showlegend=False,
        height=300,
        margin=dict(l=20, r=20, t=20, b=20),
    )
    st.plotly_chart(fig_dona, use_container_width=True)

    st.markdown("---")

    # ── Mora ──
    st.subheader("🔴 Mora – Clientes con Facturas Pendientes")

    df_pendientes = df_f[df_f["estado_factura"].str.lower() == "pendiente"].copy()
    df_pendientes["monto_num"] = df_pendientes[COL_MONTO].apply(parse_monto)
    mora = (
        df_pendientes.groupby(["cliente_id", COL_NOMBRE])
        .agg(facturas_pendientes=("factura_id", "count"),
             monto_total=("monto_num", "sum"))
        .reset_index()
        .sort_values("monto_total", ascending=False)
    )

    if not mora.empty:
        mora["monto_total_fmt"] = mora["monto_total"].apply(fmt_colones)
        mora_display = (
            mora[["cliente_id", COL_NOMBRE, "facturas_pendientes", "monto_total_fmt"]]
            .rename(columns={
                "cliente_id": "ID",
                COL_NOMBRE: "Cliente",
                "facturas_pendientes": "Fact. Pendientes",
                "monto_total_fmt": "Monto Total",
            })
            .reset_index(drop=True)
        )

        def highlight_mora(row):
            n = row["Fact. Pendientes"]
            color = "background-color:#7f1d1d; color:white" if int(n) > 1 else ""
            return [color] * len(row)

        st.dataframe(
            mora_display.style.apply(highlight_mora, axis=1),
            use_container_width=True, hide_index=True,
        )
        st.caption("🔴 Rojo = debe más de 1 factura")
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
    filt_estado = fc1.selectbox("Estado", ["Todos", "Pendiente", "Pagada", "Anulada"])
    filt_mes    = fc2.text_input("Mes (YYYY-MM)", placeholder="2026-03")
    filt_nombre = fc3.text_input("Buscar cliente")

    df_view = df_f.copy()
    if filt_estado != "Todos":
        df_view = df_view[df_view["estado_factura"].str.lower() == filt_estado.lower()]
    if filt_mes.strip():
        df_view["_fecha_dt"] = pd.to_datetime(df_view["fecha_factura"], errors="coerce")
        df_view["_mes"] = df_view["_fecha_dt"].dt.to_period("M").astype(str)
        df_view = df_view[df_view["_mes"] == filt_mes.strip()]
        df_view = df_view.drop(columns=["_fecha_dt", "_mes"], errors="ignore")
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
        estados_fac = ["Pendiente", "Pagada", "Anulada"]
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

            save_facturas(df_f)
            st.success(f"✅ Factura {fac_id_sel} actualizada a '{nuevo_estado}'.")
            st.rerun()


# ═════════════════════════════════════════════
#                  CLIENTES
# ═════════════════════════════════════════════
elif pagina == "👤 Clientes":
    st.title("👤 Gestión de Clientes")

    df_c = load_clientes()

    # Filtro
    filt = st.text_input("🔍 Buscar por nombre o ID")
    df_view = df_c.copy()
    if filt.strip():
        mask = (
            df_view[COL_NOMBRE].str.lower().str.contains(filt.lower(), na=False) |
            df_view[COL_ID_CLIENTE].str.lower().str.contains(filt.lower(), na=False)
        )
        df_view = df_view[mask]

    cols_show = [COL_ID_CLIENTE, COL_NOMBRE, COL_TELEFONO, COL_CELULAR,
                 COL_MONTO, COL_ESTADO, COL_VENDEDOR, COL_NODO]
    cols_show = [c for c in cols_show if c in df_view.columns]
    st.dataframe(df_view[cols_show], use_container_width=True, hide_index=True)
    st.caption(f"Total: {len(df_view)} clientes")

    st.markdown("---")
    tab_edit, tab_alta, tab_baja = st.tabs(["✏️ Editar Cliente", "➕ Alta de Cliente", "🚫 Dar de Baja"])

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
        # Generar ID automático
        ids_nums = df_c[COL_ID_CLIENTE].str.extract(r"(\d+)$")[0].dropna()
        ids_nums = pd.to_numeric(ids_nums, errors="coerce").dropna()
        nuevo_id = f"TCS-FTTH-{int(ids_nums.max()) + 1:06d}" if not ids_nums.empty else "TCS-FTTH-000001"

        st.info(f"ID asignado automáticamente: **{nuevo_id}**")

        ac1, ac2 = st.columns(2)
        a_nombre   = ac1.text_input("Nombre Completo *")
        a_cedula   = ac2.text_input("Número de Cédula")
        ac3, ac4 = st.columns(2)
        a_monto    = ac3.text_input("Monto Mensual (sin ₡, ej: 21000) *")
        a_vendedor = ac4.text_input("Vendedor")
        ac5, ac6 = st.columns(2)
        a_tel      = ac5.text_input("Teléfono")
        a_celular  = ac6.text_input("Celular")
        a_dir      = st.text_area("Dirección")
        a_fecha_contrato    = st.date_input("Fecha Contrato", value=date.today())
        a_fecha_instalacion = st.date_input("Fecha Instalación", value=date.today())
        a_fecha_primer_fact = st.date_input("Fecha Primer Factura", value=date.today())

        if st.button("➕ Registrar Cliente", type="primary"):
            if not a_nombre.strip():
                st.error("El Nombre es obligatorio.")
            elif not a_monto.strip() or parse_monto(a_monto) <= 0:
                st.error("El Monto debe ser un número válido mayor a 0.")
            else:
                nueva_fila = {col: "" for col in df_c.columns}
                nueva_fila[COL_ID_CLIENTE]   = nuevo_id
                nueva_fila[COL_NOMBRE]       = a_nombre.strip()
                nueva_fila[COL_CEDULA]       = a_cedula.strip()
                nueva_fila[COL_MONTO]        = a_monto.strip()
                nueva_fila[COL_VENDEDOR]     = a_vendedor.strip()
                nueva_fila[COL_TELEFONO]     = a_tel.strip()
                nueva_fila[COL_CELULAR]      = a_celular.strip()
                nueva_fila["Dirrecion"]      = a_dir.strip()
                nueva_fila["Fecha Contrato"]     = str(a_fecha_contrato)
                nueva_fila["Fecha Instalacion"]  = str(a_fecha_instalacion)
                nueva_fila[COL_FECHA_1FAC]       = str(a_fecha_primer_fact)
                nueva_fila[COL_ESTADO]       = "Activo"
                nueva_fila["Tipo de Red"]    = "FTTH"
                df_c = pd.concat([df_c, pd.DataFrame([nueva_fila])], ignore_index=True)
                save_clientes(df_c)
                st.success(f"✅ Cliente {nuevo_id} – {a_nombre} registrado.")
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
            motivo   = st.selectbox("Motivo", ["DX - No Pago", "DX - Al Dia", "DX", "DX - Cortesia"])
            fecha_desc = st.date_input("Fecha Desconexión", value=date.today())

            if st.button("🚫 Confirmar Baja", type="primary"):
                idx = df_c.index[df_c[COL_ID_CLIENTE] == baja_id][0]
                df_c.at[idx, COL_ESTADO]   = motivo
                df_c.at[idx, COL_FECHA_DX] = str(fecha_desc)
                save_clientes(df_c)
                st.success(f"✅ Cliente {baja_id} dado de baja con estado '{motivo}'.")
                st.rerun()


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
        filt_mes_p = pm1.text_input("Filtrar por mes (YYYY-MM)", placeholder="2026-03")
        filt_cli_p = pm2.text_input("Buscar por cliente")

        df_pv = df_p.copy()
        if filt_mes_p.strip():
            df_pv["_fecha_dt"] = pd.to_datetime(df_pv["fecha_pago"], errors="coerce")
            df_pv["_mes"] = df_pv["_fecha_dt"].dt.to_period("M").astype(str)
            df_pv = df_pv[df_pv["_mes"] == filt_mes_p.strip()]
            df_pv = df_pv.drop(columns=["_fecha_dt", "_mes"], errors="ignore")
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
