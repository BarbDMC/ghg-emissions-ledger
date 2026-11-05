"""Corporate GHG Scope 1 & 2 emissions dashboard on top of marts.fct_emissions_ledger.

Run: streamlit run esg_dashboard.py

Data source: WAREHOUSE_TARGET (local | supabase), the same setting the DAG uses.
  - supabase: [connections.supabase] in .streamlit/secrets.toml, or the SUPABASE_* keys.
    If it isn't configured, can't be reached or has no ledger yet, the app falls back
    to local and says why in the sidebar.
  - local: [connections.local] in secrets, or the local Docker warehouse defaults.
  - unset: supabase when it's configured, otherwise local.
Settings are read from Streamlit secrets, then env vars, then .env (re-read on every
rerun, so edits to .env apply on the next interaction without a restart).
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import dotenv_values
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError
from streamlit.errors import StreamlitSecretNotFoundError

ENV_FILE = Path(__file__).with_name(".env")

st.set_page_config(
    page_title="Corporate GHG Scope 1 & 2 Emissions Ledger",
    page_icon="🌍",
    layout="wide",
)

OFFICES = {
    "OFF_UIO": "Quito (EC)",
    "OFF_NYC": "New York (US)",
    "OFF_LON": "London (GB)",
    "OFF_FRA": "Frankfurt (DE)",
    "OFF_PAR": "Paris (FR)",
}
SCOPE_COLORS = {"Scope 1": "#E07A1F", "Scope 2": "#2A6FBB"}
SCOPE_HELP = {
    "Scope 1": "Direct emissions from sources the company owns or controls. "
               "Here: diesel burned in the offices' backup generators.",
    "Scope 2": "Indirect emissions from the generation of purchased electricity, "
               "calculated with each country's grid emission factor (location-based).",
}
NUMERIC_COLUMNS = ["activity_amount", "co2e_factor", "co2e_kg", "co2e_tonnes", "cost_amount"]
PLOTLY_CONFIG = {"displaylogo": False, "responsive": True}
CACHE_TTL = 600

HEALTHCHECK_SQL = "select 1 from marts.fct_emissions_ledger limit 1"
SOURCE_BADGES = {
    "supabase": {"icon": ":material/cloud:", "color": "green"},
    "local": {"icon": ":material/computer:", "color": "blue"},
}

FILTER_OPTIONS_SQL = """
    select distinct reporting_year, office_id, ghg_scope
    from marts.fct_emissions_ledger
"""

LEDGER_SQL = """
    select
        invoice_id,
        office_id,
        country_code,
        issue_date,
        reporting_year,
        reporting_month,
        make_date(reporting_year, reporting_month, 1) as reporting_period,
        energy_type,
        ghg_scope,
        activity_amount,
        activity_unit,
        co2e_factor,
        factor_source,
        factor_year,
        co2e_kg,
        co2e_tonnes,
        cost_amount,
        currency
    from marts.fct_emissions_ledger
    where reporting_year = any(:years)
      and office_id = any(:offices)
      and ghg_scope = any(:scopes)
    order by reporting_period, office_id, energy_type
"""


def load_secrets() -> dict:
    try:
        return st.secrets.to_dict()
    except StreamlitSecretNotFoundError:
        return {}


SECRETS = load_secrets()
DOTENV = dotenv_values(ENV_FILE) if ENV_FILE.exists() else {}


def setting(key: str, default=None):
    return SECRETS.get(key) or os.getenv(key) or DOTENV.get(key) or default


def supabase_kwargs() -> dict | None:
    """st.connection kwargs for Supabase, or None when Supabase isn't configured."""
    if "supabase" in SECRETS.get("connections", {}):
        return {}
    if not setting("SUPABASE_HOST"):
        return None
    return {
        "dialect": "postgresql",
        "driver": "psycopg2",
        "host": setting("SUPABASE_HOST"),
        "port": int(setting("SUPABASE_PORT", 5432)),
        "database": setting("SUPABASE_DB", "postgres"),
        "username": setting("SUPABASE_USER"),
        "password": setting("SUPABASE_PASSWORD"),
        "query": {"sslmode": setting("SUPABASE_SSLMODE", "require")},
    }


def local_kwargs() -> dict:
    if "local" in SECRETS.get("connections", {}):
        return {}
    return {
        "dialect": "postgresql",
        "driver": "psycopg2",
        "host": "localhost",
        "port": int(setting("POSTGRES_HOST_PORT", 5432)),
        "database": "warehouse",
        "username": "postgres",
        "password": "postgres",
    }


def connect_warehouse():
    """Connect to WAREHOUSE_TARGET; fall back to the local warehouse if Supabase isn't usable."""
    kwargs = supabase_kwargs()
    target = str(setting("WAREHOUSE_TARGET") or ("supabase" if kwargs is not None else "local"))
    target = target.strip().lower()
    if target not in ("local", "supabase"):
        st.sidebar.warning(f"Unknown WAREHOUSE_TARGET {target!r} (use local or supabase). "
                           "Showing the local warehouse.")
    elif target == "supabase" and kwargs is None:
        st.sidebar.warning("WAREHOUSE_TARGET is supabase, but SUPABASE_HOST, SUPABASE_USER and "
                           "SUPABASE_PASSWORD aren't set. Showing the local warehouse.")
    elif target == "supabase":
        try:
            conn = st.connection("supabase", type="sql", connect_args={"connect_timeout": 5},
                                 **kwargs)
            conn.query(HEALTHCHECK_SQL, ttl=CACHE_TTL)
            return conn, "supabase"
        except ProgrammingError:
            st.sidebar.warning("Supabase has no emissions ledger yet: trigger the DAG with "
                               "target = supabase. Showing the local warehouse.")
        except SQLAlchemyError as exc:
            reason = str(getattr(exc, "orig", exc)).strip().splitlines()[0]
            st.sidebar.warning(f"Can't reach Supabase ({reason}). Showing the local warehouse.")
    return st.connection("local", type="sql", **local_kwargs()), "local"


def office_label(office_id: str) -> str:
    return OFFICES.get(office_id, office_id)


def load_filter_options(conn) -> pd.DataFrame:
    return conn.query(FILTER_OPTIONS_SQL, ttl=CACHE_TTL)


def load_ledger(conn, years: list[int], offices: list[str], scopes: list[str]) -> pd.DataFrame:
    df = conn.query(LEDGER_SQL, params={"years": years, "offices": offices, "scopes": scopes},
                    ttl=CACHE_TTL)
    df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].astype(float)
    df["reporting_period"] = pd.to_datetime(df["reporting_period"])
    df["office"] = df["office_id"].map(office_label)
    return df


def scope_totals(df: pd.DataFrame) -> dict[str, float]:
    totals = df.groupby("ghg_scope")["co2e_tonnes"].sum()
    return {scope: float(totals.get(scope, 0.0)) for scope in SCOPE_COLORS}


def grid_intensity(df: pd.DataFrame) -> pd.DataFrame:
    """Effective electricity intensity per office (kg CO2e / kWh)."""
    electricity = df[df["energy_type"] == "electricity"]
    result = (electricity.groupby("office", as_index=False)
              .agg(kwh=("activity_amount", "sum"), co2e_kg=("co2e_kg", "sum"),
                   factor_source=("factor_source", "first")))
    result["kg_co2e_per_kwh"] = result["co2e_kg"] / result["kwh"]
    return result.sort_values("kg_co2e_per_kwh")


def render_sidebar(options: pd.DataFrame) -> tuple[list[int], list[str], list[str]]:
    st.sidebar.header("Filters")
    years = sorted(int(y) for y in options["reporting_year"].unique())
    year_choice = st.sidebar.selectbox("Reporting year", ["All", *years])
    office_ids = sorted(options["office_id"].unique(), key=office_label)
    offices = st.sidebar.multiselect("Office / Country", office_ids, default=office_ids,
                                     format_func=office_label)
    scope_options = sorted(options["ghg_scope"].unique())
    scopes = st.sidebar.multiselect("GHG scope", scope_options, default=scope_options,
                                    help="\n\n".join(f"**{s}:** {SCOPE_HELP[s]}"
                                                     for s in scope_options if s in SCOPE_HELP))
    st.sidebar.caption("Source table: marts.fct_emissions_ledger (dbt). "
                       "Year is based on the consumption start date.")
    return (years if year_choice == "All" else [year_choice]), offices, scopes


def render_kpis(df: pd.DataFrame) -> None:
    totals = scope_totals(df)
    total = sum(totals.values())
    share = {scope: (value / total * 100 if total else 0.0) for scope, value in totals.items()}

    col_total, col_s1, col_s2, col_cost = st.columns(4)
    col_total.metric("Total emissions", f"{total:,.2f} t CO₂e", f"{len(df)} invoices",
                     delta_color="off")
    col_s1.metric("Scope 1 · direct combustion (diesel)", f"{totals['Scope 1']:,.2f} t CO₂e",
                  f"{share['Scope 1']:.1f}% of total", delta_color="off",
                  help=SCOPE_HELP["Scope 1"])
    col_s2.metric("Scope 2 · purchased electricity", f"{totals['Scope 2']:,.2f} t CO₂e",
                  f"{share['Scope 2']:.1f}% of total", delta_color="off",
                  help=SCOPE_HELP["Scope 2"])

    costs = df.groupby("currency")["cost_amount"].sum().sort_index()
    with col_cost:
        st.markdown("<p style='font-size:0.875rem;margin-bottom:0.25rem'>"
                    "Total energy cost</p>", unsafe_allow_html=True)
        st.markdown("  \n".join(f"**{currency} {amount:,.0f}**"
                                for currency, amount in costs.items()) or "—")
        st.caption("Per invoice currency, no FX conversion.")


def render_charts(df: pd.DataFrame) -> None:
    left, right = st.columns(2)

    by_office = df.groupby(["office", "ghg_scope"], as_index=False)["co2e_tonnes"].sum()
    fig_office = px.bar(
        by_office, x="office", y="co2e_tonnes", color="ghg_scope", barmode="stack",
        color_discrete_map=SCOPE_COLORS, title="Emissions by office and scope",
        labels={"office": "", "co2e_tonnes": "t CO₂e", "ghg_scope": "Scope"})
    left.plotly_chart(fig_office, config=PLOTLY_CONFIG)

    monthly = (df.groupby(["reporting_period", "ghg_scope"], as_index=False)["co2e_tonnes"]
               .sum().sort_values("reporting_period"))
    fig_monthly = px.area(
        monthly, x="reporting_period", y="co2e_tonnes", color="ghg_scope",
        color_discrete_map=SCOPE_COLORS, markers=True, title="Monthly emissions trend",
        labels={"reporting_period": "", "co2e_tonnes": "t CO₂e", "ghg_scope": "Scope"})
    fig_monthly.update_xaxes(dtick="M1", tickformat="%b %Y")
    right.plotly_chart(fig_monthly, config=PLOTLY_CONFIG)

    st.caption(f":orange[**■ Scope 1**] — {SCOPE_HELP['Scope 1']}  \n"
               f":blue[**■ Scope 2**] — {SCOPE_HELP['Scope 2']}")

    intensity = grid_intensity(df)
    if intensity.empty:
        st.info("Select Scope 2 to compare electricity grid intensity.")
        return
    fig_grid = px.bar(
        intensity, x="kg_co2e_per_kwh", y="office", orientation="h", color="kg_co2e_per_kwh",
        color_continuous_scale="RdYlGn_r", text="kg_co2e_per_kwh",
        hover_data={"factor_source": True, "kwh": ":,.0f", "office": False},
        title="Electricity grid intensity (kg CO₂e / kWh)",
        labels={"kg_co2e_per_kwh": "kg CO₂e / kWh", "office": "", "kwh": "kWh",
                "factor_source": "Factor source"})
    fig_grid.update_traces(texttemplate="%{text:.3f}", textposition="outside")
    fig_grid.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig_grid, config=PLOTLY_CONFIG)

    cleanest, dirtiest = intensity.iloc[0], intensity.iloc[-1]
    if len(intensity) > 1 and cleanest["kg_co2e_per_kwh"] > 0:
        ratio = dirtiest["kg_co2e_per_kwh"] / cleanest["kg_co2e_per_kwh"]
        st.caption(f"Each kWh consumed in {dirtiest['office']} emits {ratio:.1f}× more CO₂e "
                   f"than in {cleanest['office']}.")


def render_audit_table(df: pd.DataFrame) -> None:
    st.subheader("Ledger audit")
    audit = df[["invoice_id", "office", "reporting_period", "energy_type", "ghg_scope",
                "activity_amount", "activity_unit", "co2e_factor", "factor_source",
                "co2e_kg", "co2e_tonnes"]]
    st.dataframe(
        audit, width="stretch", hide_index=True,
        column_config={
            "invoice_id": "Invoice ID",
            "office": "Office",
            "reporting_period": st.column_config.DateColumn("Period", format="MMM YYYY"),
            "energy_type": "Energy type",
            "ghg_scope": "Scope",
            "activity_amount": st.column_config.NumberColumn("Consumption", format="%.2f"),
            "activity_unit": "Unit",
            "co2e_factor": st.column_config.NumberColumn("Factor (kg CO₂e/unit)", format="%.6f"),
            "factor_source": "Factor source",
            "co2e_kg": st.column_config.NumberColumn("kg CO₂e", format="%.4f"),
            "co2e_tonnes": st.column_config.NumberColumn("t CO₂e", format="%.4f"),
        })
    st.download_button("Download CSV", audit.to_csv(index=False).encode("utf-8"),
                       file_name="emissions_ledger.csv", mime="text/csv")


def main() -> None:
    st.title("Corporate GHG Scope 1 & 2 Emissions Ledger")
    conn, source = connect_warehouse()
    st.badge(f"Data source: {source}", icon=SOURCE_BADGES[source]["icon"],
             color=SOURCE_BADGES[source]["color"])
    st.caption("Invoice-level carbon accounting · GHG Protocol, location-based method")

    years, offices, scopes = render_sidebar(load_filter_options(conn))
    if not (years and offices and scopes):
        st.warning("Select at least one office and one scope.")
        st.stop()

    df = load_ledger(conn, years, offices, scopes)
    if df.empty:
        st.info("No invoices match the selected filters.")
        st.stop()

    render_kpis(df)
    st.divider()
    render_charts(df)
    st.divider()
    render_audit_table(df)


main()
