import streamlit as st
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Financial Data Platform",
    page_icon="",
    layout="wide"
)


# ============================================================
# CHARGEMENT DES DONNÉES
# ============================================================

GOLD_FILE = "data/gold_dashboard.csv"

df = pd.read_csv(GOLD_FILE)


# ============================================================
# HEADER
# ============================================================

st.title("Real-Time Financial Data Platform")
st.caption("Financial Transactions Analytics — Gold Layer")


# ============================================================
# FILTRE
# ============================================================

transaction_types = ["All"] + sorted(df["type"].unique().tolist())

selected_type = st.selectbox(
    "Transaction type",
    transaction_types
)

if selected_type == "All":
    filtered_df = df.copy()
else:
    filtered_df = df[df["type"] == selected_type]


# ============================================================
# KPIs
# ============================================================

total_transactions = filtered_df["transaction_count"].sum()
total_amount = filtered_df["total_amount"].sum()
average_amount = (
    filtered_df["average_amount"].mean()
    if selected_type != "All"
    else (
        filtered_df["total_amount"].sum()
        / filtered_df["transaction_count"].sum()
    )
)
total_fraud = filtered_df["fraud_count"].sum()

fraud_rate = (
    total_fraud / total_transactions * 100
    if total_transactions > 0
    else 0
)


col1, col2, col3, col4, col5 = st.columns(5)

col1.metric(
    "Transactions",
    f"{total_transactions:,.0f}"
)

col2.metric(
    "Total Amount",
    f"${total_amount:,.2f}"
)

col3.metric(
    "Average Amount",
    f"${average_amount:,.2f}"
)

col4.metric(
    "Fraud Count",
    f"{total_fraud:,.0f}"
)

col5.metric(
    "Fraud Rate",
    f"{fraud_rate:.2f}%"
)


# ============================================================
# CHARTS
# ============================================================

st.divider()

if selected_type == "All":

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Transactions by Type")

        chart_data = filtered_df[
            ["type", "transaction_count"]
        ].set_index("type")

        st.bar_chart(chart_data)

    with col2:
        st.subheader("Total Amount by Type")

        chart_data = filtered_df[
            ["type", "total_amount"]
        ].set_index("type")

        st.bar_chart(chart_data)

else:

    st.subheader(f"Statistics — {selected_type}")

    st.write(
        f"Analysis of {selected_type} transactions."
    )


# ============================================================
# FRAUD ANALYSIS
# ============================================================

st.divider()

st.subheader("Fraud Analysis")

fraud_chart = filtered_df[
    ["type", "fraud_rate"]
].set_index("type")

st.bar_chart(fraud_chart)


# ============================================================
# GOLD DATA
# ============================================================

st.divider()

st.subheader("Gold Layer Data")

st.dataframe(
    filtered_df,
    use_container_width=True,
    hide_index=True
)