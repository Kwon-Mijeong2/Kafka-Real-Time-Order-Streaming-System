import streamlit as st
import pandas as pd
import psycopg2
import time

st.set_page_config(page_title="Order Streaming Dashboard", layout="wide")

st.title("📦 Real-time Order Streaming Dashboard")

DB_CONFIG = {
    "host": "localhost",
    "database": "order_stream",
    "user": "admin",
    "password": "admin123",
    "port": 5432
}


def load_data():
    conn = psycopg2.connect(**DB_CONFIG)

    query = """
    SELECT * FROM orders
    ORDER BY timestamp DESC
    """

    df = pd.read_sql(query, conn)

    conn.close()
    return df


placeholder = st.empty()

while True:
    df = load_data()

    with placeholder.container():

        if not df.empty:
            total_orders = len(df)
            total_revenue = (df["price"] * df["quantity"]).sum()
            avg_order = total_revenue / total_orders

            col1, col2, col3 = st.columns(3)

            col1.metric("Total Orders", total_orders)
            col2.metric("Total Revenue", f"${total_revenue:,.2f}")
            col3.metric("Average Order", f"${avg_order:,.2f}")

            st.subheader("Top Products")

            product_counts = df["product"].value_counts()
            st.bar_chart(product_counts)

            st.subheader("Recent Orders")
            st.dataframe(df.head(20))

        else:
            st.warning("No data available.")

    time.sleep(5)