import pandas as pd
import sqlite3

# Load raw CSV (already encoding-fixed)
df = pd.read_csv(
    "data/raw/sales.csv",
    encoding="latin-1"
)

# Connect to SQLite database
conn = sqlite3.connect("sales.db")

# Store raw data (never modify this)
df.to_sql("raw_sales", conn, if_exists="replace", index=False)

print("Raw data stored safely in database")

clean_query = """
CREATE TABLE clean_sales AS
SELECT
    InvoiceNo,
    StockCode,
    TRIM(Description) AS Description,
    Quantity,
    datetime(InvoiceDate) AS InvoiceDate,
    UnitPrice,
    CustomerID,
    Country,
    Quantity * UnitPrice AS Revenue
FROM raw_sales
WHERE
    Quantity > 0
    AND UnitPrice > 0
    AND InvoiceNo NOT LIKE 'C%';
"""

# Remove old clean table if exists
conn.execute("DROP TABLE IF EXISTS clean_sales;")
conn.execute(clean_query)
conn.commit()

print("Clean sales table created")

clean_df = pd.read_sql_query(
    "SELECT * FROM clean_sales",
    conn
)

print(clean_df.head())
print(clean_df.info())
print(clean_df.shape)

clean_df.to_csv(
    "data/cleaned/sales_cleaned.csv",
    index=False
)

print("Total revenue:", clean_df["Revenue"].sum())
print("Date range:", clean_df["InvoiceDate"].min(), "to", clean_df["InvoiceDate"].max())
print("Top countries:")
print(clean_df["Country"].value_counts().head())