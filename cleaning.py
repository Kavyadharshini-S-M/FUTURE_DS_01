import sqlite3
import pandas as pd

conn = sqlite3.connect("sales.db")
df = pd.read_csv("data/raw/data.csv")
df.to_sql("raw_sales", conn, if_exists="replace", index=False)