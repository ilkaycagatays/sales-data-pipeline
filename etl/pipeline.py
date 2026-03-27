import pandas as pd
import sqlite3
import os

def extract(filepath):
    """Ham veriyi CSV'den oku"""
    print("📥 Extracting data...")
    df = pd.read_csv(filepath)
    print(f"   {len(df)} rows loaded.")
    return df

def transform(df):
    """Veriyi temizle ve dönüştür"""
    print("🔄 Transforming data...")

    # 1. Tarih sütununu datetime'a çevir
    df['date'] = pd.to_datetime(df['date'])

    # 2. Yeni sütunlar ekle
    df['total_price'] = df['quantity'] * df['unit_price']
    df['month'] = df['date'].dt.to_period('M').astype(str)
    df['year'] = df['date'].dt.year

    # 3. Eksik değer kontrolü
    missing = df.isnull().sum().sum()
    print(f"   Missing values: {missing}")

    # 4. Duplicate kontrolü
    dupes = df.duplicated().sum()
    print(f"   Duplicate rows: {dupes}")
    df = df.drop_duplicates()

    print(f"   Transform complete. {len(df)} clean rows.")
    return df

def load(df, db_path):
    """Temiz veriyi SQLite database'e yaz"""
    print("💾 Loading data to database...")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)

    df.to_sql('sales', conn, if_exists='replace', index=False)

    conn.close()
    print(f"   Data loaded to {db_path}")

def run_pipeline():
    """ETL pipeline'ı çalıştır"""
    print("=" * 45)
    print("🚀 SALES DATA PIPELINE STARTED")
    print("=" * 45)

    df_raw = extract('data/raw_sales.csv')
    df_clean = transform(df_raw)
    load(df_clean, 'database/sales.db')

    print("=" * 45)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 45)
    return df_clean

if __name__ == "__main__":
    run_pipeline()