import sqlite3
import pandas as pd

# Veritabanı dosyasının adı
DB_NAME = "quanfina.db"

def get_connection():
    """Veritabanına bağlanmayı sağlar."""
    return sqlite3.connect(DB_NAME)

def init_db():
    """Uygulama ilk çalıştığında gerekli tabloları oluşturur."""
    conn = get_connection()
    cursor = conn.cursor()

    # 1. Pozisyonlar Tablosu (Yeni Pozisyon ve Pozisyonlar sayfaları için)
    # Tharp risk hesaplamalarını ve işlem detaylarını burada tutacağız
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            strategy TEXT,
            entry_date TEXT,
            entry_price REAL,
            stop_loss REAL,
            quantity INTEGER,
            risk_amount REAL,
            r_multiple REAL,
            status TEXT DEFAULT 'Open', -- 'Open' veya 'Closed'
            exit_date TEXT,
            exit_price REAL,
            profit_loss REAL
        )
    ''')

    # 2. Trade Journal Tablosu (5 ana bölüm için)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            category TEXT, -- 'Daily', 'Trade', 'Plan', 'Error', 'Monthly'
            content TEXT,
            linked_trade_id INTEGER, -- Eğer bir işleme bağlıysa ID'si
            FOREIGN KEY (linked_trade_id) REFERENCES trades (id)
        )
    ''')

    conn.commit()
    conn.close()
    print("Veritabanı ve tablolar başarıyla hazırlandı.")

if __name__ == "__main__":
    init_db()