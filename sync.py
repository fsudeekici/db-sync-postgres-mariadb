import os
import time
import schedule
import requests
import psycopg2
import mysql.connector
from datetime import datetime
from threading import Thread
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Database config from .env
BE_DB = {
    'host': os.getenv("BE_DB_HOST"),
    'port': os.getenv("BE_DB_PORT"),
    'database': os.getenv("BE_DB_NAME"),
    'user': os.getenv("BE_DB_USER"),
    'password': os.getenv("BE_DB_PASSWORD")
}

ORHAN_DB = {
    'host': os.getenv("ORHAN_DB_HOST"),
    'port': os.getenv("ORHAN_DB_PORT"),
    'database': os.getenv("ORHAN_DB_NAME"),
    'user': os.getenv("ORHAN_DB_USER"),
    'password': os.getenv("ORHAN_DB_PASSWORD")
}

TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")

# region Utility Functions
def send_teams_message(message: str, title: Optional[str] = None, color: str = "0078d4",
                       summary: Optional[str] = "Bildirim"):
    """Send notification to Teams channel"""
    try:
        payload = {
            "@type": "MessageCard",
            "@context": "",
            "summary": summary,
            "themeColor": color,
            "title": title,
            "text": message
        }
        headers = {'Content-Type': 'application/json'}

        def send_request():
            response = requests.post(TEAMS_WEBHOOK_URL, json=payload, headers=headers)
            response.raise_for_status()

        thread = Thread(target=send_request)
        thread.start()
        return True
    except Exception as e:
        print(f"Teams mesajı gönderilirken hata oluştu: {e}")
        return False


def print_notification(message):
    """Print formatted notification with timestamp"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n=== BİLDİRİM [{now}] ===")
    print(message)
    print("=" * 50)


# endregion

# region Enhanced Teams Messaging Functions

def send_detailed_start_message():
    start_time = datetime.now()

    message = f"""Database Synchronization Basladi

Tarih: {start_time.strftime('%d.%m.%Y')}
Baslangic Saati: {start_time.strftime('%H:%M:%S')}
Sunucu: {os.getenv('COMPUTERNAME', os.getenv('HOSTNAME', 'Unknown'))}

Islem Sirasi:
1. Customers Master tablosu
2. Routes tablosu  
3. Users tablosu (Upsert Logic)
"""

    send_teams_message(message, "Database Sync Basladi", "0078d4")
    return start_time


def send_individual_table_completion(table_name, stats):
    """Her tablo için ayrı tamamlanma mesajı"""

    duration_str = str(stats.get('duration', 'N/A')).split('.')[0]  # Saniye kısmını kaldır

    if table_name.lower() == "customers":
        message = f"""Customers Sync Tamamlandi

Istatistikler:
- Kaynak: PostgreSQL (neocortex_schema_v1.customers_master)
- Hedef: MariaDB (admin_efes1.customers_master)
- Okunan Kayit: {stats.get('read_count', 0):,}
- Eklenen Kayit: {stats.get('insert_count', 0):,}
- Final Toplam: {stats.get('total_count', 0):,}

Sure: {duration_str}"""

    elif table_name.lower() == "routes":
        message = f"""Routes Sync Tamamlandi

Istatistikler:
- Kaynak: PostgreSQL (neocortex_schema_v1.routes)
- Hedef: MariaDB (admin_efes1.routes)
- Okunan Kayit: {stats.get('read_count', 0):,}
- Eklenen Kayit: {stats.get('insert_count', 0):,}
- Final Toplam: {stats.get('total_count', 0):,}

Sure: {duration_str}"""

    elif table_name.lower() == "users":
        message = f"""Users Sync Tamamlandi

Istatistikler:
- Kaynak: PostgreSQL (neocortex_schema_v1.users)
- Hedef: MariaDB (users_Sude)
- Toplam Okunan: {stats.get('read_count', 0):,}
- Yeni Eklenen: {stats.get('insert_count', 0):,}
- Guncellenen: {stats.get('update_count', 0):,}
- Degismeyen: {stats.get('no_change_count', 0):,}
- Cakisan/Hatali: {stats.get('error_count', 0):,}

Sure: {duration_str}"""

    color = "00ff00" if stats.get('error_count', 0) == 0 else "ff9900"
    send_teams_message(message, f"{table_name.title()} Tamamlandi", color)


def send_final_summary(start_time, customers_stats, routes_stats, users_stats, success=True):
    """general summary"""
    end_time = datetime.now()
    total_duration = end_time - start_time
    duration_str = str(total_duration).split('.')[0]  # Saniye kısmını kaldır

    if success:
        # Toplam kayıt sayıları
        total_processed = (
                customers_stats.get('read_count', 0) +
                routes_stats.get('read_count', 0) +
                users_stats.get('read_count', 0)
        )

        total_inserted = (
                customers_stats.get('insert_count', 0) +
                routes_stats.get('insert_count', 0) +
                users_stats.get('insert_count', 0)
        )

        total_updated = users_stats.get('update_count', 0)
        total_errors = users_stats.get('error_count', 0)

        message = f"""Database Synchronization Basariyla Tamamlandi!

Zaman Bilgileri:
- Baslangic: {start_time.strftime('%d.%m.%Y %H:%M:%S')}
- Bitis: {end_time.strftime('%d.%m.%Y %H:%M:%S')}
- Toplam Sure: {duration_str}

Genel Istatistikler:
- Toplam Okunan: {total_processed:,} kayit
- Toplam Eklenen: {total_inserted:,} kayit  
- Toplam Guncellenen: {total_updated:,} kayit
- Toplam Hata: {total_errors:,} kayit

Tablo Bazinda Ozet:

Customers Master:
- {customers_stats.get('total_count', 0):,} kayit - Tam Sync

Routes:
- {routes_stats.get('total_count', 0):,} kayit - Tam Sync

Users:
- {users_stats.get('insert_count', 0):,} yeni + {users_stats.get('update_count', 0):,} guncelleme - Upsert

Veritabanlari:
- Kaynak: PostgreSQL (BE_DB)
- Hedef: MariaDB (ORHAN_DB)

Sonuc: {"BASARILI" if total_errors == 0 else "UYARILARLA BASARILI"}"""

        color = "00ff00" if total_errors == 0 else "ff9900"

    else:
        message = f"""Database Synchronization Basarisiz!

Zaman Bilgileri:
- Baslangic: {start_time.strftime('%d.%m.%Y %H:%M:%S')}
- Hata Zamani: {end_time.strftime('%d.%m.%Y %H:%M:%S')}
- Gecen Sure: {duration_str}

Hata Durumu: Islem yarida kesildi
Oneri: Loglari kontrol edin ve tekrar deneyin"""

        color = "ff0000"

    send_teams_message(message, "SYNC OZET RAPORU", color)


# endregion

# region Customer Sync Function

def sync_customers(pg_cursor, maria_cursor, maria_conn):
    print("Customers tablosu senkronizasyonu başlıyor")
    start_time = datetime.now()

    # region Data Fetch and Preparation
    # Fetch all records from PostgreSQL
    pg_cursor.execute("SELECT * FROM neocortex_schema_v1.customers_master ORDER BY id")
    records = pg_cursor.fetchall()
    print(f"{len(records)} kayıt bulundu")

    # Clear destination table
    maria_cursor.execute("TRUNCATE TABLE admin_efes1.customers_master")
    maria_conn.commit()

    # Get destination table columns
    maria_cursor.execute("SHOW COLUMNS FROM admin_efes1.customers_master")
    columns = []
    for row in maria_cursor.fetchall():
        columns.append(row[0])

    query = f"INSERT INTO admin_efes1.customers_master ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
    # endregion

    # region Data Processing and Insertion
    batch = []
    insert_count = 0

    # Process records in batches
    for i, record in enumerate(records, 1):
        data = {}
        for j, col_name in enumerate([desc[0] for desc in pg_cursor.description]):
            data[col_name] = record[j]

        # Handle boolean to string conversion for 'durum' field
        if 'durum' in data:
            if data['durum']:
                data['durum'] = 'true'
            elif not data['durum']:
                data['durum'] = 'false'
            else:
                data['durum'] = None

        # Map columns and handle special cases
        values = []
        for col in columns:
            if col == 'telefon':
                values.append(None)
            elif col == 'telefon_1':
                values.append(data.get('telefon'))
            else:
                value = data.get(col)
                # Convert arrays to PostgreSQL array format
                if isinstance(value, list):
                    if value:
                        value = '{' + ','.join(str(x) for x in value) + '}'
                    else:
                        value = None
                values.append(value)

        batch.append(values)

        # Execute batch insert every 1000 records
        if i % 1000 == 0 or i == len(records):
            maria_cursor.executemany(query, batch)
            maria_conn.commit()
            insert_count += len(batch)
            print(f"{i} kayıt yazıldı")
            batch = []
    # endregion

    # region Final Count and Stats
    # Verify final count
    maria_cursor.execute("SELECT COUNT(*) FROM admin_efes1.customers_master")
    total_count = maria_cursor.fetchone()[0]

    # Prepare stats
    stats = {
        'read_count': len(records),
        'insert_count': insert_count,
        'total_count': total_count,
        'duration': datetime.now() - start_time
    }

    # Send completion notification
    print(f"Customers tamamlandı - Toplam: {total_count} kayıt")
    send_individual_table_completion("customers", stats)
    print_notification(f"CUSTOMERS SYNC TAMAMLANDI\nEklenen: {insert_count} kayıt")

    return stats
    # endregion


# endregion

# region Routes Sync Function

def sync_routes(pg_cursor, maria_cursor, maria_conn):
    print("Routes tablosu senkronizasyonu başlıyor...")
    start_time = datetime.now()

    # region Data Fetch and Preparation
    # Fetch all routes from PostgreSQL
    pg_cursor.execute("SELECT * FROM neocortex_schema_v1.routes ORDER BY id")
    records = pg_cursor.fetchall()
    print(f"{len(records)} kayıt bulundu")

    # Clear and prepare destination table
    maria_cursor.execute("TRUNCATE TABLE admin_efes1.routes")
    maria_conn.commit()

    maria_cursor.execute("SHOW COLUMNS FROM admin_efes1.routes")
    columns = [row[0] for row in maria_cursor.fetchall()]

    query = f"INSERT INTO admin_efes1.routes ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
    # endregion

    # region Data Processing and Insertion
    batch = []
    insert_count = 0

    # Process and insert routes in batches
    for i, record in enumerate(records, 1):
        data = {}
        for j, col_name in enumerate([desc[0] for desc in pg_cursor.description]):
            data[col_name] = record[j]

        values = []
        for col in columns:
            values.append(data.get(col))
        batch.append(values)

        # Batch insert every 1000 records
        if i % 1000 == 0 or i == len(records):
            maria_cursor.executemany(query, batch)
            maria_conn.commit()
            insert_count += len(batch)
            print(f"{i} kayıt yazıldı")
            batch = []
    # endregion

    # region Final Count and Stats
    # Verify final count
    maria_cursor.execute("SELECT COUNT(*) FROM admin_efes1.routes")
    total_count = maria_cursor.fetchone()[0]

    # Prepare stats
    stats = {
        'read_count': len(records),
        'insert_count': insert_count,
        'total_count': total_count,
        'duration': datetime.now() - start_time
    }

    # Send completion notification
    print(f"Routes tamamlandı - Toplam: {total_count} kayıt")
    send_individual_table_completion("routes", stats)
    print_notification(f"ROUTES SYNC TAMAMLANDI\nEklenen: {insert_count} kayıt")

    return stats
    # endregion


# endregion

# region Users Sync Function

def sync_users(pg_cursor, maria_cursor, maria_conn):
    print("Users senkronizasyonu başlıyor.")
    start_time = datetime.now()

    insert_count = 0
    update_count = 0
    no_change_count = 0
    error_messages = []

    # Change maria_cursor to dictionary mode
    maria_cursor = maria_conn.cursor(dictionary=True)

    # Get all users from PostgreSQL
    pg_cursor.execute("SELECT * FROM neocortex_schema_v1.users ORDER BY id, position_code")
    rows = pg_cursor.fetchall()
    columns = [desc[0] for desc in pg_cursor.description]
    pg_user_keys = set()

    # Process each user record
    for row in rows:
        user_data = {}
        for i, col in enumerate(columns):
            val = row[i]
            # Convert arrays to string format
            if isinstance(val, list):
                val = '{' + ','.join(str(x) for x in val) + '}'
            user_data[col] = val

        if 'account_active' in user_data:
            if user_data['account_active']:
                user_data['account_active'] = 1
            else:
                user_data['account_active'] = 0

        user_id = user_data['id']
        position_code = user_data['position_code']
        pg_user_keys.add((user_id, position_code))

        # Check if user exists with same id and position_code
        maria_cursor.execute(
            "SELECT * FROM admin_efes1.users WHERE id=%s AND position_code=%s",
            (user_id, position_code)
        )
        existing_user = maria_cursor.fetchone()

        if existing_user:
            # Update existing user if data changed
            updates, update_values = [], []

            for col in columns:
                if col in ('id', 'position_code'):
                    continue
                if user_data[col] != existing_user[col]:
                    updates.append(f"{col}=%s")
                    update_values.append(user_data[col])

            if updates:
                update_values.extend([user_id, position_code])
                sql = f"UPDATE admin_efes1.users SET {', '.join(updates)} WHERE id=%s AND position_code=%s"
                maria_cursor.execute(sql, update_values)
                print(f"Güncellendi: id={user_id}, position={position_code}")
                update_count += 1
            else:
                print(f"Değişiklik yok: id={user_id}, position={position_code}")
                no_change_count += 1

        else:
            # Handle new user insertion with conflict resolution
            maria_cursor.execute("SELECT * FROM admin_efes1.users WHERE id=%s", (user_id,))
            rows_same_id = maria_cursor.fetchall()

            # Skip if PostgreSQL has NULL position_code but MariaDB already has this ID
            if rows_same_id and position_code is None:
                mesaj = f"SKIP: id={user_id} → PostgreSQL'de position_code NULL ve MariaDB'de zaten bu id var."
                print(mesaj)
                error_messages.append(mesaj)
                continue

            # Update position_code for existing user with same ID
            if rows_same_id:
                for row_id in rows_same_id:
                    old_position_code = row_id['position_code']
                    if old_position_code != position_code:
                        updates, update_values = [], []
                        for col in columns:
                            if col in ('id', 'position_code'):
                                continue
                            if user_data[col] != row_id[col]:
                                updates.append(f"{col}=%s")
                                update_values.append(user_data[col])
                        updates.append("position_code=%s")
                        update_values.append(position_code)
                        update_values.extend([user_id, old_position_code])

                        sql = f"UPDATE admin_efes1.users SET {', '.join(updates)} WHERE id=%s AND position_code=%s"
                        maria_cursor.execute(sql, update_values)
                        print(
                            f"Position_code güncellendi: ID={user_id}, eski Position={old_position_code}, yeni Position={position_code}")
                        update_count += 1
            else:
                # Check for position_code conflicts before inserting
                maria_cursor.execute(
                    "SELECT * FROM admin_efes1.users WHERE position_code=%s",
                    (position_code,)
                )
                rows_same_position = maria_cursor.fetchall()

                conflict_found = False
                for row in rows_same_position:
                    if row['id'] != user_id:
                        mesaj = f"UYARI: Aynı position_code farklı id ile var: position_code={position_code}, mevcut_id={row['id']}, yeni_id={user_id}"
                        print(mesaj)
                        error_messages.append(mesaj)
                        conflict_found = True

                # Insert new user if no conflicts
                if not conflict_found:
                    cols_str = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(columns))
                    values = []
                    for col in columns:
                        val = user_data[col]
                        if isinstance(val, list):
                            val = '{' + ','.join(str(x) for x in val) + '}'
                        values.append(val)

                    insert_sql = f"INSERT INTO admin_efes1.users ({cols_str}) VALUES ({placeholders})"
                    maria_cursor.execute(insert_sql, values)
                    print(f"Eklendi: ID={user_id}, Position={position_code}")
                    insert_count += 1

    # Check for records in MariaDB that don't exist in PostgreSQL
    maria_cursor.execute("SELECT id, position_code FROM admin_efes1.users")
    maria_users = maria_cursor.fetchall()

    for user in maria_users:
        key = (user['id'], user['position_code'])
        if key not in pg_user_keys:
            mesaj = f"MariaDB'de var ama PostgreSQL'de yok: ID={key[0]}, Position={key[1]}"
            print(mesaj)
            error_messages.append(mesaj)

    maria_conn.commit()
    maria_cursor.close()  # Close dictionary cursor

    # Prepare stats
    stats = {
        'read_count': len(rows),
        'insert_count': insert_count,
        'update_count': update_count,
        'no_change_count': no_change_count,
        'error_count': len(error_messages),
        'duration': datetime.now() - start_time
    }

    # Summary and send notifications
    print("\n--- SENKRONİZASYON ÖZETİ ---")
    print(f"Toplam PostgreSQL'den okunan kayıt: {len(rows)}")
    print(f"Güncellenen: {update_count}")
    print(f"Ekleme yapılan: {insert_count}")
    print(f"Değişmeyen: {no_change_count}")
    print(f"Hatalı / Çakışan: {len(error_messages)}")
    for err in error_messages:
        print("HATA:", err)

    send_individual_table_completion("users", stats)
    print_notification(f"USERS SYNC TAMAMLANDI\nEklenen: {insert_count}, Güncellenen: {update_count}")

    return stats


# endregion

# region Main Sync Function

def run_sync():
    start_time = send_detailed_start_message()

    print("=== DATABASE SYNC BAŞLIYOR ===")
    print(f"Başlangıç zamanı: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Database connections - tek sefer açılıyor
    pg_conn = None
    maria_conn = None

    try:
        # Connect to PostgreSQL (source)
        print("PostgreSQL bağlantısı kuruluyor...")
        pg_conn = psycopg2.connect(**BE_DB)
        pg_conn.set_client_encoding('UTF8')
        pg_cursor = pg_conn.cursor()

        # Connect to MariaDB (destination)
        print("MariaDB bağlantısı kuruluyor...")
        maria_conn = mysql.connector.connect(**ORHAN_DB, charset='utf8mb4', collation='utf8mb4_unicode_ci')
        maria_cursor = maria_conn.cursor()

        # Tüm sync işlemlerini çalıştır ve sonuçları al
        print("\nCustomers sync başlıyor.")
        customers_stats = sync_customers(pg_cursor, maria_cursor, maria_conn)

        print("\nRoutes sync başlıyor.")
        routes_stats = sync_routes(pg_cursor, maria_cursor, maria_conn)

        print("\nUsers sync başlıyor.")
        users_stats = sync_users(pg_cursor, maria_cursor, maria_conn)

        # Final özet raporu gönder
        send_final_summary(start_time, customers_stats, routes_stats, users_stats, success=True)

        end_time = datetime.now()
        duration = end_time - start_time

        print("TÜM SYNC TAMAMLANDI")
        print(f"Süre: {duration}")
        print(f"Bitiş zamanı: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

        print_notification(f"TÜM SYNC TAMAMLANDI\nSüre: {duration}")

    except Exception as e:
        # Hata durumunda detaylı rapor
        end_time = datetime.now()
        duration = end_time - start_time
        duration_str = str(duration).split('.')[0]  # Saniye kısmını kaldır

        error_message = f"""SYNC HATASI

Zaman: {end_time.strftime('%d.%m.%Y %H:%M:%S')}
Hata: {str(e)}
Gecen Sure: {duration_str}
Durum: Islem yarida kesildi
"""

        send_teams_message(error_message, "SYNC HATASI", "ff0000")
        print(f"HATA: {e}")
        raise

    finally:
        # Database bağlantılarını kapat
        try:
            if pg_conn:
                pg_cursor.close()
                pg_conn.close()
                print("PostgreSQL bağlantısı kapatıldı")
        except:
            pass

        try:
            if maria_conn:
                maria_cursor.close()
                maria_conn.close()
                print("MariaDB bağlantısı kapatıldı")
        except:
            pass


# endregion


# Example of scheduler setup
if __name__ == "__main__":
    schedule.every().day.at("23:00").do(run_sync)

    print("Zamanlayıcı aktif - Her gece 23:00'da sync çalışacak")
    print("Eğer hemen test etmek istiyorsanız run_sync() fonksiyonunu çağırabilirsiniz")
    print("Durdurmak için Ctrl+C")

    while True:
        schedule.run_pending()
        time.sleep(60)