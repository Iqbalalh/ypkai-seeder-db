import pandas as pd
import psycopg2

# DB_CONFIG = {
#     "dbname": "ypkai",
#     "user": "ypkai",
#     "password": "ypkai123",
#     "host": "ypkai.crk26ae6i2kw.ap-southeast-2.rds.amazonaws.com",
#     "port": 5432,
# }

DB_CONFIG = {
    "dbname": "ypkai",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": 5432,
}

CSV_FILE = "homes.csv"

def clean_value(value):
    """Ubah nilai kosong (NaN, 'nan', '', spasi) jadi None"""
    if pd.isna(value):
        return None
    if isinstance(value, str) and value.strip().lower() in ["nan", "none", "null", ""]:
        return None
    return value

def seed_homes():
    df = pd.read_csv(CSV_FILE)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    sql = """
        INSERT INTO homes (partner_id, employee_id, wali_id)
        VALUES (%s, %s, %s)
    """

    for _, row in df.iterrows():
        partner_id = clean_value(row.get("partner_id"))
        employee_id = clean_value(row.get("employee_id"))
        wali_id = clean_value(row.get("wali_id"))

        # Pastikan id berupa int (bukan float dari CSV)
        if partner_id is not None:
            partner_id = int(partner_id)
        if employee_id is not None:
            employee_id = int(employee_id)
        if wali_id is not None:
            wali_id = int(wali_id)

        values = (partner_id, employee_id, wali_id)
        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Seeder homes.csv berhasil dimasukkan ke database!")

if __name__ == "__main__":
    seed_homes()
