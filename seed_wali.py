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

CSV_FILE = "wali.csv"


def clean_value(value):
    """Ubah nilai kosong (NaN, 'nan', '', spasi) jadi None"""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("", "nan", "none", "null"):
            return None
    return value


def normalize_phone(value):
    """Pastikan nomor telepon tetap dalam format string (08...)"""
    v = clean_value(value)
    if v is None:
        return None

    # pastikan jadi string
    v = str(v).strip()

    # kalau float (misal "8123456789.0") ubah jadi int string
    if v.replace(".", "", 1).isdigit() and "." in v:
        v = str(int(float(v)))

    # kalau tidak diawali 0 tapi diawali 8 → tambahkan 0
    if not v.startswith("0") and v.startswith("8"):
        v = "0" + v

    # kalau kosong setelah clean, return None
    if v in ("", "nan", "none", "null"):
        return None

    return v


def seed_wali():
    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip().str.lower()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        employee_id = clean_value(row.get("employee_id"))
        wali_name = clean_value(row.get("wali_name"))
        relation = clean_value(row.get("relation"))
        wali_address = clean_value(row.get("wali_address"))
        address_coordinate = clean_value(row.get("address_coordinate"))
        wali_phone = normalize_phone(row.get("wali_phone"))

        sql = """
        INSERT INTO wali (
            employee_id, wali_name, relation, wali_address, 
            address_coordinate, wali_phone
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

        values = (
            employee_id,
            wali_name,
            relation,
            wali_address,
            address_coordinate,
            wali_phone,
        )

        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Wali seeded successfully (safe for int/float phone numbers).")


if __name__ == "__main__":
    seed_wali()
