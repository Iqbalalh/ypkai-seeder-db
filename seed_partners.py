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

CSV_FILE = "partners.csv"


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
    """Pastikan nomor telepon tetap string dan tidak berubah menjadi float"""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        # Hilangkan .0 dan pastikan tetap diawali 0 jika ada di data asli
        value = str(int(value))
    value = str(value).strip()
    # Jika hilang leading zero karena CSV (misal "812345678"), tambahkan 0
    if not value.startswith("0") and value.isdigit():
        value = "0" + value
    return value


def seed_partners():
    # Pastikan dua kolom dibaca sebagai string
    df = pd.read_csv(CSV_FILE, dtype={"phone_number": str, "phone_number_alt": str})
    df.columns = df.columns.str.strip().str.lower()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        employee_id = clean_value(row.get("employee_id"))
        partner_name = clean_value(row.get("partner_name"))
        partner_job = clean_value(row.get("partner_job"))
        partner_nik = clean_value(row.get("partner_nik"))
        region_id = clean_value(row.get("region_id"))
        address = clean_value(row.get("address"))
        subdistrict_id = clean_value(row.get("subdistrict_id"))
        postal_code = clean_value(row.get("postal_code"))
        home_coordinate = clean_value(row.get("home_coordinate"))
        phone_number = normalize_phone(row.get("phone_number"))
        phone_number_alt = normalize_phone(row.get("phone_number_alt"))
        is_active = clean_value(row.get("is_active"))

        # Konversi boolean string ke tipe bool Python
        if isinstance(is_active, str):
            is_active = is_active.strip().lower() == "true"

        sql = """
        INSERT INTO partners (
            employee_id, partner_name, partner_job, partner_nik,
            region_id, address, subdistrict_id, postal_code,
            home_coordinate, phone_number, phone_number_alt, is_active
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

        values = (
            employee_id,
            partner_name,
            partner_job,
            partner_nik,
            region_id,
            address,
            subdistrict_id,
            postal_code,
            home_coordinate,
            phone_number,
            phone_number_alt,
            is_active,
        )

        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Partners seeded successfully (phone numbers preserved as strings).")


if __name__ == "__main__":
    seed_partners()
