import pandas as pd
import psycopg2

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


def seed_partners():
    df = pd.read_csv(CSV_FILE)
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
        phone_number = clean_value(row.get("phone_number"))
        phone_number_alt = clean_value(row.get("phone_number_alt"))
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
    print("✅ Partners seeded successfully (with proper NULL handling).")


if __name__ == "__main__":
    seed_partners()
