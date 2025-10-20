import pandas as pd
import psycopg2

DB_CONFIG = {
    "dbname": "ypkai",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": 5432,
}

CSV_FILE = "umkm.csv"


def clean_value(value):
    """Ubah nilai kosong (NaN, 'nan', '', spasi) jadi None"""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("", "nan", "none", "null"):
            return None
    return value


def seed_umkm():
    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip().str.lower()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        employee_id = clean_value(row.get("employee_id"))
        owner_name = clean_value(row.get("owner_name"))
        business_name = clean_value(row.get("business_name"))
        business_address = clean_value(row.get("business_address"))
        region_id = clean_value(row.get("region_id"))
        subdistrict_id = clean_value(row.get("subdistrict_id"))
        postal_code = clean_value(row.get("postal_code"))
        umkm_coordinate = clean_value(row.get("umkm_coordinate"))
        business_type = clean_value(row.get("business_type"))
        products = clean_value(row.get("products"))
        partner_id = clean_value(row.get("partner_id"))

        sql = """
        INSERT INTO umkm (
            employee_id, owner_name, business_name, business_address,
            region_id, subdistrict_id, postal_code, umkm_coordinate,
            business_type, products, partner_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        values = (
            employee_id,
            owner_name,
            business_name,
            business_address,
            region_id,
            subdistrict_id,
            postal_code,
            umkm_coordinate,
            business_type,
            products,
            partner_id
        )

        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ UMKM seeded successfully (clean NULL handling).")


if __name__ == "__main__":
    seed_umkm()
