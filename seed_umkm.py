import pandas as pd
import psycopg2
import re

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

CSV_FILE = "umkm.csv"


def clean_value(value):
    """Ubah nilai kosong (NaN, 'nan', '', spasi) jadi None"""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        v = value.strip()
        if v.lower() in ("", "nan", "none", "null"):
            return None
        return v
    return value


def clean_coordinate(value):
    """Pastikan format koordinat 'lat,lon' valid"""
    if not value:
        return None
    if isinstance(value, (int, float)):
        return None
    value = str(value).strip()
    if not re.match(r"^-?[0-9]+(\.[0-9]+)?,\s*-?[0-9]+(\.[0-9]+)?$", value):
        return None
    return value


def clean_postal_code(value):
    """Pastikan postal code berupa string 5 digit"""
    v = clean_value(value)
    if v and re.match(r"^\d{5}$", str(v)):
        return str(v)
    return None


def seed_umkm():
    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip().str.lower()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        partner_id = clean_value(row.get("partner_id"))
        owner_name = clean_value(row.get("owner_name"))
        business_name = clean_value(row.get("business_name"))
        business_address = clean_value(row.get("business_address"))
        region_id = clean_value(row.get("region_id"))
        subdistrict_id = clean_value(row.get("subdistrict_id"))
        postal_code = clean_postal_code(row.get("postal_code"))
        umkm_coordinate = clean_coordinate(row.get("umkm_coordinate"))
        business_type = clean_value(row.get("business_type"))
        products = clean_value(row.get("products"))
        employee_id = clean_value(row.get("employee_id"))
        wali_id = clean_value(row.get("wali_id"))
        children_id = clean_value(row.get("children_id"))

        sql = """
        INSERT INTO umkm (
            partner_id, owner_name, business_name, business_address,
            region_id, subdistrict_id, postal_code, umkm_coordinate,
            business_type, products, employee_id, wali_id, children_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

        values = (
            partner_id,
            owner_name,
            business_name,
            business_address,
            region_id,
            subdistrict_id,
            postal_code,
            umkm_coordinate,
            business_type,
            products,
            employee_id,
            wali_id,
            children_id
        )

        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ UMKM seeded successfully with clean data validation.")


if __name__ == "__main__":
    seed_umkm()
