import pandas as pd
import psycopg2
from datetime import datetime

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

CSV_FILE = "children.csv"


def clean_value(value):
    """Convert empty (NaN, 'nan', '', ' ', etc.) to None"""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        v = value.strip()
        if v.lower() in ("", "nan", "none", "null"):
            return None
        return v
    return value


def parse_bool(value):
    """Convert text/number to boolean"""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    v = str(value).strip().lower()
    return v in ("true", "1", "yes", "y")


def parse_date(value):
    """Convert string to date if possible"""
    if value is None:
        return None
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def seed_childrens():
    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip().str.lower()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        employee_id = clean_value(row.get("employee_id"))
        partner_id = clean_value(row.get("partner_id"))
        home_id = clean_value(row.get("home_id"))
        children_name = clean_value(row.get("children_name"))
        is_active = parse_bool(row.get("is_active"))
        children_birthdate = parse_date(row.get("children_birthdate"))
        children_address = clean_value(row.get("children_address"))
        children_phone = clean_value(row.get("children_phone"))
        notes = clean_value(row.get("notes"))
        is_father_alive = parse_bool(row.get("is_father_alive"))
        is_mother_alive = parse_bool(row.get("is_mother_alive"))
        children_gender = clean_value(row.get("children_gender"))
        is_condition = parse_bool(row.get("is_condition"))

        # Pastikan gender valid
        if children_gender not in ("M", "F"):
            children_gender = None

        sql = """
        INSERT INTO childrens (
            employee_id, partner_id, home_id, children_name, is_active,
            children_birthdate, children_address, children_phone, notes,
            is_father_alive, is_mother_alive, children_gender, is_condition
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

        values = (
            employee_id,
            partner_id,
            home_id,
            children_name,
            is_active,
            children_birthdate,
            children_address,
            children_phone,
            notes,
            is_father_alive,
            is_mother_alive,
            children_gender,
            is_condition,
        )

        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Childrens seeded successfully (full fields + safe type handling).")


if __name__ == "__main__":
    seed_childrens()
