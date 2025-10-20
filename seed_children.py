import pandas as pd
import psycopg2

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
        v = value.strip().lower()
        if v in ("", "nan", "none", "null"):
            return None
    return value


def seed_children():
    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip().str.lower()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        employee_id = clean_value(row.get("employee_id"))
        children_name = clean_value(row.get("children_name"))
        partner_id =  clean_value(row.get("partner_id"))
        sql = """
        INSERT INTO children (children_name, employee_id, partner_id)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

        values = (children_name, employee_id, partner_id)
        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Children seeded successfully (with proper NULL handling).")


if __name__ == "__main__":
    seed_children()
