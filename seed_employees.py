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

CSV_FILE = "employees.csv"


def clean_value(value):
    """Ubah nilai kosong (NaN, 'nan', '', spasi) jadi None"""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("", "nan", "none", "null"):
            return None
    return value


def seed_employees():
    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip().str.lower()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        nip_nipp = clean_value(row.get("nip_nipp"))
        employee_name = clean_value(row.get("employee_name"))
        death_cause = clean_value(row.get("death_cause"))
        last_position = clean_value(row.get("last_position"))
        region_id = clean_value(row.get("region_id"))
        notes = clean_value(row.get("notes"))
        employee_gender = clean_value(row.get("employee_gender"))
        is_accident = clean_value(row.get("is_accident"))

        # Konversi boolean (string → bool)
        if isinstance(is_accident, str):
            is_accident = is_accident.strip().lower() == "true"

        sql = """
        INSERT INTO employees (
            nip_nipp, employee_name, death_cause, last_position,
            region_id, notes, employee_gender, is_accident
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (nip_nipp) DO NOTHING;
        """

        values = (
            nip_nipp,
            employee_name,
            death_cause,
            last_position,
            region_id,
            notes,
            employee_gender,
            is_accident,
        )

        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Employees seeded successfully (with proper NULL handling).")


if __name__ == "__main__":
    seed_employees()
