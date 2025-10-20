import pandas as pd
import psycopg2

# --- Database connection config ---
DB_CONFIG = {
    "dbname": "ypkai",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": 5432,
}

# --- Path to CSV file ---
CSV_FILE = "provinces.csv"


def seed_provinces():
    # Read CSV
    df = pd.read_csv(CSV_FILE)

    # Normalize headers
    df.columns = df.columns.str.strip().str.lower()

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        province_code = str(row["province_code"])
        province_name = str(row["province_name"])

        # Handle region_id = null
        region_id = str(row["region_id"])

        sql = """
        INSERT INTO provinces (region_id, province_code, province_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (province_code) DO NOTHING;
        """
        cur.execute(sql, (region_id, province_code, province_name))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Provinces seeded successfully.")


if __name__ == "__main__":
    seed_provinces()
