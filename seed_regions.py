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
CSV_FILE = "regions.csv"


def seed_regions():
    # Read CSV
    df = pd.read_csv(CSV_FILE)

    # Normalize headers
    df.columns = df.columns.str.strip().str.lower()

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        region_code = str(row["region_code"])
        region_name = str(row["region_name"])

        sql = """
        INSERT INTO regions (region_code, region_name)
        VALUES (%s, %s)
        ON CONFLICT (region_code) DO NOTHING;
        """
        cur.execute(sql, (region_code, region_name))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Regions seeded successfully.")


if __name__ == "__main__":
    seed_regions()
