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
CSV_FILE = "subdistricts.csv"


def seed_subdistricts():
    # Read CSV
    df = pd.read_csv(CSV_FILE)

    # Normalize headers
    df.columns = df.columns.str.strip().str.lower()

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        city_id = int(row["city_id"])
        subdistrict_code = str(row["subdistrict_code"]) if pd.notna(row["subdistrict_code"]) else None
        subdistrict_name = str(row["subdistrict_name"])

        sql = """
        INSERT INTO subdistricts (city_id, subdistrict_code, subdistrict_name)
        VALUES (%s, %s, %s);
        """
        cur.execute(sql, (city_id, subdistrict_code, subdistrict_name))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Subdistricts seeded successfully.")


if __name__ == "__main__":
    seed_subdistricts()
