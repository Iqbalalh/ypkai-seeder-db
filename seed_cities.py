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
CSV_FILE = "cities.csv"


def seed_cities():
    # Read CSV
    df = pd.read_csv(CSV_FILE)

    # Normalize headers
    df.columns = df.columns.str.strip().str.lower()

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        province_id = row["province_id"]  # biarkan kosong apa adanya
        city_code = str(row["city_code"])
        city_name = str(row["city_name"])

        sql = """
        INSERT INTO cities (province_id, city_code, city_name)
        VALUES (%s, %s, %s);
        """
        cur.execute(sql, (province_id, city_code, city_name))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Cities seeded successfully.")


if __name__ == "__main__":
    seed_cities()
