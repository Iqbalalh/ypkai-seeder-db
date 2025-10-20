import pandas as pd
import psycopg2
import bcrypt

DB_CONFIG = {
    "dbname": "ypkai",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": 5432,
}

CSV_FILE = "users.csv"


def clean_value(value):
    """Normalize empty values to None."""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("", "nan", "none", "null"):
            return None
    return value


def hash_password(plain_password):
    """Hash password using bcrypt."""
    if plain_password is None:
        return None
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(plain_password.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def seed_users():
    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip().str.lower()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        username = clean_value(row.get("username"))
        password = clean_value(row.get("password"))

        if not username or not password:
            print(f"⚠️ Skipping invalid row: {row}")
            continue

        hashed_password = hash_password(password)

        sql = """
        INSERT INTO users (username, password)
        VALUES (%s, %s)
        ON CONFLICT (username) DO NOTHING;
        """

        cur.execute(sql, (username, hashed_password))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Users seeded successfully with bcrypt-hashed passwords.")


if __name__ == "__main__":
    seed_users()
