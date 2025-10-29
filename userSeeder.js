import bcrypt from "bcrypt";
import pool from "../config/db.js";


async function seedUser() {
  const username = "ypkai";
  const password = "ypkai123";
  const hashedPassword = await bcrypt.hash(password, 10);

  try {
    const checkUser = await pool.query(
      "SELECT * FROM users WHERE username = $1",
      [username]
    );

    if (checkUser.rows.length > 0) {
      console.log(`✅ User "${username}" already exists.`);
      return;
    }

    await pool.query(
      "INSERT INTO users (username, password) VALUES ($1, $2)",
      [username, hashedPassword]
    );

    console.log(`🎉 Admin user created: ${username}`);
  } catch (error) {
    console.error("❌ Error seeding user:", error);
  } finally {
    await pool.end();
  }
}

seedUser();
