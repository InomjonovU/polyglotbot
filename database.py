import aiosqlite
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "bot_data.db")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                phone TEXT,
                referred_by INTEGER,
                registered_at TEXT DEFAULT (datetime('now')),
                is_registered INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT UNIQUE NOT NULL,
                channel_url TEXT NOT NULL,
                title TEXT DEFAULT ''
            )
        """)
        # Default settings
        defaults = {
            "prizes_text": (
                "🏆 <b>Sovg'alar ro'yxati:</b>\n\n"
                "🥇 1-o'rin — 1 000 000 so'm\n"
                "🥈 2-o'rin — 500 000 so'm\n"
                "🥉 3-o'rin — 300 000 so'm\n"
                "4-10 o'rin — 100 000 so'mdan"
            ),
            "about_text": "",
            "welcome_text": (
                "Assalomu alaykum! 👋\n\n"
                "🎉 <b>PolyglotLC konkurs botiga xush kelibsiz!</b>\n\n"
                "Do'stlaringizni taklif qiling va ajoyib "
                "sovg'alar yutib oling! 🎁"
            ),
        }
        for key, value in defaults.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, value),
            )
        await db.commit()


async def get_db():
    return await aiosqlite.connect(DB_PATH)


# ── User operations ──────────────────────────────────────────


async def add_user(user_id: int, username: str, referred_by: int | None = None):
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, username, referred_by) VALUES (?, ?, ?)",
            (user_id, username, referred_by),
        )
        await db.commit()
    finally:
        await db.close()


async def update_user_registration(
    user_id: int, first_name: str, last_name: str, phone: str
):
    db = await get_db()
    try:
        await db.execute(
            """UPDATE users
               SET first_name = ?, last_name = ?, phone = ?, is_registered = 1, is_active = 1
               WHERE user_id = ?""",
            (first_name, last_name, phone, user_id),
        )
        await db.commit()
    finally:
        await db.close()


async def get_user(user_id: int):
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return await cursor.fetchone()
    finally:
        await db.close()


async def is_registered(user_id: int) -> bool:
    user = await get_user(user_id)
    return user is not None and user["is_registered"] == 1


async def deactivate_user(user_id: int):
    db = await get_db()
    try:
        await db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (user_id,))
        await db.commit()
    finally:
        await db.close()


async def activate_user(user_id: int):
    db = await get_db()
    try:
        await db.execute("UPDATE users SET is_active = 1 WHERE user_id = ?", (user_id,))
        await db.commit()
    finally:
        await db.close()


async def get_referral_count(user_id: int) -> int:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE referred_by = ? AND is_registered = 1 AND is_active = 1",
            (user_id,),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0
    finally:
        await db.close()


async def get_referrals(user_id: int) -> list:
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT first_name, last_name, registered_at, is_active
               FROM users
               WHERE referred_by = ? AND is_registered = 1
               ORDER BY registered_at DESC""",
            (user_id,),
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_top_referrers(limit: int = 10) -> list:
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT u.user_id, u.first_name, u.last_name, u.username,
                      COUNT(r.user_id) as ref_count
               FROM users u
               JOIN users r ON r.referred_by = u.user_id AND r.is_registered = 1 AND r.is_active = 1
               WHERE u.is_registered = 1 AND u.is_active = 1
               GROUP BY u.user_id
               ORDER BY ref_count DESC
               LIMIT ?""",
            (limit,),
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_all_user_ids() -> list[int]:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT user_id FROM users")
        rows = await cursor.fetchall()
        return [row[0] for row in rows]
    finally:
        await db.close()


async def get_user_count() -> int:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE is_registered = 1")
        row = await cursor.fetchone()
        return row[0] if row else 0
    finally:
        await db.close()


async def get_active_user_count() -> int:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE is_registered = 1 AND is_active = 1"
        )
        row = await cursor.fetchone()
        return row[0] if row else 0
    finally:
        await db.close()


# ── Channel operations ────────────────────────────────────────


async def add_channel(channel_id: str, channel_url: str, title: str = "") -> bool:
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO channels (channel_id, channel_url, title) VALUES (?, ?, ?)",
            (channel_id, channel_url, title),
        )
        await db.commit()
        return True
    except Exception:
        return False
    finally:
        await db.close()


async def remove_channel(channel_id: str) -> bool:
    db = await get_db()
    try:
        cursor = await db.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


async def get_all_channels() -> list:
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM channels ORDER BY id")
        return await cursor.fetchall()
    finally:
        await db.close()


# ── Settings operations ──────────────────────────────────────


async def get_setting(key: str) -> str | None:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row[0] if row else None
    finally:
        await db.close()


async def set_setting(key: str, value: str):
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        await db.commit()
    finally:
        await db.close()
