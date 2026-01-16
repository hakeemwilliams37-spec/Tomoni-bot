import os
import sqlite3
from datetime import datetime, timezone, timedelta

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))

DB_PATH = "points.db"
SEASON_LENGTH_WEEKS = 24


# ----------------- Time helpers -----------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso_year_week(dt: datetime) -> tuple[int, int]:
    iso = dt.isocalendar()
    return int(iso.year), int(iso.week)

def week_key(dt: datetime) -> str:
    y, w = iso_year_week(dt)
    return f"{y}-W{w:02d}"

def weeks_between_iso(start_year: int, start_week: int, end_year: int, end_week: int) -> int:
    start_date = datetime.fromisocalendar(start_year, start_week, 1).replace(tzinfo=timezone.utc)
    end_date = datetime.fromisocalendar(end_year, end_week, 1).replace(tzinfo=timezone.utc)
    return max(0, int((end_date - start_date).days // 7))

def compute_end_week(start_year: int, start_week: int, length_weeks: int) -> tuple[int, int]:
    start_date = datetime.fromisocalendar(start_year, start_week, 1).replace(tzinfo=timezone.utc)
    end_date = start_date + timedelta(weeks=length_weeks - 1)
    iso = end_date.isocalendar()
    return int(iso.year), int(iso.week)


# ----------------- DB helpers -----------------

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def db_init():
    now = utc_now().isoformat()
    with db_connect() as conn:
        # Current balances
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_points (
            user_id INTEGER PRIMARY KEY,
            points INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        # Seasons (24-week cycles)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS seasons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_year INTEGER NOT NULL,
            start_week INTEGER NOT NULL,
            end_year INTEGER NOT NULL,
            end_week INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """)

        # Ledger / history of every change
        conn.execute("""
        CREATE TABLE IF NOT EXISTS ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            actor_id INTEGER,                -- who initiated (staff/user)
            action TEXT NOT NULL,            -- award | reset | deduct (redeem removed from public)
            delta INTEGER NOT NULL,           -- +/- change
            balance_before INTEGER NOT NULL,
            balance_after INTEGER NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(season_id) REFERENCES seasons(id)
        )
        """)

        # Ensure we have a current season
        cur = conn.execute("SELECT id FROM seasons ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        if row is None:
            y, w = iso_year_week(utc_now())
            end_y, end_w = compute_end_week(y, w, SEASON_LENGTH_WEEKS)
            conn.execute(
                "INSERT INTO seasons (start_year, start_week, end_year, end_week, created_at) VALUES (?, ?, ?, ?, ?)",
                (y, w, end_y, end_w, now)
            )

        conn.commit()

def get_current_season(conn: sqlite3.Connection) -> tuple[int, int, int, int, int]:
    cur = conn.execute("SELECT id, start_year, start_week, end_year, end_week FROM seasons ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    return int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4])

def get_current_season_info():
    with db_connect() as conn:
        db_init()
        return get_current_season(conn)

def ensure_user(conn: sqlite3.Connection, user_id: int):
    now = utc_now().isoformat()
    cur = conn.execute("SELECT user_id FROM user_points WHERE user_id=?", (user_id,))
    if cur.fetchone() is None:
        conn.execute(
            "INSERT INTO user_points (user_id, points, created_at, updated_at) VALUES (?, 0, ?, ?)",
            (user_id, now, now)
        )

def get_points(user_id: int) -> int:
    with db_connect() as conn:
        ensure_user(conn, user_id)
        cur = conn.execute("SELECT points FROM user_points WHERE user_id=?", (user_id,))
        return int(cur.fetchone()[0])

def apply_delta(user_id: int, delta: int, action: str, note: str | None, actor_id: int | None):
    """
    Atomic update: compute before/after, update balance, write ledger row.
    """
    now = utc_now().isoformat()
    with db_connect() as conn:
        db_init()
        season_id, *_ = get_current_season(conn)

        ensure_user(conn, user_id)
        cur = conn.execute("SELECT points FROM user_points WHERE user_id=?", (user_id,))
        before = int(cur.fetchone()[0])
        after = before + delta

        if after < 0:
            raise ValueError("Insufficient points")

        conn.execute("UPDATE user_points SET points=?, updated_at=? WHERE user_id=?", (after, now, user_id))

        conn.execute("""
            INSERT INTO ledger (season_id, user_id, actor_id, action, delta, balance_before, balance_after, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (season_id, user_id, actor_id, action, delta, before, after, note, now))

        conn.commit()
        return before, after, season_id

def get_user_history_for_season(user_id: int, season_id: int, limit: int = 10):
    with db_connect() as conn:
        db_init()
        cur = conn.execute("""
            SELECT action, delta, balance_before, balance_after, note, created_at
            FROM ledger
            WHERE user_id=? AND season_id=?
            ORDER BY id DESC
            LIMIT ?
        """, (user_id, season_id, limit))
        return cur.fetchall()

def reset_all_points(actor_id: int | None):
    """
    Start a new season and set everyone's points to 0.
    Writes a ledger reset entry for each user (history preserved).
    """
    now = utc_now().isoformat()
    y, w = iso_year_week(utc_now())
    end_y, end_w = compute_end_week(y, w, SEASON_LENGTH_WEEKS)

    with db_connect() as conn:
        db_init()

        # Create new season
        conn.execute(
            "INSERT INTO seasons (start_year, start_week, end_year, end_week, created_at) VALUES (?, ?, ?, ?, ?)",
            (y, w, end_y, end_w, now)
        )
        season_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

        # Reset all users currently in DB
        cur = conn.execute("SELECT user_id, points FROM user_points")
        users = cur.fetchall()

        for user_id, points in users:
            before = int(points)
            after = 0
            conn.execute("UPDATE user_points SET points=0, updated_at=? WHERE user_id=?", (now, user_id))

            conn.execute("""
                INSERT INTO ledger (season_id, user_id, actor_id, action, delta, balance_before, balance_after, note, created_at)
                VALUES (?, ?, ?, 'reset', ?, ?, ?, ?, ?)
            """, (season_id, user_id, actor_id, -before, before, after, "season reset", now))

        conn.commit()
        return season_id, f"{y}-W{w:02d}", f"{end_y}-W{end_w:02d}"


# ----------------- Bot setup -----------------

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
guild_obj = discord.Object(id=GUILD_ID) if GUILD_ID else None


@bot.event
async def on_ready():
    db_init()
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    try:
        if guild_obj:
            synced = await bot.tree.sync(guild=guild_obj)
            print(f"Synced {len(synced)} commands to guild {GUILD_ID}")
        else:
            synced = await bot.tree.sync()
            print(f"Synced {len(synced)} global commands")
    except Exception as e:
        print("Command sync failed:", e)


# ----------------- Commands -----------------

@bot.tree.command(name="points", description="Check your points.")
async def points_cmd(interaction: discord.Interaction):
    pts = get_points(interaction.user.id)
    await interaction.response.send_message(f"**{interaction.user.display_name}**, you have **{pts}** points.")


@bot.tree.command(name="season", description="Show the current 24-week season window.")
async def season_cmd(interaction: discord.Interaction):
    season_id, sy, sw, ey, ew = get_current_season_info()
    cy, cw = iso_year_week(utc_now())

    elapsed = weeks_between_iso(sy, sw, cy, cw)
    remaining = max(0, SEASON_LENGTH_WEEKS - 1 - elapsed)

    await interaction.response.send_message(
        f"📅 **Season Info**\n"
        f"Season ID: **{season_id}**\n"
        f"Window: **{sy}-W{sw:02d} → {ey}-W{ew:02d}**\n"
        f"Weeks remaining: **{remaining}**"
    )


@bot.tree.command(name="award", description="(Staff) Add points to a member.")
@app_commands.checks.has_permissions(manage_guild=True)
async def award_cmd(interaction: discord.Interaction, member: discord.Member, amount: int, reason: str = "manual award"):
    if amount <= 0:
        await interaction.response.send_message("Amount must be a positive integer.", ephemeral=True)
        return
    before, after, _ = apply_delta(member.id, amount, "award", reason, actor_id=interaction.user.id)
    await interaction.response.send_message(
        f"✅ Awarded **+{amount}** to {member.mention}.\n"
        f"Balance: **{before} → {after}**\n"
        f"Reason: {reason}"
    )


@bot.tree.command(name="deduct", description="(Staff) Remove points from a member.")
@app_commands.checks.has_permissions(manage_guild=True)
async def deduct_cmd(interaction: discord.Interaction, member: discord.Member, amount: int, reason: str = "manual deduction"):
    if amount <= 0:
        await interaction.response.send_message("Amount must be a positive integer.", ephemeral=True)
        return
    try:
        before, after, _ = apply_delta(member.id, -amount, "deduct", reason, actor_id=interaction.user.id)
    except ValueError:
        await interaction.response.send_message("❌ That user does not have enough points.", ephemeral=True)
        return

    await interaction.response.send_message(
        f"✅ Deducted **-{amount}** from {member.mention}.\n"
        f"Balance: **{before} → {after}**\n"
        f"Reason: {reason}"
    )


@bot.tree.command(
    name="history",
    description="View point history for the current season (your own, or any user if staff)."
)
async def history_cmd(
    interaction: discord.Interaction,
    member: discord.Member | None = None,
    limit: int = 10
):
    """
    - If member is not provided: show caller's history.
    - If member is provided: only staff (Manage Server) may view.
    - Always limited to current season (24-week span).
    """
    limit = max(1, min(limit, 20))

    target = member or interaction.user

    if member is not None:
        perms = interaction.user.guild_permissions
        if not perms.manage_guild:
            await interaction.response.send_message(
                "❌ You don't have permission to view other users' history.",
                ephemeral=True
            )
            return

    season_id, *_ = get_current_season_info()
    rows = get_user_history_for_season(target.id, season_id, limit=limit)

    if not rows:
        await interaction.response.send_message(
            f"No history found for **{target.display_name}** in the current season.",
            ephemeral=True
        )
        return

    lines = []
    for action, delta, before, after, note, created_at in rows:
        sign = "+" if delta > 0 else ""
        note_txt = f" — {note}" if note else ""
        lines.append(f"`{created_at[:19]}Z` **{action}** {sign}{delta} | {before} → {after}{note_txt}")

    header = f"📜 **History (current season) — {target.mention if isinstance(target, discord.Member) else target.display_name}**"
    await interaction.response.send_message(
        header + "\n" + "\n".join(lines),
        ephemeral=True
    )


@bot.tree.command(name="reset_season", description="(Staff) Reset everyone to 0 and start a new 24-week season.")
@app_commands.checks.has_permissions(manage_guild=True)
async def reset_season_cmd(interaction: discord.Interaction, confirm: bool):
    if not confirm:
        await interaction.response.send_message(
            "⚠️ Reset cancelled. Re-run with `confirm: true` to proceed.",
            ephemeral=True
        )
        return

    season_id, start_wk, end_wk = reset_all_points(actor_id=interaction.user.id)
    await interaction.response.send_message(
        f"♻️ **Season reset complete.**\n"
        f"New season ID: **{season_id}**\n"
        f"Season window: **{start_wk} → {end_wk}**\n"
        f"All user points set to **0** (history preserved)."
    )


# Error handler for staff-only commands
@award_cmd.error
@deduct_cmd.error
@reset_season_cmd.error
async def staff_cmd_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.errors.MissingPermissions):
        await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
    else:
        await interaction.response.send_message(f"Error: {error}", ephemeral=True)


# ----------------- Run -----------------

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN not found in .env")

bot.run(TOKEN)

