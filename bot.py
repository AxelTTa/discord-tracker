"""Parsewave Activity Tracker — Discord bot. Writes to Turso cloud DB."""
import asyncio
import datetime as dt
import os
import shutil
import subprocess
import sys
import threading
from collections import defaultdict
from pathlib import Path

import discord
import libsql_experimental as libsql
from flask import Flask, jsonify, request

# ---------- config ----------
TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
GUILD_ID = os.environ.get("DISCORD_GUILD_ID")
TURSO_URL = os.environ.get("TURSO_URL")
TURSO_TOKEN = os.environ.get("TURSO_TOKEN")
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "60"))
BACKFILL_CONCURRENCY = int(os.environ.get("BACKFILL_CONCURRENCY", "4"))
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "60"))
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")
BOT_API_PORT = int(os.environ.get("BOT_API_PORT", "5001"))

_ask_lock = threading.Lock()
_WORK_DIR = Path(__file__).parent.resolve()

if not TOKEN or not GUILD_ID:
    sys.exit("Set DISCORD_BOT_TOKEN and DISCORD_GUILD_ID.")
GUILD_ID = int(GUILD_ID)

if not TURSO_URL or not TURSO_TOKEN:
    sys.exit("Set TURSO_URL and TURSO_TOKEN.")


# ---------- DB ----------
def db():
    return libsql.connect(TURSO_URL, auth_token=TURSO_TOKEN)


SCHEMA = [
    """CREATE TABLE IF NOT EXISTS members (
        user_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        display_name TEXT,
        avatar_url TEXT,
        joined_at TEXT,
        left_at TEXT,
        is_bot INTEGER NOT NULL DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS roles (
        role_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        color INTEGER DEFAULT 0,
        position INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS member_roles (
        user_id INTEGER NOT NULL,
        role_id INTEGER NOT NULL,
        PRIMARY KEY (user_id, role_id),
        FOREIGN KEY (user_id) REFERENCES members(user_id),
        FOREIGN KEY (role_id) REFERENCES roles(role_id)
    )""",
    """CREATE TABLE IF NOT EXISTS channels (
        channel_id INTEGER PRIMARY KEY,
        name TEXT,
        category TEXT,
        type TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY,
        channel_id INTEGER NOT NULL,
        author_id INTEGER NOT NULL,
        content TEXT,
        created_at TEXT NOT NULL,
        attachment_count INTEGER DEFAULT 0
    )""",
    "CREATE INDEX IF NOT EXISTS idx_msg_created ON messages(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_msg_author ON messages(author_id)",
    "CREATE INDEX IF NOT EXISTS idx_msg_author_created ON messages(author_id, created_at)",
    """CREATE TABLE IF NOT EXISTS voice_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        joined_at TEXT NOT NULL,
        left_at TEXT,
        duration_sec INTEGER
    )""",
    "CREATE INDEX IF NOT EXISTS idx_vs_user ON voice_sessions(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_vs_joined ON voice_sessions(joined_at)",
    "CREATE INDEX IF NOT EXISTS idx_vs_open ON voice_sessions(left_at)",
]


def init_schema():
    conn = db()
    for stmt in SCHEMA:
        try:
            conn.execute(stmt)
        except Exception as e:
            print(f"[schema] {e}")
    conn.commit()
    print("[schema] initialized.")


def now_iso():
    return discord.utils.utcnow().isoformat()


# ---------- Discord bot ----------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.voice_states = True
client = discord.Client(intents=intents)


def snapshot_guild(guild: discord.Guild):
    conn = db()
    for r in guild.roles:
        conn.execute(
            """INSERT INTO roles (role_id, name, color, position) VALUES (?,?,?,?)
               ON CONFLICT(role_id) DO UPDATE SET
                 name=excluded.name, color=excluded.color, position=excluded.position""",
            (r.id, r.name, r.color.value if r.color else 0, r.position),
        )
    for ch in guild.channels:
        cat = ch.category.name if getattr(ch, "category", None) else None
        conn.execute(
            """INSERT INTO channels (channel_id, name, category, type) VALUES (?,?,?,?)
               ON CONFLICT(channel_id) DO UPDATE SET
                 name=excluded.name, category=excluded.category, type=excluded.type""",
            (ch.id, ch.name, cat, type(ch).__name__),
        )
    for m in guild.members:
        conn.execute(
            """INSERT INTO members (user_id, name, display_name, avatar_url, joined_at, is_bot, left_at)
               VALUES (?,?,?,?,?,?,NULL)
               ON CONFLICT(user_id) DO UPDATE SET
                 name=excluded.name, display_name=excluded.display_name,
                 avatar_url=excluded.avatar_url, left_at=NULL""",
            (m.id, str(m), m.display_name,
             str(m.display_avatar.url) if m.display_avatar else None,
             m.joined_at.isoformat() if m.joined_at else None, int(m.bot)),
        )
        conn.execute("DELETE FROM member_roles WHERE user_id = ?", (m.id,))
        for r in m.roles:
            if r.is_default():
                continue
            conn.execute("INSERT OR IGNORE INTO member_roles (user_id, role_id) VALUES (?, ?)", (m.id, r.id))
    now = now_iso()
    conn.execute(
        """UPDATE voice_sessions SET left_at = ?,
           duration_sec = CAST((julianday(?) - julianday(joined_at)) * 86400 AS INTEGER)
           WHERE left_at IS NULL""",
        (now, now),
    )
    for ch in guild.voice_channels:
        for member in ch.members:
            conn.execute(
                "INSERT INTO voice_sessions (user_id, channel_id, joined_at) VALUES (?, ?, ?)",
                (member.id, ch.id, now),
            )
    conn.commit()


async def _backfill_channel(ch: discord.TextChannel, after: dt.datetime, sem: asyncio.Semaphore):
    async with sem:
        count = 0
        try:
            async for msg in ch.history(after=after, limit=None, oldest_first=True):
                conn = db()
                conn.execute(
                    """INSERT OR REPLACE INTO members (user_id, name, display_name, is_bot)
                       VALUES (?, ?, ?, ?)""",
                    (msg.author.id, str(msg.author), msg.author.display_name, int(msg.author.bot)),
                )
                conn.execute(
                    """INSERT OR IGNORE INTO messages
                       (id, channel_id, author_id, content, created_at, attachment_count)
                       VALUES (?,?,?,?,?,?)""",
                    (msg.id, ch.id, msg.author.id, msg.content,
                     msg.created_at.isoformat(), len(msg.attachments)),
                )
                conn.commit()
                count += 1
            if count:
                print(f"[backfill] #{ch.name}: {count} msgs")
            return count, None
        except discord.Forbidden:
            return 0, "forbidden"
        except Exception as e:
            return 0, repr(e)


def prune_old(window_days: int):
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=window_days)).isoformat()
    conn = db()
    n_msg = conn.execute("DELETE FROM messages WHERE created_at < ?", (cutoff,)).rowcount
    n_vs = conn.execute(
        """DELETE FROM voice_sessions
           WHERE COALESCE(left_at, joined_at) < ? AND left_at IS NOT NULL""",
        (cutoff,)
    ).rowcount
    conn.commit()
    print(f"[prune] removed {n_msg} messages, {n_vs} voice sessions older than {window_days}d.")


async def backfill(guild: discord.Guild, days: int):
    after = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    prune_old(days)
    print(f"[backfill] starting (last {days}d, concurrency={BACKFILL_CONCURRENCY})...")
    sem = asyncio.Semaphore(BACKFILL_CONCURRENCY)
    candidates = []
    for ch in guild.text_channels:
        p = ch.permissions_for(guild.me)
        if p.view_channel and p.read_message_history:
            candidates.append(ch)
    no_perm = len(guild.text_channels) - len(candidates)
    print(f"[backfill] eligible channels: {len(candidates)}, skipped (no perms): {no_perm}")
    results = await asyncio.gather(*[_backfill_channel(ch, after, sem) for ch in candidates])
    total = sum(c for c, _ in results)
    forbidden = sum(1 for _, e in results if e == "forbidden")
    print(f"[backfill] done. {total} messages across {len(candidates) - forbidden} channels.")


@client.event
async def on_ready():
    guild = client.get_guild(GUILD_ID)
    if not guild:
        print(f"WARNING: bot is not in guild {GUILD_ID}.")
        return
    snapshot_guild(guild)
    print(f"Online as {client.user}. {guild.member_count} members, "
          f"{len(guild.roles)} roles, {len(guild.channels)} channels in '{guild.name}'.")
    if BACKFILL_DAYS > 0:
        client.loop.create_task(backfill(guild, BACKFILL_DAYS))
    if not getattr(client, "_voice_resync_started", False):
        client._voice_resync_started = True
        client.loop.create_task(periodic_voice_resync())


@client.event
async def on_guild_join(guild: discord.Guild):
    if guild.id != GUILD_ID:
        return
    print(f"*** Joined target guild '{guild.name}'. Snapshotting + backfilling. ***")
    snapshot_guild(guild)
    if BACKFILL_DAYS > 0:
        client.loop.create_task(backfill(guild, BACKFILL_DAYS))


@client.event
async def on_guild_remove(guild: discord.Guild):
    print(f"*** Removed from guild '{guild.name}' ({guild.id}). ***")


@client.event
async def on_message(msg: discord.Message):
    if msg.guild is None or msg.guild.id != GUILD_ID or msg.author.bot:
        return
    conn = db()
    conn.execute("""INSERT OR REPLACE INTO members (user_id, name, display_name, avatar_url, is_bot)
                     VALUES (?, ?, ?, ?, ?)""",
                 (msg.author.id, str(msg.author), msg.author.display_name,
                  str(msg.author.display_avatar.url) if msg.author.display_avatar else None,
                  int(msg.author.bot)))
    cat = msg.channel.category.name if getattr(msg.channel, "category", None) else None
    conn.execute("""INSERT INTO channels (channel_id, name, category, type) VALUES (?,?,?,?)
                     ON CONFLICT(channel_id) DO UPDATE SET name=excluded.name, category=excluded.category, type=excluded.type""",
                 (msg.channel.id, getattr(msg.channel, "name", None), cat, type(msg.channel).__name__))
    conn.execute("""INSERT OR IGNORE INTO messages (id, channel_id, author_id, content, created_at, attachment_count)
                     VALUES (?,?,?,?,?,?)""",
                 (msg.id, msg.channel.id, msg.author.id, msg.content,
                  msg.created_at.isoformat(), len(msg.attachments)))
    conn.commit()


@client.event
async def on_member_join(m: discord.Member):
    if m.guild.id != GUILD_ID:
        return
    conn = db()
    conn.execute("""INSERT OR REPLACE INTO members
                     (user_id, name, display_name, avatar_url, joined_at, is_bot, left_at)
                     VALUES (?, ?, ?, ?, ?, ?, NULL)""",
                 (m.id, str(m), m.display_name,
                  str(m.display_avatar.url) if m.display_avatar else None,
                  m.joined_at.isoformat() if m.joined_at else None, int(m.bot)))
    conn.commit()


@client.event
async def on_member_remove(m: discord.Member):
    if m.guild.id != GUILD_ID:
        return
    conn = db()
    conn.execute("UPDATE members SET left_at = ? WHERE user_id = ?", (now_iso(), m.id))
    conn.commit()


@client.event
async def on_member_update(before, after):
    if after.guild.id != GUILD_ID:
        return
    if {r.id for r in before.roles} != {r.id for r in after.roles}:
        conn = db()
        conn.execute("DELETE FROM member_roles WHERE user_id = ?", (after.id,))
        for r in after.roles:
            if r.is_default():
                continue
            conn.execute("INSERT OR IGNORE INTO member_roles (user_id, role_id) VALUES (?, ?)", (after.id, r.id))
        conn.commit()


async def periodic_voice_resync():
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            guild = client.get_guild(GUILD_ID)
            if guild is not None:
                now = now_iso()
                live = {(m.id, ch.id) for ch in guild.voice_channels for m in ch.members if not m.bot}
                conn = db()
                open_rows = conn.execute(
                    "SELECT id, user_id, channel_id FROM voice_sessions WHERE left_at IS NULL"
                ).fetchall()
                open_keys = {(r[1], r[2]): r[0] for r in open_rows}
                for key, vs_id in open_keys.items():
                    if key not in live:
                        conn.execute(
                            """UPDATE voice_sessions SET left_at = ?,
                               duration_sec = CAST((julianday(?) - julianday(joined_at)) * 86400 AS INTEGER)
                               WHERE id = ?""",
                            (now, now, vs_id),
                        )
                for key in live - set(open_keys.keys()):
                    uid, cid = key
                    conn.execute(
                        "INSERT INTO voice_sessions (user_id, channel_id, joined_at) VALUES (?,?,?)",
                        (uid, cid, now),
                    )
                conn.commit()
        except Exception as e:
            print(f"[voice-resync] error: {e!r}")
        await asyncio.sleep(300)


@client.event
async def on_voice_state_update(member, before, after):
    if member.guild.id != GUILD_ID or member.bot:
        return
    now = now_iso()
    conn = db()
    conn.execute("""INSERT INTO members (user_id, name, display_name, avatar_url, is_bot)
                     VALUES (?, ?, ?, ?, 0)
                     ON CONFLICT(user_id) DO UPDATE SET
                       name=excluded.name, display_name=excluded.display_name,
                       avatar_url=excluded.avatar_url""",
                 (member.id, str(member), member.display_name,
                  str(member.display_avatar.url) if member.display_avatar else None))
    if before.channel is None and after.channel is not None:
        conn.execute("INSERT INTO voice_sessions (user_id, channel_id, joined_at) VALUES (?,?,?)",
                     (member.id, after.channel.id, now))
    elif before.channel is not None and after.channel is None:
        conn.execute("""UPDATE voice_sessions SET left_at = ?,
                         duration_sec = CAST((julianday(?) - julianday(joined_at)) * 86400 AS INTEGER)
                         WHERE user_id = ? AND left_at IS NULL""",
                     (now, now, member.id))
    elif before.channel and after.channel and before.channel.id != after.channel.id:
        conn.execute("""UPDATE voice_sessions SET left_at = ?,
                         duration_sec = CAST((julianday(?) - julianday(joined_at)) * 86400 AS INTEGER)
                         WHERE user_id = ? AND left_at IS NULL""",
                     (now, now, member.id))
        conn.execute("INSERT INTO voice_sessions (user_id, channel_id, joined_at) VALUES (?,?,?)",
                     (member.id, after.channel.id, now))
    conn.commit()


# ---------- AI agent ----------

def run_agent_query(question: str) -> dict:
    """Run a natural-language question through Claude Code CLI."""
    claude_bin = shutil.which("claude") or "claude"
    env = {k: v for k, v in os.environ.items() if k != "ANTHROPIC_API_KEY"}
    db_abs = str(_WORK_DIR / "tracker.db")
    prompt = (
        f"You are an analytics assistant for the Parsewave Discord server.\n"
        f"Read CLAUDE.md in your working directory for the full schema and context.\n"
        f"The SQLite database is at: {db_abs}\n\n"
        f"Question: {question}\n\n"
        f"Query the database with sqlite3 or python3 as needed — run as many queries as necessary "
        f"to give a complete, accurate answer. Return your answer as clean markdown with real names "
        f"and numbers. Be specific and concise."
    )
    t0 = dt.datetime.now(dt.timezone.utc)
    try:
        result = subprocess.run(
            [claude_bin, "-p", prompt, "--model", CLAUDE_MODEL, "--allowedTools", "Bash"],
            capture_output=True, text=True, timeout=180,
            cwd=str(_WORK_DIR), env=env,
        )
        elapsed = round((dt.datetime.now(dt.timezone.utc) - t0).total_seconds(), 1)
        if result.returncode != 0:
            stderr = (result.stderr or result.stdout or "unknown error").strip()[:800]
            return {"ok": False, "elapsed": elapsed, "model": CLAUDE_MODEL,
                    "answer": f"**Agent error (exit {result.returncode}):**\n```\n{stderr}\n```"}
        answer = result.stdout.strip() or "*(agent returned no output)*"
        return {"ok": True, "elapsed": elapsed, "model": CLAUDE_MODEL, "answer": answer}
    except subprocess.TimeoutExpired:
        return {"ok": False, "elapsed": 180, "model": CLAUDE_MODEL,
                "answer": "**Query timed out** (>180s). Try a more specific question."}
    except FileNotFoundError:
        return {"ok": False, "elapsed": 0, "model": CLAUDE_MODEL,
                "answer": "**`claude` CLI not found in PATH.** Install: `npm install -g @anthropic-ai/claude-code && claude login`"}
    except Exception as e:
        return {"ok": False, "elapsed": 0, "model": CLAUDE_MODEL,
                "answer": f"**Unexpected error:** `{e}`"}


# ---------- internal HTTP API (proxied by Vercel dashboard) ----------

_bot_api = Flask("bot_api")


@_bot_api.route("/internal/ask", methods=["POST"])
def internal_ask():
    data = request.get_json(silent=True) or {}
    question = (data.get("q") or "").strip()
    if not question:
        return jsonify({"error": "no question"}), 400
    if len(question) > 600:
        return jsonify({"error": "question too long (max 600 chars)"}), 400
    acquired = _ask_lock.acquire(timeout=6)
    if not acquired:
        return jsonify({"error": "Another query is already running, try again shortly."}), 429
    try:
        return jsonify(run_agent_query(question))
    finally:
        _ask_lock.release()


@_bot_api.route("/internal/health")
def internal_health():
    return jsonify({"ok": True, "service": "bot"})


def _start_bot_api():
    _bot_api.run(host="0.0.0.0", port=BOT_API_PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    print(f"Connecting to Turso: {TURSO_URL}")
    init_schema()
    print("Schema ready. Starting bot API on port", BOT_API_PORT)
    t = threading.Thread(target=_start_bot_api, daemon=True)
    t.start()
    print("Starting Discord bot...")
    client.run(TOKEN, log_handler=None)
