"""Parsewave Activity Tracker — Discord-styled dashboard for tracking dev/reviewer activity."""
import asyncio
import datetime as dt
import os
import sqlite3
import sys
import threading
from collections import defaultdict
from html import escape

import discord
from flask import Flask, abort, jsonify, render_template_string, request

# ---------- config ----------
TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
GUILD_ID = os.environ.get("DISCORD_GUILD_ID")
DB = os.environ.get("DB_PATH", "tracker.db")
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "60"))
BACKFILL_CONCURRENCY = int(os.environ.get("BACKFILL_CONCURRENCY", "4"))
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "60"))  # retention + max UI window

if not TOKEN or not GUILD_ID:
    sys.exit("Set DISCORD_BOT_TOKEN and DISCORD_GUILD_ID.")
GUILD_ID = int(GUILD_ID)


# ---------- DB ----------
def db():
    c = sqlite3.connect(DB, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


SCHEMA = """
CREATE TABLE IF NOT EXISTS members (
    user_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    display_name TEXT,
    avatar_url TEXT,
    joined_at TEXT,
    left_at TEXT,
    is_bot INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS roles (
    role_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    color INTEGER,
    position INTEGER
);
CREATE TABLE IF NOT EXISTS member_roles (
    user_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    PRIMARY KEY (user_id, role_id)
);
CREATE TABLE IF NOT EXISTS channels (
    channel_id INTEGER PRIMARY KEY,
    name TEXT,
    category TEXT,
    type TEXT
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    content TEXT,
    created_at TEXT NOT NULL,
    attachment_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_msg_author ON messages(author_id);
CREATE INDEX IF NOT EXISTS idx_msg_created ON messages(created_at);
CREATE INDEX IF NOT EXISTS idx_msg_channel ON messages(channel_id);
CREATE INDEX IF NOT EXISTS idx_msg_author_created ON messages(author_id, created_at);

CREATE TABLE IF NOT EXISTS voice_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    joined_at TEXT NOT NULL,
    left_at TEXT,
    duration_sec INTEGER
);
CREATE INDEX IF NOT EXISTS idx_vs_user ON voice_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_vs_joined ON voice_sessions(joined_at);
CREATE INDEX IF NOT EXISTS idx_vs_open ON voice_sessions(left_at) WHERE left_at IS NULL;
"""

with db() as _c:
    _c.executescript(SCHEMA)


def now_iso():
    return discord.utils.utcnow().isoformat()


# ---------- Discord bot ----------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.voice_states = True
client = discord.Client(intents=intents)


def snapshot_guild(guild: discord.Guild):
    with db() as c:
        for r in guild.roles:
            c.execute(
                """INSERT INTO roles (role_id, name, color, position) VALUES (?,?,?,?)
                   ON CONFLICT(role_id) DO UPDATE SET
                     name=excluded.name, color=excluded.color, position=excluded.position""",
                (r.id, r.name, r.color.value if r.color else 0, r.position),
            )
        for ch in guild.channels:
            cat = ch.category.name if getattr(ch, "category", None) else None
            c.execute(
                """INSERT INTO channels (channel_id, name, category, type) VALUES (?,?,?,?)
                   ON CONFLICT(channel_id) DO UPDATE SET
                     name=excluded.name, category=excluded.category, type=excluded.type""",
                (ch.id, ch.name, cat, type(ch).__name__),
            )
        for m in guild.members:
            c.execute(
                """INSERT INTO members (user_id, name, display_name, avatar_url, joined_at, is_bot, left_at)
                   VALUES (?,?,?,?,?,?,NULL)
                   ON CONFLICT(user_id) DO UPDATE SET
                     name=excluded.name, display_name=excluded.display_name,
                     avatar_url=excluded.avatar_url, left_at=NULL""",
                (m.id, str(m), m.display_name,
                 str(m.display_avatar.url) if m.display_avatar else None,
                 m.joined_at.isoformat() if m.joined_at else None, int(m.bot)),
            )
            c.execute("DELETE FROM member_roles WHERE user_id = ?", (m.id,))
            for r in m.roles:
                if r.is_default():
                    continue
                c.execute("INSERT OR IGNORE INTO member_roles (user_id, role_id) VALUES (?, ?)", (m.id, r.id))
        now = now_iso()
        c.execute(
            """UPDATE voice_sessions SET left_at = ?,
               duration_sec = CAST((julianday(?) - julianday(joined_at)) * 86400 AS INTEGER)
               WHERE left_at IS NULL""",
            (now, now),
        )
        for ch in guild.voice_channels:
            for member in ch.members:
                c.execute(
                    "INSERT INTO voice_sessions (user_id, channel_id, joined_at) VALUES (?, ?, ?)",
                    (member.id, ch.id, now),
                )


async def _backfill_channel(ch: discord.TextChannel, after: dt.datetime, sem: asyncio.Semaphore):
    async with sem:
        count = 0
        try:
            async for msg in ch.history(after=after, limit=None, oldest_first=True):
                with db() as c:
                    c.execute(
                        """INSERT OR REPLACE INTO members (user_id, name, display_name, is_bot)
                           VALUES (?, ?, ?, ?)""",
                        (msg.author.id, str(msg.author), msg.author.display_name, int(msg.author.bot)),
                    )
                    c.execute(
                        """INSERT OR IGNORE INTO messages
                           (id, channel_id, author_id, content, created_at, attachment_count)
                           VALUES (?,?,?,?,?,?)""",
                        (msg.id, ch.id, msg.author.id, msg.content,
                         msg.created_at.isoformat(), len(msg.attachments)),
                    )
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
    with db() as c:
        n_msg = c.execute("DELETE FROM messages WHERE created_at < ?", (cutoff,)).rowcount
        n_vs = c.execute(
            """DELETE FROM voice_sessions
               WHERE COALESCE(left_at, joined_at) < ? AND left_at IS NOT NULL""",
            (cutoff,)
        ).rowcount
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
    with db() as c:
        c.execute("""INSERT OR REPLACE INTO members (user_id, name, display_name, avatar_url, is_bot)
                     VALUES (?, ?, ?, ?, ?)""",
                  (msg.author.id, str(msg.author), msg.author.display_name,
                   str(msg.author.display_avatar.url) if msg.author.display_avatar else None,
                   int(msg.author.bot)))
        cat = msg.channel.category.name if getattr(msg.channel, "category", None) else None
        c.execute("""INSERT INTO channels (channel_id, name, category, type) VALUES (?,?,?,?)
                     ON CONFLICT(channel_id) DO UPDATE SET name=excluded.name, category=excluded.category, type=excluded.type""",
                  (msg.channel.id, getattr(msg.channel, "name", None), cat, type(msg.channel).__name__))
        c.execute("""INSERT OR IGNORE INTO messages (id, channel_id, author_id, content, created_at, attachment_count)
                     VALUES (?,?,?,?,?,?)""",
                  (msg.id, msg.channel.id, msg.author.id, msg.content,
                   msg.created_at.isoformat(), len(msg.attachments)))


@client.event
async def on_member_join(m: discord.Member):
    if m.guild.id != GUILD_ID:
        return
    with db() as c:
        c.execute("""INSERT OR REPLACE INTO members
                     (user_id, name, display_name, avatar_url, joined_at, is_bot, left_at)
                     VALUES (?, ?, ?, ?, ?, ?, NULL)""",
                  (m.id, str(m), m.display_name,
                   str(m.display_avatar.url) if m.display_avatar else None,
                   m.joined_at.isoformat() if m.joined_at else None, int(m.bot)))


@client.event
async def on_member_remove(m: discord.Member):
    if m.guild.id != GUILD_ID:
        return
    with db() as c:
        c.execute("UPDATE members SET left_at = ? WHERE user_id = ?", (now_iso(), m.id))


@client.event
async def on_member_update(before, after):
    if after.guild.id != GUILD_ID:
        return
    if {r.id for r in before.roles} != {r.id for r in after.roles}:
        with db() as c:
            c.execute("DELETE FROM member_roles WHERE user_id = ?", (after.id,))
            for r in after.roles:
                if r.is_default():
                    continue
                c.execute("INSERT OR IGNORE INTO member_roles (user_id, role_id) VALUES (?, ?)", (after.id, r.id))


@client.event
async def on_voice_state_update(member, before, after):
    if member.guild.id != GUILD_ID or member.bot:
        return
    now = now_iso()
    with db() as c:
        # ensure member exists with up-to-date info
        c.execute("""INSERT INTO members (user_id, name, display_name, avatar_url, is_bot)
                     VALUES (?, ?, ?, ?, 0)
                     ON CONFLICT(user_id) DO UPDATE SET
                       name=excluded.name, display_name=excluded.display_name,
                       avatar_url=excluded.avatar_url""",
                  (member.id, str(member), member.display_name,
                   str(member.display_avatar.url) if member.display_avatar else None))
        if before.channel is None and after.channel is not None:
            c.execute("INSERT INTO voice_sessions (user_id, channel_id, joined_at) VALUES (?,?,?)",
                      (member.id, after.channel.id, now))
        elif before.channel is not None and after.channel is None:
            c.execute("""UPDATE voice_sessions SET left_at = ?,
                         duration_sec = CAST((julianday(?) - julianday(joined_at)) * 86400 AS INTEGER)
                         WHERE user_id = ? AND left_at IS NULL""",
                      (now, now, member.id))
        elif before.channel and after.channel and before.channel.id != after.channel.id:
            c.execute("""UPDATE voice_sessions SET left_at = ?,
                         duration_sec = CAST((julianday(?) - julianday(joined_at)) * 86400 AS INTEGER)
                         WHERE user_id = ? AND left_at IS NULL""",
                      (now, now, member.id))
            c.execute("INSERT INTO voice_sessions (user_id, channel_id, joined_at) VALUES (?,?,?)",
                      (member.id, after.channel.id, now))


# ---------- helpers ----------
def humanize(iso):
    if not iso:
        return "never"
    try:
        ts = dt.datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return iso
    s = int((dt.datetime.now(dt.timezone.utc) - ts).total_seconds())
    if s < 60: return f"{s}s ago"
    if s < 3600: return f"{s // 60}m ago"
    if s < 86400: return f"{s // 3600}h ago"
    if s < 86400 * 7: return f"{s // 86400}d ago"
    if s < 86400 * 30: return f"{s // (86400 * 7)}w ago"
    return f"{s // (86400 * 30)}mo ago"


def parse_iso(s):
    if not s:
        return None
    try:
        ts = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        return ts
    except ValueError:
        return None


def presence_status(last_iso):
    """Return (label, css_class) based on last activity."""
    if not last_iso:
        return ("inactive", "off")
    ts = parse_iso(last_iso)
    if not ts:
        return ("inactive", "off")
    sec = (dt.datetime.now(dt.timezone.utc) - ts).total_seconds()
    if sec < 60 * 30:   return ("active",  "online")
    if sec < 3600 * 8:  return ("working", "online")
    if sec < 86400:     return ("recent",  "idle")
    if sec < 86400 * 7: return ("away",    "away")
    return ("ghost", "off")


def color_to_hex(c):
    if not c or c == 0:
        return "#b5bac1"
    return f"#{c:06x}"


# ---------- data ----------
def build_data(days, role_id, channel_id, search):
    now = dt.datetime.now(dt.timezone.utc)
    cutoff = (now - dt.timedelta(days=days)).isoformat()
    last24h = (now - dt.timedelta(hours=24)).isoformat()
    last8h = (now - dt.timedelta(hours=8)).isoformat()

    with db() as c:
        c.row_factory = sqlite3.Row

        if role_id:
            members = c.execute("""SELECT m.* FROM members m
                                   JOIN member_roles mr ON mr.user_id = m.user_id
                                   WHERE m.is_bot = 0 AND m.left_at IS NULL AND mr.role_id = ?""",
                                (role_id,)).fetchall()
        else:
            members = c.execute("SELECT * FROM members WHERE is_bot = 0 AND left_at IS NULL").fetchall()

        if search:
            s = search.lower()
            members = [m for m in members if s in (m["name"] or "").lower() or s in (m["display_name"] or "").lower()]

        roles_by_user = defaultdict(list)
        for r in c.execute("""SELECT mr.user_id, r.name, r.color, r.position
                              FROM member_roles mr JOIN roles r ON r.role_id = mr.role_id
                              WHERE r.name != '@everyone' ORDER BY r.position DESC""").fetchall():
            roles_by_user[r["user_id"]].append(
                {"name": r["name"], "color": color_to_hex(r["color"]), "position": r["position"]}
            )

        ch_clause = "AND channel_id = ?" if channel_id else ""
        ch_params = [channel_id] if channel_id else []

        msg_stats = {}
        for r in c.execute(
            f"""SELECT author_id, COUNT(*) as cnt, MAX(created_at) as last_at,
                       SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS cnt_24h,
                       SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS cnt_8h
                FROM messages WHERE created_at >= ? {ch_clause}
                GROUP BY author_id""",
            [last24h, last8h, cutoff, *ch_params]
        ).fetchall():
            msg_stats[r["author_id"]] = r

        last_msg = {}
        for r in c.execute(
            f"""SELECT author_id, content, created_at, channel_id FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY id DESC) AS rn
                  FROM messages WHERE created_at >= ? {ch_clause}
                ) WHERE rn = 1""",
            [cutoff, *ch_params]
        ).fetchall():
            last_msg[r["author_id"]] = dict(r)

        top_channel = {}
        for r in c.execute(
            f"""SELECT author_id, channel_id, cnt FROM (
                  SELECT author_id, channel_id, COUNT(*) AS cnt,
                         ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY COUNT(*) DESC) AS rn
                  FROM messages WHERE created_at >= ? {ch_clause}
                  GROUP BY author_id, channel_id
                ) WHERE rn = 1""",
            [cutoff, *ch_params]
        ).fetchall():
            top_channel[r["author_id"]] = (r["channel_id"], r["cnt"])

        channel_names = {r["channel_id"]: r["name"] for r in c.execute("SELECT channel_id, name FROM channels").fetchall()}

        voice_stats = {}
        for r in c.execute(
            """SELECT user_id,
                      SUM(COALESCE(duration_sec,
                          CAST((julianday('now') - julianday(joined_at)) * 86400 AS INTEGER))) AS sec,
                      MAX(joined_at) AS last_join
               FROM voice_sessions WHERE joined_at >= ?
               GROUP BY user_id""",
            [cutoff]
        ).fetchall():
            voice_stats[r["user_id"]] = r

        ts_by_user = defaultdict(list)
        for r in c.execute(
            """SELECT user_id, ts FROM (
                 SELECT author_id AS user_id, created_at AS ts FROM messages WHERE created_at >= ?
                 UNION ALL
                 SELECT user_id, joined_at AS ts FROM voice_sessions WHERE joined_at >= ?
               ) ORDER BY user_id, ts""",
            [last24h, last24h]
        ).fetchall():
            ts_by_user[r["user_id"]].append(r["ts"])

        # daily activity for sparkline (last 7 days)
        spark_cutoff = (now - dt.timedelta(days=7)).isoformat()
        spark_raw = defaultdict(dict)
        for r in c.execute(
            f"""SELECT author_id, DATE(created_at) AS day, COUNT(*) AS cnt
                FROM messages WHERE created_at >= ? {ch_clause}
                GROUP BY author_id, DATE(created_at)""",
            [spark_cutoff, *ch_params]
        ).fetchall():
            spark_raw[r["author_id"]][r["day"]] = r["cnt"]
        days_list = [(now - dt.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6, -1, -1)]

    cutoff_dt = parse_iso(last24h)
    rows = []
    for m in members:
        uid = m["user_id"]
        ms = msg_stats.get(uid)
        vs = voice_stats.get(uid)
        lm = last_msg.get(uid)
        cnt = ms["cnt"] if ms else 0
        cnt_24h = ms["cnt_24h"] if ms else 0
        cnt_8h = ms["cnt_8h"] if ms else 0
        voice_sec = (vs["sec"] or 0) if vs else 0
        voice_min = voice_sec // 60

        moments = [cutoff_dt]
        for t in ts_by_user.get(uid, []):
            pt = parse_iso(t)
            if pt:
                moments.append(pt)
        moments.append(now)
        max_gap_h = 0
        for i in range(1, len(moments)):
            g = (moments[i] - moments[i - 1]).total_seconds() / 3600
            if g > max_gap_h:
                max_gap_h = g

        last_at = ms["last_at"] if ms else None
        if vs and vs["last_join"] and (not last_at or vs["last_join"] > last_at):
            last_at = vs["last_join"]

        tc = top_channel.get(uid)
        top_ch_name = channel_names.get(tc[0]) if tc else None
        top_ch_pct = round(tc[1] / cnt * 100) if tc and cnt else 0

        spark = [spark_raw[uid].get(d, 0) for d in days_list]
        spark_max = max(spark) if spark and max(spark) else 1

        pres_label, pres_class = presence_status(last_at)

        score = cnt + voice_min / 10.0
        top_role = (roles_by_user.get(uid) or [{"name": None, "color": "#b5bac1"}])[0]

        rows.append({
            "user_id": uid,
            "name": m["display_name"] or m["name"],
            "username": m["name"],
            "avatar": m["avatar_url"],
            "initial": (m["display_name"] or m["name"] or "?")[0].upper(),
            "roles": roles_by_user.get(uid, [])[:3],
            "top_role": top_role,
            "msg_count": cnt,
            "msg_24h": cnt_24h,
            "msg_8h": cnt_8h,
            "voice_min": voice_min,
            "last_msg_content": (lm["content"] or "") if lm else "",
            "last_msg_channel": channel_names.get(lm["channel_id"]) if lm else None,
            "last_msg_at": humanize(lm["created_at"]) if lm else None,
            "last_seen": humanize(last_at),
            "last_seen_iso": last_at,
            "score": score,
            "sleep_gap_h": round(max_gap_h, 1),
            "is_active": cnt > 0 or voice_min > 0,
            "working_now": cnt_8h > 0,
            "took_break": max_gap_h >= 8,
            "top_channel_name": top_ch_name,
            "top_channel_pct": top_ch_pct,
            "spark": spark,
            "spark_max": spark_max,
            "presence_label": pres_label,
            "presence_class": pres_class,
        })
    return rows


def sidebar_data(project_window_days=7, project_n=12):
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=project_window_days)).isoformat()
    with db() as c:
        c.row_factory = sqlite3.Row
        roles = c.execute("""SELECT r.role_id, r.name, r.color, r.position,
                                    COUNT(mr.user_id) AS cnt
                             FROM roles r
                             LEFT JOIN member_roles mr ON mr.role_id = r.role_id
                             LEFT JOIN members m ON m.user_id = mr.user_id
                                                AND m.is_bot = 0 AND m.left_at IS NULL
                             WHERE r.name != '@everyone'
                             GROUP BY r.role_id
                             HAVING cnt > 0
                             ORDER BY r.position DESC""").fetchall()
        channels = c.execute("""SELECT channel_id, name, category FROM channels
                                WHERE type LIKE '%TextChannel%' OR type LIKE '%Thread%'
                                ORDER BY category, name""").fetchall()
        projects = c.execute("""SELECT m.channel_id, ch.name, ch.category,
                                       COUNT(*) AS cnt
                                FROM messages m
                                LEFT JOIN channels ch ON ch.channel_id = m.channel_id
                                WHERE m.created_at >= ?
                                  AND ch.name NOT LIKE 'ticket-%'
                                  AND ch.name NOT LIKE '%-comp'
                                  AND ch.name NOT LIKE 'contributor-%'
                                GROUP BY m.channel_id
                                ORDER BY cnt DESC
                                LIMIT ?""", (cutoff, project_n)).fetchall()
        guild_meta = {
            "name": "Parsewave (Developer)",
            "total_messages": c.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
            "total_members": c.execute("SELECT COUNT(*) FROM members WHERE is_bot=0 AND left_at IS NULL").fetchone()[0],
        }
    return {
        "roles": [dict(r) for r in roles],
        "channels": [dict(ch) for ch in channels],
        "projects": [dict(p) for p in projects],
        "meta": guild_meta,
    }


def top_projects_with_contributors(days=7, n=6):
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)).isoformat()
    out = []
    with db() as c:
        c.row_factory = sqlite3.Row
        rows = c.execute("""SELECT m.channel_id, ch.name, ch.category,
                                   COUNT(*) AS cnt,
                                   COUNT(DISTINCT m.author_id) AS author_cnt
                            FROM messages m
                            LEFT JOIN channels ch ON ch.channel_id = m.channel_id
                            WHERE m.created_at >= ?
                              AND ch.name NOT LIKE 'ticket-%'
                              AND ch.name NOT LIKE '%-comp'
                              AND ch.name NOT LIKE 'contributor-%'
                            GROUP BY m.channel_id
                            ORDER BY cnt DESC
                            LIMIT ?""", (cutoff, n)).fetchall()
        for r in rows:
            top = c.execute("""SELECT m.author_id, mb.name, mb.display_name, mb.avatar_url, COUNT(*) AS cnt
                               FROM messages m
                               JOIN members mb ON mb.user_id = m.author_id
                               WHERE m.channel_id = ? AND m.created_at >= ? AND mb.is_bot = 0
                               GROUP BY m.author_id ORDER BY cnt DESC LIMIT 4""",
                            (r["channel_id"], cutoff)).fetchall()
            out.append({
                "channel_id": r["channel_id"],
                "name": r["name"],
                "category": r["category"],
                "cnt": r["cnt"],
                "author_cnt": r["author_cnt"],
                "top_contributors": [{
                    "user_id": t["author_id"],
                    "name": t["display_name"] or t["name"],
                    "avatar": t["avatar_url"],
                    "initial": (t["display_name"] or t["name"] or "?")[0].upper(),
                    "cnt": t["cnt"],
                } for t in top],
            })
    return out


def live_voice():
    with db() as c:
        c.row_factory = sqlite3.Row
        return [dict(r) for r in c.execute("""
            SELECT vs.user_id, vs.channel_id, vs.joined_at,
                   m.name, m.display_name, m.avatar_url,
                   ch.name AS channel_name
            FROM voice_sessions vs
            JOIN members m ON m.user_id = vs.user_id AND m.is_bot = 0
            LEFT JOIN channels ch ON ch.channel_id = vs.channel_id
            WHERE vs.left_at IS NULL
            ORDER BY vs.joined_at
        """).fetchall()]


def voice_overview(days=7):
    """Live VC state grouped by channel + per-channel stats over window."""
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)).isoformat()
    with db() as c:
        c.row_factory = sqlite3.Row
        live = c.execute("""
            SELECT vs.user_id, vs.channel_id, vs.joined_at,
                   m.name, m.display_name, m.avatar_url,
                   ch.name AS channel_name
            FROM voice_sessions vs
            JOIN members m ON m.user_id = vs.user_id AND m.is_bot = 0
            LEFT JOIN channels ch ON ch.channel_id = vs.channel_id
            WHERE vs.left_at IS NULL
            ORDER BY vs.joined_at
        """).fetchall()
        per_channel_stats = c.execute("""
            SELECT vs.channel_id, ch.name,
                   COUNT(*) AS session_cnt,
                   COUNT(DISTINCT vs.user_id) AS unique_users,
                   SUM(COALESCE(vs.duration_sec,
                       CAST((julianday('now') - julianday(vs.joined_at)) * 86400 AS INTEGER))
                   ) AS total_sec
            FROM voice_sessions vs
            LEFT JOIN channels ch ON ch.channel_id = vs.channel_id
            WHERE vs.joined_at >= ?
            GROUP BY vs.channel_id
            ORDER BY total_sec DESC
        """, (cutoff,)).fetchall()
        top_voice_users = c.execute("""
            SELECT vs.user_id, m.name, m.display_name, m.avatar_url,
                   COUNT(*) AS sessions,
                   SUM(COALESCE(vs.duration_sec,
                       CAST((julianday('now') - julianday(vs.joined_at)) * 86400 AS INTEGER))
                   ) AS total_sec
            FROM voice_sessions vs
            JOIN members m ON m.user_id = vs.user_id AND m.is_bot = 0
            WHERE vs.joined_at >= ?
            GROUP BY vs.user_id
            ORDER BY total_sec DESC
            LIMIT 20
        """, (cutoff,)).fetchall()
    live_by_channel = defaultdict(list)
    for r in live:
        live_by_channel[r["channel_id"]].append(dict(r))
    return {
        "live_by_channel": {k: v for k, v in live_by_channel.items()},
        "per_channel_stats": [dict(r) for r in per_channel_stats],
        "top_voice_users": [dict(r) for r in top_voice_users],
        "live_count": len(live),
    }


# ---------- Flask ----------
app = Flask(__name__)

CSS = """
@import url('https://rsms.me/inter/inter.css');
:root {
  /* Discord's exact 2024+ palette */
  --bg-tertiary:  #1e1f22;   /* deepest — server list / dividers */
  --bg-secondary: #2b2d31;   /* sidebar */
  --bg-primary:   #313338;   /* main chat area */
  --bg-floating:  #2b2d31;
  --bg-hover:     #35363c;
  --bg-active:    #404249;
  --bg-modifier-selected: rgba(78, 80, 88, 0.6);
  --border:       #1e1f22;
  --interactive-normal: #b5bac1;
  --interactive-hover:  #dbdee1;
  --interactive-active: #ffffff;
  --interactive-muted:  #4e5058;
  --header-primary:   #f2f3f5;
  --header-secondary: #b5bac1;
  --text-normal:  #dbdee1;
  --text-muted:   #949ba4;
  --text-link:    #00a8fc;
  --brand:        #5865f2;
  --brand-hover:  #4752c4;
  --green:        #23a55a;
  --yellow:       #f0b232;
  --red:          #f23f43;
  --grey:         #80848e;
  /* legacy aliases used in template */
  --bg-darker: var(--bg-tertiary);
  --bg-dark:   var(--bg-secondary);
  --bg:        var(--bg-secondary);
  --bg-elev:   var(--bg-primary);
  --text:      var(--header-primary);
  --muted:     var(--header-secondary);
  --dim:       var(--text-muted);
  --accent:    var(--brand);
  --accent-h:  var(--brand-hover);
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; height: 100%; background: var(--bg-primary); color: var(--text-normal);
  font-family: 'Inter', 'gg sans', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  font-feature-settings: 'cv11', 'ss01', 'ss03';
  font-size: 14px; line-height: 1.4; -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility; }
@supports (font-variation-settings: normal) {
  html, body { font-family: 'Inter var', 'gg sans', -apple-system, BlinkMacSystemFont, sans-serif; }
}
a { color: inherit; text-decoration: none; }
button, select, input { font-family: inherit; font-size: 14px; color: var(--text); }
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #1a1b1e; border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: #2b2d31; }

.app { display: flex; min-height: 100vh; }

/* Sidebar */
.sidebar { width: 248px; flex: 0 0 248px; background: var(--bg); border-right: 1px solid var(--border);
  display: flex; flex-direction: column; }
.sidebar-header { padding: 14px 16px; box-shadow: 0 1px 0 var(--border); display: flex; align-items: center; gap: 10px;
  position: sticky; top: 0; background: var(--bg); z-index: 2; }
.server-icon { width: 38px; height: 38px; border-radius: 14px; background: var(--accent); color: #fff;
  display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 18px;
  flex-shrink: 0; }
.server-name { font-weight: 700; font-size: 16px; }
.server-sub { color: var(--dim); font-size: 12px; margin-top: 1px; }
.sidebar-scroll { flex: 1; overflow-y: auto; padding: 12px 8px 24px; }
.nav-section { margin-bottom: 18px; }
.nav-section-title { color: var(--dim); font-size: 12px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .03em; padding: 8px 10px 4px; }
.nav-item { display: flex; align-items: center; gap: 8px; padding: 7px 10px; border-radius: 6px;
  color: var(--muted); cursor: pointer; font-size: 14px; margin-bottom: 2px; user-select: none; }
.nav-item:hover { background: var(--bg-hover); color: var(--text); }
.nav-item.active { background: rgba(88,101,242,.18); color: #fff; font-weight: 600; }
.nav-item.active::before { content: ''; position: absolute; left: 0; width: 3px; height: 18px; background: #fff;
  border-radius: 0 4px 4px 0; }
.nav-item { position: relative; }
.nav-item .icon { width: 18px; text-align: center; opacity: .9; flex-shrink: 0; }
.nav-item .hash { color: var(--dim); font-weight: 500; }
.nav-item .count { margin-left: auto; color: var(--dim); font-size: 12px; font-variant-numeric: tabular-nums;
  background: var(--bg-hover); padding: 1px 7px; border-radius: 10px; }
.nav-item.active .count { background: rgba(255,255,255,.1); color: #fff; }
.nav-item .role-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.nav-item .label { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* Main */
.main { flex: 1; min-width: 0; display: flex; flex-direction: column; }
.topbar { display: flex; gap: 10px; align-items: center; padding: 14px 24px;
  border-bottom: 1px solid var(--border); background: var(--bg-elev); position: sticky; top: 0; z-index: 5; }
.topbar h1 { margin: 0; font-size: 20px; font-weight: 700; }
.topbar .subtitle { color: var(--muted); font-size: 13px; margin-left: 4px; }
.spacer { flex: 1; }
.topbar select, .topbar input { background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
  color: var(--text); padding: 7px 10px; outline: none; }
.topbar input:focus, .topbar select:focus { border-color: var(--accent); }
.topbar select:hover { background: var(--bg-hover); }

.content { padding: 20px 24px 60px; flex: 1; }
.section-title { color: var(--muted); font-size: 13px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .04em; margin: 22px 0 10px; display: flex; align-items: center; gap: 8px; }
.section-title .live { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: var(--green);
  box-shadow: 0 0 0 4px rgba(35, 165, 90, .25); animation: pulse 2s infinite; }

/* VC strip */
.vc-strip { background: var(--bg); border: 1px solid var(--border);
  border-radius: 10px; padding: 12px; margin-bottom: 12px; }
.vc-list { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 8px; }
.vc-card { display: flex; align-items: center; gap: 10px; background: var(--bg-hover); border-radius: 8px;
  padding: 8px 12px; transition: background .12s; }
.vc-card:hover { background: #45474e; }
.vc-card .avatar { width: 32px; height: 32px; }
.vc-name { font-weight: 600; font-size: 14px; }
.vc-meta { color: var(--muted); font-size: 12px; }

@keyframes pulse { 0%, 100% { opacity: 1 } 50% { opacity: .5 } }

/* Stat cards */
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-bottom: 6px; }
.stat { background: var(--bg); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }
.stat .v { font-size: 26px; font-weight: 700; line-height: 1.15; }
.stat .l { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; margin-top: 4px; }
.stat.green .v { color: var(--green); } .stat.yellow .v { color: var(--yellow); } .stat.red .v { color: var(--red); }

/* Project cards */
.proj-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 12px; }
.proj-card { background: var(--bg); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px;
  transition: border-color .15s, transform .12s; }
.proj-card:hover { border-color: var(--accent); transform: translateY(-1px); cursor: pointer; }
.proj-card .name { font-weight: 700; font-size: 15px; margin-bottom: 2px; }
.proj-card .name .hash { color: var(--dim); margin-right: 2px; }
.proj-card .meta { color: var(--muted); font-size: 12px; margin-bottom: 12px; }
.proj-card .meta .pill { display: inline-block; background: var(--bg-hover); padding: 1px 7px; border-radius: 10px;
  margin-right: 5px; font-weight: 600; color: var(--text); }
.proj-card .contribs { display: flex; flex-direction: column; gap: 3px; }
.contrib-row { display: flex; align-items: center; gap: 8px; padding: 5px 6px; border-radius: 6px;
  font-size: 13px; color: var(--muted); }
.contrib-row:hover { background: var(--bg-hover); color: var(--text); }
.contrib-row .avatar { width: 22px; height: 22px; font-size: 11px; }
.contrib-row .name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.contrib-row .cnt { font-variant-numeric: tabular-nums; color: var(--dim); font-size: 12px; }

/* Member table */
.panel { background: var(--bg); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
.panel-header { display: flex; align-items: center; padding: 14px 18px; border-bottom: 1px solid var(--border);
  color: var(--muted); font-size: 13px; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; }
.panel-header .count { margin-left: 8px; color: var(--dim); font-weight: 500; }
.panel-header a { margin-left: auto; color: var(--accent); font-size: 13px; font-weight: 600;
  text-transform: none; letter-spacing: 0; }
table.members { width: 100%; border-collapse: collapse; }
table.members th, table.members td { padding: 12px 16px; text-align: left; border-bottom: 1px solid var(--border);
  vertical-align: middle; font-size: 14px; }
table.members thead th { color: var(--dim); font-weight: 600; font-size: 11px; text-transform: uppercase;
  letter-spacing: .05em; position: sticky; top: 62px; background: var(--bg); z-index: 2; }
table.members tbody tr:hover { background: var(--bg-hover); cursor: pointer; }
table.members tbody tr:last-child td { border-bottom: 0; }
.num { text-align: right; font-variant-numeric: tabular-nums; }

.member-cell { display: flex; align-items: center; gap: 12px; }
.avatar { width: 36px; height: 36px; border-radius: 50%; background: var(--bg-hover); display: inline-flex;
  align-items: center; justify-content: center; color: #fff; font-weight: 700; font-size: 14px;
  position: relative; flex-shrink: 0; overflow: hidden; }
.avatar img { width: 100%; height: 100%; object-fit: cover; }
.avatar .status { position: absolute; bottom: -2px; right: -2px; width: 14px; height: 14px; border-radius: 50%;
  border: 3px solid var(--bg); }
.status.online { background: var(--green); } .status.idle { background: var(--yellow); }
.status.away { background: var(--grey); } .status.off { background: #4e5058; }

.member-name { font-weight: 600; font-size: 14.5px; }
.member-handle { color: var(--dim); font-size: 12px; }

.chip { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; margin-right: 4px;
  font-weight: 500; background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.04); }
.role-chip { font-weight: 600; }
.role-chip .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 5px;
  vertical-align: middle; }

.snippet { color: var(--muted); font-size: 13px; max-width: 320px; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap; }
.snippet-meta { color: var(--dim); font-size: 12px; margin-top: 2px; }

.flag { display: inline-block; padding: 3px 8px; border-radius: 5px; font-size: 12px; font-weight: 600; }
.flag.ok { background: rgba(35, 165, 90, .18); color: #3fce6f; }
.flag.warn { background: rgba(240, 178, 50, .18); color: #f5c252; }
.flag.bad { background: rgba(242, 63, 67, .22); color: #ff5b5e; }

.spark { display: inline-flex; align-items: flex-end; gap: 2px; height: 26px; }
.spark .bar { width: 6px; background: var(--accent); border-radius: 1px; opacity: .9; }
.spark .bar.zero { background: var(--bg-hover); height: 3px !important; opacity: 1; }

.empty { text-align: center; padding: 60px 20px; color: var(--muted); }
.empty h3 { margin: 0 0 8px; color: var(--text); font-weight: 600; }

/* Member detail page */
.profile { padding: 24px; max-width: 1100px; margin: 0 auto; }
.profile-header { display: flex; gap: 20px; align-items: center; margin-bottom: 22px; }
.profile-header .avatar { width: 88px; height: 88px; font-size: 32px; }
.profile-header h1 { margin: 0; font-size: 26px; }
.profile-header .handle { color: var(--muted); font-size: 14px; }
.profile-header .roles-strip { display: flex; gap: 5px; flex-wrap: wrap; margin-top: 6px; }
.back-link { color: var(--accent); font-size: 13px; padding-bottom: 8px; display: inline-block; font-weight: 600; }
.section { background: var(--bg); border: 1px solid var(--border); border-radius: 10px; padding: 18px;
  margin-bottom: 14px; }
.section h2 { margin: 0 0 12px; font-size: 13px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .04em; color: var(--muted); }
.bar-chart { display: flex; align-items: flex-end; gap: 4px; height: 140px; padding: 6px 0; }
.bar-chart .bar { flex: 1; background: var(--accent); border-radius: 2px; min-height: 2px; position: relative;
  transition: background .12s; }
.bar-chart .bar:hover { background: #7984f8; }
.day-row { display: flex; gap: 4px; }
.day-row .col { flex: 1; }
.msg-row { padding: 10px 12px; border-bottom: 1px solid var(--border); }
.msg-row:last-child { border-bottom: 0; }
.msg-row .meta { color: var(--dim); font-size: 12px; margin-bottom: 3px; }
.msg-row .body { font-size: 14px; white-space: pre-wrap; word-break: break-word; }
"""

LAYOUT = """
<!doctype html>
<html><head>
<meta charset="utf-8">
<title>{{ page_title }} · Parsewave Tracker</title>
<style>{{ css }}</style>
</head>
<body>
<div class="app">
  <aside class="sidebar">
    <a class="sidebar-header" href="/" style="text-decoration:none;color:inherit;">
      <div class="server-icon">P</div>
      <div>
        <div class="server-name">{{ sidebar.meta.name }}</div>
        <div class="server-sub">{{ sidebar.meta.total_members }} members · {{ sidebar.meta.total_messages }} msgs</div>
      </div>
    </a>
    <div class="sidebar-scroll">
      <div class="nav-section">
        <div class="nav-section-title">Overview</div>
        <a class="nav-item {% if route == 'home' %}active{% endif %}" href="/"><span class="icon">🏠</span><span class="label">Home</span></a>
        <a class="nav-item {% if route == 'members' and not role_id and show == 'all' %}active{% endif %}" href="/members?days={{ days }}"><span class="icon">👥</span><span class="label">All members</span> <span class="count">{{ sidebar.meta.total_members }}</span></a>
        <a class="nav-item {% if show == 'working' %}active{% endif %}"  href="/members?days={{ days }}&show=working"><span class="icon">💼</span><span class="label">Working (8h)</span> <span class="count">{{ counts.working }}</span></a>
        <a class="nav-item {% if show == 'active' %}active{% endif %}"   href="/members?days={{ days }}&show=active"><span class="icon">🟢</span><span class="label">Active in window</span> <span class="count">{{ counts.active }}</span></a>
        <a class="nav-item {% if route == 'voice' %}active{% endif %}" href="/voice?days={{ days }}"><span class="icon">🎙</span><span class="label">Voice channels</span> <span class="count">{{ counts.voice_live }}</span></a>
        <a class="nav-item {% if show == 'nosleep' %}active{% endif %}"  href="/members?days={{ days }}&show=nosleep"><span class="icon">⚠️</span><span class="label">No 8h break</span> <span class="count">{{ counts.no_break }}</span></a>
        <a class="nav-item {% if show == 'inactive' %}active{% endif %}" href="/members?days={{ days }}&show=inactive"><span class="icon">💤</span><span class="label">Inactive in window</span> <span class="count">{{ counts.inactive }}</span></a>
      </div>
      <div class="nav-section">
        <div class="nav-section-title">Projects</div>
        {% for p in sidebar.projects %}
        <a class="nav-item {% if channel_id == p.channel_id %}active{% endif %}" href="/members?days={{ days }}&channel={{ p.channel_id }}" title="{{ p.category or '' }}">
          <span class="hash">#</span><span class="label">{{ p.name }}</span><span class="count">{{ p.cnt }}</span>
        </a>
        {% endfor %}
      </div>
      <div class="nav-section">
        <div class="nav-section-title">Teams / Roles</div>
        {% for r in sidebar.roles %}
        <a class="nav-item {% if role_id == r.role_id %}active{% endif %}" href="/members?days={{ days }}&role={{ r.role_id }}">
          <span class="role-dot" style="background: {{ r.color if r.color and r.color != 0 else '#5865f2' }}"></span>
          <span class="label">{{ r.name }}</span>
          <span class="count">{{ r.cnt }}</span>
        </a>
        {% endfor %}
      </div>
    </div>
  </aside>

  <div class="main">
    {{ body|safe }}
  </div>
</div>
</body></html>
"""

INDEX_BODY = """
<div class="topbar">
  <h1>{{ page_title }}</h1>
  <span class="subtitle">{{ filtered_count }} {{ 'member' if filtered_count == 1 else 'members' }} · {{ days }} day window</span>
  <span class="spacer"></span>
  <form method="get" style="display:flex;gap:8px;align-items:center;">
    {% if role_id %}<input type="hidden" name="role" value="{{ role_id }}">{% endif %}
    {% if channel_id %}<input type="hidden" name="channel" value="{{ channel_id }}">{% endif %}
    {% if show %}<input type="hidden" name="show" value="{{ show }}">{% endif %}
    <input type="search" name="q" value="{{ search }}" placeholder="Search members…" style="width:200px;">
    <select name="days" onchange="this.form.submit()">
      {% for d in [1,3,7,14,30,60] %}
      <option value="{{ d }}" {% if d == days %}selected{% endif %}>{{ d }}d</option>
      {% endfor %}
    </select>
    <select name="channel" onchange="this.form.submit()">
      <option value="">All channels</option>
      {% for ch in sidebar.channels %}
      <option value="{{ ch.channel_id }}" {% if channel_id == ch.channel_id %}selected{% endif %}>{% if ch.category %}{{ ch.category }} / {% endif %}#{{ ch.name }}</option>
      {% endfor %}
    </select>
    <select name="sort" onchange="this.form.submit()">
      <option value="score"  {% if sort == 'score' %}selected{% endif %}>Score</option>
      <option value="msgs"   {% if sort == 'msgs' %}selected{% endif %}>Messages</option>
      <option value="msgs24" {% if sort == 'msgs24' %}selected{% endif %}>Msgs 24h</option>
      <option value="voice"  {% if sort == 'voice' %}selected{% endif %}>Voice min</option>
      <option value="last"   {% if sort == 'last' %}selected{% endif %}>Last seen</option>
      <option value="sleep"  {% if sort == 'sleep' %}selected{% endif %}>Max gap (sleep)</option>
      <option value="name"   {% if sort == 'name' %}selected{% endif %}>Name</option>
    </select>
    <button type="submit" style="display:none;">Apply</button>
  </form>
</div>

<div class="content">
  {% if voice_now %}
  <div class="vc-strip">
    <div class="vc-header"><span class="live"></span>Live in voice <span style="color:var(--dim);font-weight:500;">· {{ voice_now|length }} active</span></div>
    <div class="vc-list">
      {% for v in voice_now %}
      <a class="vc-card" href="/member/{{ v.user_id }}">
        <span class="avatar">{% if v.avatar_url %}<img src="{{ v.avatar_url }}" alt="">{% else %}{{ (v.display_name or v.name)[0]|upper }}{% endif %}<span class="status online"></span></span>
        <div>
          <div class="vc-name">{{ v.display_name or v.name }}</div>
          <div class="vc-meta">🔊 {{ v.channel_name or '?' }} · {{ v.joined_at | humanize }}</div>
        </div>
      </a>
      {% endfor %}
    </div>
  </div>
  {% endif %}

  <div class="stats">
    <div class="stat"><div class="v">{{ filtered_count }}</div><div class="l">shown</div></div>
    <div class="stat green"><div class="v">{{ counts.working }}</div><div class="l">working (last 8h)</div></div>
    <div class="stat"><div class="v">{{ counts.active }}</div><div class="l">active in window</div></div>
    <div class="stat red"><div class="v">{{ counts.no_break }}</div><div class="l">no 8h break (24h)</div></div>
    <div class="stat"><div class="v">{{ counts.voice_live }}</div><div class="l">in voice now</div></div>
    <div class="stat"><div class="v">{{ counts.total_msgs }}</div><div class="l">messages tracked</div></div>
  </div>

  <div class="panel">
    <div class="panel-header">{{ page_title }} <span class="count">· {{ filtered_count }}</span></div>
    {% if rows %}
    <table class="members">
      <thead><tr>
        <th>Member</th>
        <th>Role</th>
        <th class="num">Msgs ({{ days }}d)</th>
        <th class="num">24h</th>
        <th class="num">8h</th>
        <th class="num">Voice (min)</th>
        <th>Activity 7d</th>
        <th>Main channel</th>
        <th>Last activity</th>
        <th>24h gap</th>
        <th>Status</th>
      </tr></thead>
      <tbody>
      {% for r in rows %}
      <tr onclick="location='/member/{{ r.user_id }}'">
        <td>
          <div class="member-cell">
            <span class="avatar">
              {% if r.avatar %}<img src="{{ r.avatar }}" alt="">{% else %}{{ r.initial }}{% endif %}
              <span class="status {{ r.presence_class }}"></span>
            </span>
            <div>
              <div class="member-name">{{ r.name }}</div>
              <div class="member-handle">{{ r.username }}</div>
            </div>
          </div>
        </td>
        <td>{% if r.top_role.name %}<span class="chip role-chip"><span class="dot" style="background: {{ r.top_role.color }}"></span>{{ r.top_role.name }}</span>{% else %}<span class="member-handle">—</span>{% endif %}</td>
        <td class="num">{{ r.msg_count }}</td>
        <td class="num">{{ r.msg_24h }}</td>
        <td class="num">{{ r.msg_8h }}</td>
        <td class="num">{{ r.voice_min }}</td>
        <td>
          <span class="spark">
          {% for v in r.spark %}
            <span class="bar {% if v == 0 %}zero{% endif %}" style="height: {{ (v / r.spark_max * 22)|round|int if v > 0 else 3 }}px;" title="{{ v }} msgs"></span>
          {% endfor %}
          </span>
        </td>
        <td>{% if r.top_channel_name %}<span class="chip">#{{ r.top_channel_name }}</span> <span class="member-handle">{{ r.top_channel_pct }}%</span>{% else %}<span class="member-handle">—</span>{% endif %}</td>
        <td>
          {% if r.last_msg_content %}
          <div class="snippet">{{ r.last_msg_content[:80] }}</div>
          <div class="snippet-meta">#{{ r.last_msg_channel or '?' }} · {{ r.last_seen }}</div>
          {% else %}
          <span class="member-handle">{{ r.last_seen }}</span>
          {% endif %}
        </td>
        <td><span class="flag {% if r.took_break %}ok{% else %}bad{% endif %}">{{ r.sleep_gap_h }}h</span></td>
        <td>
          {% if r.working_now %}<span class="flag ok">working</span>
          {% elif r.is_active %}<span class="flag warn">recent</span>
          {% else %}<span class="flag bad">idle</span>{% endif %}
        </td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
    {% else %}
    <div class="empty">No members match these filters.</div>
    {% endif %}
  </div>
</div>
"""

MEMBER_BODY = """
<div class="profile">
  <a class="back-link" href="javascript:history.back()">← Back</a>
  <div class="profile-header">
    <span class="avatar" style="width:80px;height:80px;font-size:30px;">
      {% if m.avatar %}<img src="{{ m.avatar }}" alt="">{% else %}{{ m.initial }}{% endif %}
      <span class="status {{ m.presence_class }}" style="width:18px;height:18px;border-width:3px;"></span>
    </span>
    <div>
      <h1>{{ m.name }}</h1>
      <div class="handle">{{ m.username }} · {{ m.presence_label }}</div>
      <div class="roles-strip">
        {% for role in m.roles_full %}
        <span class="chip role-chip"><span class="dot" style="background:{{ role.color }}"></span>{{ role.name }}</span>
        {% endfor %}
      </div>
    </div>
  </div>

  <div class="stats">
    <div class="stat"><div class="v">{{ m.msg_count }}</div><div class="l">messages ({{ days }}d)</div></div>
    <div class="stat green"><div class="v">{{ m.msg_24h }}</div><div class="l">last 24h</div></div>
    <div class="stat"><div class="v">{{ m.voice_min }}</div><div class="l">voice minutes</div></div>
    <div class="stat"><div class="v">{{ m.days_active }}</div><div class="l">days active</div></div>
    <div class="stat {% if m.took_break %}green{% else %}red{% endif %}"><div class="v">{{ m.sleep_gap_h }}h</div><div class="l">max 24h gap</div></div>
    <div class="stat"><div class="v">{{ m.last_seen }}</div><div class="l">last seen</div></div>
  </div>

  <div class="section">
    <h2>Daily activity — last 30 days</h2>
    <div class="bar-chart">
      {% for d in m.daily %}
      <div class="bar" style="height: {{ (d.cnt / m.daily_max * 110)|round|int if d.cnt > 0 else 2 }}px;" title="{{ d.day }}: {{ d.cnt }} msgs"></div>
      {% endfor %}
    </div>
    <div class="day-row">
      {% for d in m.daily %}
      <div class="col label" style="font-size:10px;color:var(--dim);text-align:center;">{% if loop.index0 % 5 == 0 %}{{ d.day[5:] }}{% endif %}</div>
      {% endfor %}
    </div>
  </div>

  <div class="section">
    <h2>Top channels (their projects)</h2>
    {% if m.top_channels %}
    <table class="members" style="border-collapse:collapse;width:100%;">
      <tbody>
      {% for ch in m.top_channels %}
      <tr>
        <td><span class="chip">#{{ ch.name }}</span> {% if ch.category %}<span class="member-handle">{{ ch.category }}</span>{% endif %}</td>
        <td class="num">{{ ch.cnt }} msgs</td>
        <td class="num"><span class="member-handle">{{ ch.pct }}%</span></td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
    {% else %}<div class="member-handle">No messages in window.</div>{% endif %}
  </div>

  <div class="section">
    <h2>Voice sessions ({{ days }}d)</h2>
    {% if m.voice_sessions %}
    <table class="members" style="width:100%;"><tbody>
    {% for vs in m.voice_sessions %}
    <tr><td><span class="chip">🔊 {{ vs.channel_name or '?' }}</span></td>
        <td>{{ vs.joined }}</td>
        <td class="num">{{ vs.duration_min }} min</td></tr>
    {% endfor %}
    </tbody></table>
    {% else %}<div class="member-handle">No voice activity.</div>{% endif %}
  </div>

  <div class="section">
    <h2>Recent messages</h2>
    {% if m.recent_msgs %}
    {% for msg in m.recent_msgs %}
    <div class="msg-row">
      <div class="meta">#{{ msg.channel_name or '?' }} · {{ msg.when }}</div>
      <div class="body">{{ msg.content or '(empty)' }}</div>
    </div>
    {% endfor %}
    {% else %}<div class="member-handle">No messages.</div>{% endif %}
  </div>
</div>
"""

HOME_BODY = """
<div class="topbar">
  <h1>Overview</h1>
  <span class="subtitle">{{ sidebar.meta.name }} · last {{ days }} day(s)</span>
  <span class="spacer"></span>
  <form method="get" style="display:flex;gap:8px;align-items:center;">
    <select name="days" onchange="this.form.submit()">
      {% for d in [1,3,7,14,30,60] %}
      <option value="{{ d }}" {% if d == days %}selected{% endif %}>{{ d }}d</option>
      {% endfor %}
    </select>
  </form>
</div>

<div class="content">
  <div class="stats">
    <div class="stat"><div class="v">{{ sidebar.meta.total_members }}</div><div class="l">members</div></div>
    <div class="stat green"><div class="v">{{ counts.working }}</div><div class="l">working (last 8h)</div></div>
    <div class="stat"><div class="v">{{ counts.active }}</div><div class="l">active in window</div></div>
    <div class="stat red"><div class="v">{{ counts.no_break }}</div><div class="l">no 8h break (24h)</div></div>
    <div class="stat"><div class="v">{{ counts.voice_live }}</div><div class="l">in voice now</div></div>
    <div class="stat"><div class="v">{{ sidebar.meta.total_messages }}</div><div class="l">messages tracked</div></div>
  </div>

  {% if voice_now %}
  <div class="section-title"><span class="live"></span>Live in voice · {{ voice_now|length }}</div>
  <div class="vc-strip">
    <div class="vc-list">
      {% for v in voice_now %}
      <a class="vc-card" href="/member/{{ v.user_id }}">
        <span class="avatar">{% if v.avatar_url %}<img src="{{ v.avatar_url }}" alt="">{% else %}{{ (v.display_name or v.name)[0]|upper }}{% endif %}<span class="status online"></span></span>
        <div>
          <div class="vc-name">{{ v.display_name or v.name }}</div>
          <div class="vc-meta">🔊 {{ v.channel_name or '?' }} · {{ v.joined_at | humanize }}</div>
        </div>
      </a>
      {% endfor %}
    </div>
  </div>
  {% endif %}

  <div class="section-title">Top projects · {{ days }}d</div>
  <div class="proj-grid">
    {% for p in projects %}
    <a class="proj-card" href="/members?days={{ days }}&channel={{ p.channel_id }}">
      <div class="name"><span class="hash">#</span>{{ p.name }}</div>
      <div class="meta"><span class="pill">{{ p.cnt }} msgs</span><span>{{ p.author_cnt }} contributors</span>{% if p.category %} · <span style="color:var(--dim);">{{ p.category }}</span>{% endif %}</div>
      <div class="contribs">
        {% for t in p.top_contributors %}
        <div class="contrib-row">
          <span class="avatar">{% if t.avatar %}<img src="{{ t.avatar }}" alt="">{% else %}{{ t.initial }}{% endif %}</span>
          <span class="name">{{ t.name }}</span>
          <span class="cnt">{{ t.cnt }}</span>
        </div>
        {% endfor %}
      </div>
    </a>
    {% endfor %}
  </div>

  <div class="section-title" style="margin-top:24px;">Top performers · {{ days }}d</div>
  <div class="panel">
    <div class="panel-header">By total activity score <span class="count">· top {{ top_performers|length }}</span><a href="/members?days={{ days }}">See all →</a></div>
    <table class="members">
      <thead><tr>
        <th>Member</th><th>Role</th><th class="num">Msgs</th><th class="num">Voice (min)</th>
        <th>Main channel</th><th>Last activity</th><th>Status</th>
      </tr></thead>
      <tbody>
      {% for r in top_performers %}
      <tr onclick="location='/member/{{ r.user_id }}'">
        <td>
          <div class="member-cell">
            <span class="avatar">{% if r.avatar %}<img src="{{ r.avatar }}" alt="">{% else %}{{ r.initial }}{% endif %}<span class="status {{ r.presence_class }}"></span></span>
            <div><div class="member-name">{{ r.name }}</div><div class="member-handle">{{ r.username }}</div></div>
          </div>
        </td>
        <td>{% if r.top_role.name %}<span class="chip role-chip"><span class="dot" style="background:{{ r.top_role.color }}"></span>{{ r.top_role.name }}</span>{% else %}<span class="member-handle">—</span>{% endif %}</td>
        <td class="num">{{ r.msg_count }}</td>
        <td class="num">{{ r.voice_min }}</td>
        <td>{% if r.top_channel_name %}<span class="chip">#{{ r.top_channel_name }}</span>{% else %}<span class="member-handle">—</span>{% endif %}</td>
        <td><span class="member-handle">{{ r.last_seen }}</span></td>
        <td>
          {% if r.working_now %}<span class="flag ok">working</span>
          {% elif r.is_active %}<span class="flag warn">recent</span>
          {% else %}<span class="flag bad">idle</span>{% endif %}
        </td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</div>
"""


VOICE_BODY = """
<div class="topbar">
  <h1>🎙 Voice channels</h1>
  <span class="subtitle">{{ live_count }} in voice now · stats over last {{ days }}d</span>
  <span class="spacer"></span>
  <form method="get" style="display:flex;gap:8px;align-items:center;">
    <select name="days" onchange="this.form.submit()">
      {% for d in [1,3,7,14,30,60] %}
      <option value="{{ d }}" {% if d == days %}selected{% endif %}>{{ d }}d</option>
      {% endfor %}
    </select>
  </form>
</div>

<div class="content">
  <div class="section-title"><span class="live"></span>Live in voice · {{ live_count }} {% if live_count != 1 %}people{% else %}person{% endif %}</div>
  {% if live_by_channel %}
  <div class="proj-grid">
    {% for ch_id, members in live_by_channel.items() %}
    <div class="proj-card">
      <div class="name">🔊 {{ (members[0].channel_name if members else '?') }}</div>
      <div class="meta"><span class="pill">{{ members|length }} in</span></div>
      <div class="contribs">
        {% for v in members %}
        <a class="contrib-row" href="/member/{{ v.user_id }}">
          <span class="avatar">{% if v.avatar_url %}<img src="{{ v.avatar_url }}" alt="">{% else %}{{ (v.display_name or v.name)[0]|upper }}{% endif %}<span class="status online"></span></span>
          <span class="name">{{ v.display_name or v.name }}</span>
          <span class="cnt">{{ v.joined_at | humanize }}</span>
        </a>
        {% endfor %}
      </div>
    </div>
    {% endfor %}
  </div>
  {% else %}
  <div class="empty"><h3>Nobody is in voice right now.</h3><div>Live state populates the moment anyone joins a voice channel.</div></div>
  {% endif %}

  <div class="section-title">Voice channel activity · {{ days }}d</div>
  <div class="panel">
    <div class="panel-header">By total voice time</div>
    {% if per_channel_stats %}
    <table class="members">
      <thead><tr><th>Channel</th><th class="num">Sessions</th><th class="num">Unique users</th><th class="num">Total time</th></tr></thead>
      <tbody>
      {% for ch in per_channel_stats %}
      <tr>
        <td><span class="chip">🔊 {{ ch.name or '?' }}</span></td>
        <td class="num">{{ ch.session_cnt }}</td>
        <td class="num">{{ ch.unique_users }}</td>
        <td class="num">{{ (ch.total_sec // 60) if ch.total_sec else 0 }} min</td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
    {% else %}
    <div class="empty">No voice activity in window yet.</div>
    {% endif %}
  </div>

  <div class="section-title" style="margin-top:24px;">Top voice users · {{ days }}d</div>
  <div class="panel">
    <div class="panel-header">By total voice time <a href="/members?days={{ days }}&sort=voice">See full table →</a></div>
    {% if top_voice_users %}
    <table class="members">
      <thead><tr><th>Member</th><th class="num">Sessions</th><th class="num">Total minutes</th></tr></thead>
      <tbody>
      {% for u in top_voice_users %}
      <tr onclick="location='/member/{{ u.user_id }}'">
        <td>
          <div class="member-cell">
            <span class="avatar">{% if u.avatar_url %}<img src="{{ u.avatar_url }}" alt="">{% else %}{{ (u.display_name or u.name)[0]|upper }}{% endif %}</span>
            <div><div class="member-name">{{ u.display_name or u.name }}</div><div class="member-handle">{{ u.name }}</div></div>
          </div>
        </td>
        <td class="num">{{ u.sessions }}</td>
        <td class="num">{{ (u.total_sec // 60) if u.total_sec else 0 }}</td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
    {% else %}
    <div class="empty">No voice activity in window yet.</div>
    {% endif %}
  </div>
</div>
"""


app.jinja_env.filters["humanize"] = humanize


def compute_counts(rows, voice_count, total_msgs):
    return {
        "working": sum(1 for r in rows if r["working_now"]),
        "active": sum(1 for r in rows if r["is_active"]),
        "no_break": sum(1 for r in rows if not r["took_break"]),
        "inactive": sum(1 for r in rows if not r["is_active"]),
        "voice_live": voice_count,
        "total_msgs": total_msgs,
    }


SORT_KEYS = {
    "score":  lambda r: (-r["score"], r["name"].lower()),
    "msgs":   lambda r: (-r["msg_count"], r["name"].lower()),
    "msgs24": lambda r: (-r["msg_24h"], r["name"].lower()),
    "voice":  lambda r: (-r["voice_min"], r["name"].lower()),
    "last":   lambda r: (r["last_seen_iso"] or "", r["name"].lower()),
    "name":   lambda r: r["name"].lower(),
    "sleep":  lambda r: (r["sleep_gap_h"], r["name"].lower()),
}


@app.route("/")
def home():
    try:
        days = max(1, min(int(request.args.get("days", WINDOW_DAYS)), WINDOW_DAYS))
    except ValueError:
        days = WINDOW_DAYS
    sidebar = sidebar_data()
    voice_now = live_voice()
    full_rows = build_data(days, None, None, "")
    counts = compute_counts(full_rows, len(voice_now), sidebar["meta"]["total_messages"])
    projects = top_projects_with_contributors(days=days, n=6)
    top_perf = sorted(full_rows, key=SORT_KEYS["score"])[:10]

    body = render_template_string(
        HOME_BODY,
        sidebar=sidebar, voice_now=voice_now, counts=counts,
        days=days, projects=projects, top_performers=top_perf,
    )
    return render_template_string(
        LAYOUT,
        body=body, css=CSS, sidebar=sidebar, days=days, role_id=None,
        channel_id=None, show="all", counts=counts, page_title="Overview",
        route="home",
    )


@app.route("/members")
def members():
    try:
        days = max(1, min(int(request.args.get("days", WINDOW_DAYS)), WINDOW_DAYS))
    except ValueError:
        days = WINDOW_DAYS
    role_id = request.args.get("role")
    role_id = int(role_id) if role_id and role_id.isdigit() else None
    channel_id = request.args.get("channel")
    channel_id = int(channel_id) if channel_id and channel_id.isdigit() else None
    sort = request.args.get("sort", "score")
    if sort not in SORT_KEYS:
        sort = "score"
    show = request.args.get("show", "all")
    search = (request.args.get("q") or "").strip()

    sidebar = sidebar_data()
    voice_now = live_voice()
    rows = build_data(days, role_id, channel_id, search)

    # voice members live filter
    if show == "voice":
        voice_uids = {v["user_id"] for v in voice_now}
        rows = [r for r in rows if r["user_id"] in voice_uids]
    elif show == "active":
        rows = [r for r in rows if r["is_active"]]
    elif show == "inactive":
        rows = [r for r in rows if not r["is_active"]]
    elif show == "working":
        rows = [r for r in rows if r["working_now"]]
    elif show == "nosleep":
        rows = [r for r in rows if not r["took_break"]]

    if sort == "last":
        rows.sort(key=SORT_KEYS[sort], reverse=True)
    else:
        rows.sort(key=SORT_KEYS[sort])

    # counts computed BEFORE the show-filter, against full role-filtered set
    full_rows = build_data(days, role_id, channel_id, search)
    counts = compute_counts(full_rows, len(voice_now), sidebar["meta"]["total_messages"])

    role_label = next((r["name"] for r in sidebar["roles"] if r["role_id"] == role_id), None)
    page_title = role_label or {
        "all": "All members", "active": "Active in window", "inactive": "Inactive in window",
        "working": "Working (last 8h)", "voice": "In voice now", "nosleep": "No 8h break",
    }.get(show, "All members")

    body = render_template_string(
        INDEX_BODY,
        rows=rows, sidebar=sidebar, voice_now=voice_now, counts=counts,
        days=days, role_id=role_id, channel_id=channel_id, sort=sort, show=show,
        search=search, filtered_count=len(rows), page_title=page_title,
    )
    return render_template_string(
        LAYOUT,
        body=body, css=CSS, sidebar=sidebar, days=days, role_id=role_id,
        channel_id=channel_id, show=show, counts=counts, page_title=page_title,
        route="members",
    )


@app.route("/member/<int:user_id>")
def member(user_id):
    try:
        days = max(1, min(int(request.args.get("days", WINDOW_DAYS)), WINDOW_DAYS))
    except ValueError:
        days = WINDOW_DAYS
    now = dt.datetime.now(dt.timezone.utc)
    cutoff = (now - dt.timedelta(days=days)).isoformat()

    sidebar = sidebar_data()
    voice_now = live_voice()

    with db() as c:
        c.row_factory = sqlite3.Row
        mem = c.execute("SELECT * FROM members WHERE user_id = ?", (user_id,)).fetchone()
        if not mem:
            abort(404)

        roles_full = [
            {"name": r["name"], "color": color_to_hex(r["color"])}
            for r in c.execute("""SELECT r.name, r.color FROM member_roles mr
                                  JOIN roles r ON r.role_id = mr.role_id
                                  WHERE mr.user_id = ? AND r.name != '@everyone'
                                  ORDER BY r.position DESC""", (user_id,)).fetchall()
        ]
        channel_names = {r["channel_id"]: (r["name"], r["category"]) for r in c.execute(
            "SELECT channel_id, name, category FROM channels").fetchall()}

        msg_total = c.execute("SELECT COUNT(*) FROM messages WHERE author_id = ? AND created_at >= ?",
                              (user_id, cutoff)).fetchone()[0]
        msg_24h = c.execute("SELECT COUNT(*) FROM messages WHERE author_id = ? AND created_at >= ?",
                            (user_id, (now - dt.timedelta(hours=24)).isoformat())).fetchone()[0]
        voice_sec_row = c.execute(
            """SELECT SUM(COALESCE(duration_sec,
                  CAST((julianday('now') - julianday(joined_at)) * 86400 AS INTEGER))) AS sec
               FROM voice_sessions WHERE user_id = ? AND joined_at >= ?""",
            (user_id, cutoff)).fetchone()
        voice_min = (voice_sec_row["sec"] or 0) // 60 if voice_sec_row else 0

        last_at = c.execute(
            """SELECT MAX(ts) FROM (
                 SELECT created_at AS ts FROM messages WHERE author_id = ?
                 UNION ALL
                 SELECT joined_at AS ts FROM voice_sessions WHERE user_id = ?)""",
            (user_id, user_id)).fetchone()[0]

        # daily activity last 30d
        daily_raw = {r["day"]: r["cnt"] for r in c.execute(
            """SELECT DATE(created_at) AS day, COUNT(*) AS cnt
               FROM messages WHERE author_id = ? AND created_at >= ?
               GROUP BY DATE(created_at)""", (user_id, cutoff)).fetchall()}
        daily = []
        for i in range(days - 1, -1, -1):
            day = (now - dt.timedelta(days=i)).strftime("%Y-%m-%d")
            daily.append({"day": day, "cnt": daily_raw.get(day, 0)})
        daily_max = max((d["cnt"] for d in daily), default=1) or 1
        days_active = sum(1 for d in daily if d["cnt"] > 0)

        # sleep gap (last 24h)
        last24h_iso = (now - dt.timedelta(hours=24)).isoformat()
        ts_rows = c.execute(
            """SELECT ts FROM (
                 SELECT created_at AS ts FROM messages WHERE author_id = ? AND created_at >= ?
                 UNION ALL
                 SELECT joined_at FROM voice_sessions WHERE user_id = ? AND joined_at >= ?)
               ORDER BY ts""", (user_id, last24h_iso, user_id, last24h_iso)).fetchall()
        moments = [parse_iso(last24h_iso)]
        for r in ts_rows:
            pt = parse_iso(r[0])
            if pt:
                moments.append(pt)
        moments.append(now)
        max_gap = 0
        for i in range(1, len(moments)):
            g = (moments[i] - moments[i - 1]).total_seconds() / 3600
            if g > max_gap:
                max_gap = g

        # top channels
        top_channels_raw = c.execute(
            """SELECT channel_id, COUNT(*) AS cnt FROM messages
               WHERE author_id = ? AND created_at >= ?
               GROUP BY channel_id ORDER BY cnt DESC LIMIT 10""",
            (user_id, cutoff)).fetchall()
        top_channels = []
        for r in top_channels_raw:
            name, cat = channel_names.get(r["channel_id"], (None, None))
            top_channels.append({
                "name": name or str(r["channel_id"]),
                "category": cat,
                "cnt": r["cnt"],
                "pct": round(r["cnt"] / msg_total * 100) if msg_total else 0,
            })

        # voice sessions
        voice_sessions = []
        for r in c.execute(
            """SELECT vs.channel_id, vs.joined_at, vs.left_at, vs.duration_sec, ch.name AS channel_name
               FROM voice_sessions vs LEFT JOIN channels ch ON ch.channel_id = vs.channel_id
               WHERE vs.user_id = ? AND vs.joined_at >= ?
               ORDER BY vs.joined_at DESC LIMIT 30""",
            (user_id, cutoff)).fetchall():
            sec = r["duration_sec"]
            if sec is None:
                sec = int((now - parse_iso(r["joined_at"])).total_seconds())
            voice_sessions.append({
                "channel_name": r["channel_name"],
                "joined": humanize(r["joined_at"]),
                "duration_min": sec // 60,
            })

        # recent messages
        recent_msgs = []
        for r in c.execute(
            """SELECT m.content, m.created_at, m.channel_id, ch.name AS channel_name
               FROM messages m LEFT JOIN channels ch ON ch.channel_id = m.channel_id
               WHERE m.author_id = ? ORDER BY m.id DESC LIMIT 25""",
            (user_id,)).fetchall():
            recent_msgs.append({
                "content": r["content"],
                "when": humanize(r["created_at"]),
                "channel_name": r["channel_name"],
            })

    pres_label, pres_class = presence_status(last_at)

    m = {
        "name": mem["display_name"] or mem["name"],
        "username": mem["name"],
        "avatar": mem["avatar_url"],
        "initial": (mem["display_name"] or mem["name"] or "?")[0].upper(),
        "roles_full": roles_full,
        "msg_count": msg_total,
        "msg_24h": msg_24h,
        "voice_min": voice_min,
        "days_active": days_active,
        "last_seen": humanize(last_at),
        "presence_label": pres_label,
        "presence_class": pres_class,
        "sleep_gap_h": round(max_gap, 1),
        "took_break": max_gap >= 8,
        "daily": daily,
        "daily_max": daily_max,
        "top_channels": top_channels,
        "voice_sessions": voice_sessions,
        "recent_msgs": recent_msgs,
    }

    counts = compute_counts(build_data(days, None, None, ""), len(voice_now),
                            sidebar["meta"]["total_messages"])
    body = render_template_string(MEMBER_BODY, m=m, days=days)
    return render_template_string(
        LAYOUT,
        body=body, css=CSS, sidebar=sidebar, days=days, role_id=None, channel_id=None,
        show="all", counts=counts, page_title=m["name"], route="member",
    )


@app.route("/voice")
def voice_page():
    try:
        days = max(1, min(int(request.args.get("days", WINDOW_DAYS)), WINDOW_DAYS))
    except ValueError:
        days = WINDOW_DAYS
    sidebar = sidebar_data()
    voice_now = live_voice()
    full_rows = build_data(days, None, None, "")
    counts = compute_counts(full_rows, len(voice_now), sidebar["meta"]["total_messages"])
    overview = voice_overview(days)
    body = render_template_string(
        VOICE_BODY,
        days=days, live_count=overview["live_count"],
        live_by_channel=overview["live_by_channel"],
        per_channel_stats=overview["per_channel_stats"],
        top_voice_users=overview["top_voice_users"],
    )
    return render_template_string(
        LAYOUT,
        body=body, css=CSS, sidebar=sidebar, days=days, role_id=None,
        channel_id=None, show="all", counts=counts, page_title="Voice channels",
        route="voice",
    )


@app.route("/health")
def health():
    with db() as c:
        n = c.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        m = c.execute("SELECT COUNT(*) FROM members WHERE is_bot=0 AND left_at IS NULL").fetchone()[0]
        v = c.execute("SELECT COUNT(*) FROM voice_sessions WHERE left_at IS NULL").fetchone()[0]
    return {"ok": True, "messages": n, "members": m, "voice_open": v, "ready": client.is_ready()}


@app.route("/admin/rebackfill")
def rebackfill():
    days = int(request.args.get("days", str(WINDOW_DAYS)))
    guild = client.get_guild(GUILD_ID) if client.is_ready() else None
    if not guild:
        return {"ok": False, "error": "guild not available"}, 503
    asyncio.run_coroutine_threadsafe(backfill(guild, days), client.loop)
    return {"ok": True, "queued": True, "days": days}


@app.route("/admin/probe-voice")
def probe_voice():
    guild = client.get_guild(GUILD_ID) if client.is_ready() else None
    if not guild:
        return {"ok": False, "error": "guild not available"}, 503
    live = []
    total_in_vc = 0
    for ch in guild.voice_channels:
        members = [{"user_id": m.id, "name": str(m), "display_name": m.display_name,
                    "self_mute": m.voice.self_mute if m.voice else None,
                    "self_deaf": m.voice.self_deaf if m.voice else None,
                    "streaming": m.voice.self_stream if m.voice else None,
                    "video": m.voice.self_video if m.voice else None}
                   for m in ch.members if not m.bot]
        if members:
            total_in_vc += len(members)
        live.append({"channel_id": ch.id, "channel_name": ch.name,
                     "user_limit": ch.user_limit, "members": members})
    with db() as c:
        c.row_factory = sqlite3.Row
        db_open = [dict(r) for r in c.execute("""
            SELECT vs.user_id, vs.channel_id, vs.joined_at, ch.name AS channel_name, m.name AS user_name
            FROM voice_sessions vs
            LEFT JOIN channels ch ON ch.channel_id = vs.channel_id
            LEFT JOIN members m ON m.user_id = vs.user_id
            WHERE vs.left_at IS NULL
        """).fetchall()]
    return {
        "ok": True,
        "live_total": total_in_vc,
        "db_open_total": len(db_open),
        "live_by_channel": live,
        "db_open_sessions": db_open,
    }


@app.route("/admin/resync-voice")
def resync_voice():
    guild = client.get_guild(GUILD_ID) if client.is_ready() else None
    if not guild:
        return {"ok": False, "error": "guild not available"}, 503
    now = now_iso()
    closed = opened = 0
    with db() as c:
        closed = c.execute(
            """UPDATE voice_sessions SET left_at = ?,
               duration_sec = CAST((julianday(?) - julianday(joined_at)) * 86400 AS INTEGER)
               WHERE left_at IS NULL""", (now, now)).rowcount
        for ch in guild.voice_channels:
            for m in ch.members:
                if m.bot:
                    continue
                c.execute("INSERT INTO voice_sessions (user_id, channel_id, joined_at) VALUES (?,?,?)",
                          (m.id, ch.id, now))
                # ensure member row exists with avatar
                c.execute("""INSERT INTO members (user_id, name, display_name, avatar_url, is_bot)
                             VALUES (?, ?, ?, ?, 0)
                             ON CONFLICT(user_id) DO UPDATE SET
                               name=excluded.name, display_name=excluded.display_name,
                               avatar_url=excluded.avatar_url""",
                          (m.id, str(m), m.display_name,
                           str(m.display_avatar.url) if m.display_avatar else None))
                opened += 1
    return {"ok": True, "closed": closed, "opened": opened}


@app.route("/admin/probe-perms")
def probe_perms():
    guild = client.get_guild(GUILD_ID) if client.is_ready() else None
    if not guild:
        return {"ok": False, "error": "guild not available"}, 503
    me = guild.me
    counts = {"text_total": 0, "readable": 0, "no_view": 0, "no_history": 0}
    examples = []
    for ch in guild.text_channels:
        counts["text_total"] += 1
        p = ch.permissions_for(me)
        if p.view_channel and p.read_message_history:
            counts["readable"] += 1
        elif not p.view_channel:
            counts["no_view"] += 1
            if len(examples) < 8:
                examples.append(f"#{ch.name} (no view)")
        else:
            counts["no_history"] += 1
            if len(examples) < 8:
                examples.append(f"#{ch.name} (no history)")
    return {"ok": True, "counts": counts, "examples": examples}


def run_dashboard():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), use_reloader=False)


if __name__ == "__main__":
    threading.Thread(target=run_dashboard, daemon=True).start()
    print("Dashboard: http://localhost:5000")
    client.run(TOKEN, log_handler=None)
