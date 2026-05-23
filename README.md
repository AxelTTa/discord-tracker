# Discord Activity Tracker

Single-file Python app: a Discord bot that logs every message + voice session to SQLite, and a Discord-styled Flask dashboard for tracking contributor/reviewer activity, with filters by role, project (channel), and time window.

Built for a Parsewave-style server (devs + reviewers split, projects living in channels) but works on any Discord server you admin.

## Features

- **Homepage** — overview stats, live voice list, top projects with top contributors, top performers
- **Members table** — sortable, filterable by role/project/time window, with 7-day activity sparklines per row
- **Per-member detail page** — 30-day activity chart, top channels (their projects), voice session log, recent messages
- **Sidebar nav** — quick views (Working / Active / In voice / No 8h break / Inactive), Projects (auto-populated from top channels), Roles
- **8-hour break detection** — flags any member whose longest gap in 24h is below 8h (burnout signal)
- **Live VC widget** — pulses green, shows who's in voice right now
- **Auto-prune** — keeps the DB sized to your configured window

## Setup

### 1. Create the Discord bot

1. <https://discord.com/developers/applications> → **New Application**
2. **Bot** tab → **Reset Token** → copy
3. Toggle ON: `MESSAGE CONTENT INTENT`, `SERVER MEMBERS INTENT`
4. **OAuth2 → URL Generator** → scopes: `bot`, permissions: `Administrator` (simplest — the tracker only reads)
5. Open the generated URL, pick your server, **Authorize**

### 2. Get your server ID

Discord client: **User Settings → Advanced → Developer Mode ON** → right-click server → **Copy Server ID**

### 3. Install + run

```bash
git clone https://github.com/YOUR_USER/discord-tracker.git
cd discord-tracker
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill in DISCORD_BOT_TOKEN and DISCORD_GUILD_ID
set -a; source .env; set +a
python tracker.py
```

Open <http://localhost:5000>.

The bot will snapshot members + channels + roles, open voice sessions for whoever is currently in VC, then backfill the last `BACKFILL_DAYS` days of messages from every channel it can read (parallelized).

### 4. Expose publicly (optional)

Quick public URL while you're testing:

```bash
wget -q -O cloudflared https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64
chmod +x cloudflared
./cloudflared tunnel --url http://localhost:5000
```

It prints a `*.trycloudflare.com` URL. No auth — for a permanent URL with auth use a named Cloudflare tunnel or put it behind Cloudflare Access.

## Layout

```
tracker.py        — single file: bot + Flask app + schema + templates
requirements.txt  — discord.py + Flask
.env.example      — config template
```

SQLite tables: `members`, `roles`, `member_roles`, `channels`, `messages`, `voice_sessions`.

## Routes

| Path | What |
|---|---|
| `/` | Homepage: stats, live VC, top projects, top performers |
| `/members` | Full filterable table |
| `/member/<id>` | Per-member detail |
| `/health` | JSON status |
| `/admin/probe-perms` | Diagnose which channels the bot can actually read |
| `/admin/rebackfill?days=N` | Re-run the backfill |

## Activity scoring

Score = `messages + voice_minutes / 10`. Tweak the formula in `tracker.py` around `SORT_KEYS["score"]`.

Status badges:
- **working** (green) = active in last 8h
- **recent** (yellow) = active in window but not last 8h
- **idle** (red) = no activity in window

The "24h gap" column: max gap in the last 24h between any activity (message or voice). If <8h → flagged red ("no 8h break") — likely burning out.

## Hosting 24/7

Cheapest reliable setup: a $5/mo Hetzner CX22 VPS or Oracle Cloud Free Tier ARM. Run `tracker.py` under systemd; back up `tracker.db` periodically with [litestream](https://litestream.io).

## License

MIT
