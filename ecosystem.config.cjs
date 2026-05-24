// PM2 config for discord-tracker on the DO VPS.
//
// Memory budget on 138.68.57.166 (2 GB):
//   feature4-worker × 2  ~600 MB active (Claude Code children)
//   feature3-worker × 2  ~200 MB
//   discord-tracker × 1  ~80 MB idle, ~400 MB during an /ask query
//   system               ~300 MB
//   ─────────────────────────────
//   peak                 ~1.58 GB — safe with margin
//
// /ask queries are short-lived (5–30s vs feature4's 20 min), and the
// _ask_lock serializes them, so the extra 400 MB spike is brief.
// If the box OOMs, upgrade to 4 GB rather than adding more workers here.
module.exports = {
  apps: [
    {
      name: 'discord-tracker',
      script: '.venv/bin/python',
      args: 'tracker.py',
      cwd: '/opt/discord-tracker',
      interpreter: 'none',
      max_memory_restart: '450M',
      env_file: '/opt/discord-tracker/.env',
      restart_delay: 3000,
      max_restarts: 10,
    },
  ],
};
