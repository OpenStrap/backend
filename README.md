# OpenStrap — backend

The server half: it takes the raw frames your phone pulls off the band, stores them,
and turns them into daily metrics. A single Cloudflare Worker.

> Not affiliated with or endorsed by WHOOP. Self-host it — your data, your server.

## The idea
The band is a sensor that buffers raw records; the phone drains them over Bluetooth
and ships the **raw bytes** here. The backend decodes them server-side (it bundles
[protocol](https://github.com/OpenStrap/protocol) +
[analytics](https://github.com/OpenStrap/analytics)), rolls them into per-minute
aggregates, and derives the daily numbers on a schedule. The phone app just renders
what comes back — all the heavy lifting is here, so the client stays dumb and the
analytics stay reproducible.

Raw-first is deliberate: the original frames live in object storage and can be
re-decoded later as the decoders improve. Nothing is thrown away, nothing is
computed on a value you can't trace back to a real record.

## Stack
- **Cloudflare Workers** (Hono) — the API + cron.
- **D1** (SQLite) — per-minute rollups + tiny derived tables.
- **R2** — the raw frames, re-decodable forever.

## Shape
- `POST /ingest/{batch,events}` — raw frames in (JWT-auth, per user).
- `GET /today`, `/sleep`, `/strain`, `/history`, `/day/{strain,sleep,timeline,stress}`,
  `/records`, `/journal`, `/notifications` — derived reads.
- Crons: hourly per-user analytics; nightly full re-derive + prune.

## Run it
```
npm install
npx wrangler d1 create openstrap-db        # then put the id in wrangler.toml
npx wrangler r2 bucket create openstrap-raw
npx wrangler d1 execute openstrap-db --file src/db/schema.sql
npx wrangler secret put JWT_SECRET
npx wrangler secret put ADMIN_TOKEN
npx wrangler deploy
```
Secrets live in `wrangler secret` / `.dev.vars`, never in the repo.
