# OpenStrap backend

> **This is optional. The app does not need it.**
>
> OpenStrap is local-first: [edge](https://github.com/OpenStrap/edge) pulls the bytes off
> the band over Bluetooth, decodes them with
> [protocol](https://github.com/OpenStrap/protocol), computes every metric with
> [analytics](https://github.com/OpenStrap/analytics), and stores the results on your
> phone. None of that involves this worker, or any other server.

So what is this? A small companion worker backing a handful of narrow, mostly optional
features — and nothing else. It does **not** receive your raw frames, it does **not**
decode records, and it does **not** compute recovery, strain, sleep, or any other metric.
All of that used to live here. It doesn't anymore.

Questions about self-hosting your own instance? [Discord](https://discord.gg/dUXds5MWkd).

## What it actually does

| Endpoint | What it's for | Required? |
|---|---|---|
| `GET /app/status` | OTA pointer + in-app announcements | No — the app works offline |
| `GET /legal/terms` | Terms text behind the onboarding consent gate | No |
| `POST /auth/{request-otp,verify-otp,refresh}` | Sign-in **for legacy import only** | Only to import |
| `GET /profile`, `/strain`, `/sleep`, `/sessions` | One-time read of an old OpenStrap cloud account | Only to import |
| `POST /consent` | Consent ledger entry | No |
| `POST /telemetry` | **Opt-in** crash/usage telemetry — device-id keyed, no account | No, off by default |
| `POST /health/upload` | **Opt-in** health-data contribution (full local `.db`) | No, off by default |
| `DELETE /data` | Right to be forgotten — erases everything keyed to you | — |

The two data-collecting endpoints (`/telemetry`, `/health/upload`) are opt-in, off by
default, and are compiled out entirely of App Store and Play Store builds. See
[edge's PRIVACY.md](https://github.com/OpenStrap/edge/blob/main/PRIVACY.md) for the full
two-tier policy.

## Why it still exists

Three reasons, none of them "computing your metrics":

1. **Legacy import.** Early OpenStrap had a real cloud backend. People have data in it.
   This lets them pull it down once and then never talk to a server again.
2. **An OTA/announcement pointer**, so a sideloaded app can find out there's a newer build.
3. **Opt-in contribution**, for people who actively want to donate data to improve the
   sleep-staging and protocol work.

## Run your own

One Cloudflare Worker on the **free** plan — no Queues, no Cron, no Durable Objects, by
deliberate design (see the comments in `wrangler.toml`, and keep it that way).

```bash
npm install
wrangler d1 create openstrap-v3-slim-db
wrangler r2 bucket create openstrap-v3-health
wrangler d1 execute openstrap-v3-slim-db --file=src/db/schema.sql
wrangler secret put JWT_SECRET
wrangler deploy
```

Then point the app at your worker instead of mine. If you're self-hosting just for
yourself, set `STATIC_OTP` so it doesn't need an email provider — never set that on a
shared deployment.

## History

This repo used to be the centre of OpenStrap: it ingested raw frames, stored them in R2,
rolled them into minutes, and computed everything on a cron. The architecture moved
on-device in mid-2026 and the server was cut down to what you see here. If you want the
old design, it's in `src/db/migrate_to_slim.sql` and the git history.

---

Not affiliated with, endorsed by, or connected to WHOOP. MIT licensed.
