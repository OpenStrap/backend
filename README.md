# OpenStrap backend

This is the part that does the actual thinking. The phone pulls raw bytes off the band
and ships them here. This server decodes those bytes, files the originals away untouched,
and on a schedule turns them into the numbers you actually look at: recovery, strain,
sleep, stress, all of it. The app on your phone is mostly a screen; the work happens here.

It's one Cloudflare Worker. Not a fleet of microservices, not Kubernetes, one Worker with
a SQLite database (D1) and an object bucket (R2) hanging off it. That's on purpose, it
keeps the whole thing cheap enough to run on Cloudflare's free tier and simple enough that
one person can hold it in their head.

> Not affiliated with WHOOP in any way. Run your own copy, or use mine. More on that at
> the bottom.

## How a byte becomes a number

Here's the whole life of your data, start to finish.

Your phone connects to the band, drains whatever records it buffered to flash, and POSTs
them to `/ingest/batch` as raw hex strings. It never sends me a "heart rate of 72", it
sends me the actual frame the band emitted and I figure out the 72 myself. That matters
because if I get smarter about decoding later, I can re-run everything against the bytes I
already have.

When a batch lands, `ingest.ts` does four things in order:

1. **Rate-limits you.** A token bucket per user, 0.5 tokens a second, burst of 30. Stops
   a runaway client from hammering the thing. Lives in the `rate_limit` table.
2. **Decodes the frames.** `decode.ts` walks each hex string, works out the packet type
   and record type, and pulls out what it can: timestamp, heart rate, a motion magnitude,
   a step increment, whether the band was on your wrist. The 1 Hz record (`parse_r24`,
   borrowed from the protocol package) and the IMU-bearing R10 are the ones that carry
   real signal.
3. **Saves the raw bytes to R2** under `raw/{you}/{device}/{when}-{first}-{last}.txt`,
   one frame per line. This is the bit I never throw away. Minute rollups get pruned after
   90 days; the raw frames stay so they can be re-decoded forever.
4. **Rolls everything into minutes.** `rollup.ts` buckets the decoded samples by
   `floor(ts/60)*60` and writes them to the `minute` table.

The `minute` table is the one clever bit worth understanding before you touch anything.
It doesn't store an average heart rate, it stores the running pieces: `hr_sum`, `hr_n`,
`act_sum`, `act_n`, plus min/max. The upsert adds the new pieces onto whatever's already
there. The reason is that uploads aren't clean. The phone retries, batches overlap, the
same frame shows up twice. If I stored averages I'd corrupt them on every double-send.
Storing sums means re-uploading the exact same data converges to the exact same answer.
Idempotency for free. Don't break this.

Once minutes are written, the user gets flagged dirty (or pushed onto a queue if you've
got the paid plan), and that's where ingest stops. The heavy math is deliberately not on
the request path.

## Where the metrics actually get computed

`analytics.ts` is the brain, and it runs on a cron. Two schedules, both in
`wrangler.toml`:

- **Every 30 minutes** (`*/30`): a light sweep — re-derive every dirty user (their daily
  numbers, sleep, incremental steps), and the moment a night actually finishes, kick off
  that night's HRV. Cheap work only; the heavy R2 re-decodes are fanned out onto a queue,
  one bounded `(user, day)` unit per consumer invocation.
- **Every night at 3:30** (`30 3`): the backstop — re-decode HRV and respiratory rate
  only for the recent nights still *missing* them (so a night is never decoded twice, and
  a night the wake-time run left empty gets retried), true up steps, and prune minute rows
  past their retention. Anything already computed is skipped.

HRV is real now: the beat-to-beat R-R intervals live in the 1 Hz (V24) records, so
`biometrics.ts` re-decodes them from the raw bytes in R2 (off the request path) to drive
recovery, readiness, and HRV-based stress.

`processUser` is where it happens for one person. It reads their minutes, pulls their
baselines, and calls into the [analytics package](https://github.com/OpenStrap/analytics)
for each metric: resting HR, strain, HR zones, calories, sleep detection, sleep
regularity, workout detection, training load, fitness trend, readiness, the anomaly
signal, the coach plan, stress, nocturnal heart. The results land in `daily`, `sleep`,
`sessions`, and `baselines`. I keep this on a trailing window per day so the numbers
actually move day to day instead of collapsing into one flat value, which was a real bug
early on.

Every number comes back wrapped: a value, a unit, a confidence between 0 and 1, a tier,
and a label. If the inputs aren't there, the value is `null` and the confidence is `0`. I
would rather show you a dash than make something up. The whole project falls apart the
moment it starts inventing numbers, so it doesn't.

## What's in src

| File | What it's for |
|------|---------------|
| `index.ts` | The Hono app, the route table, and the cron handler |
| `auth.ts` | JWT signing/verifying, the email OTP flow, sending mail |
| `ingest.ts` | The whole ingest path above |
| `decode.ts` | Hex frames into decoded samples |
| `rollup.ts` | Decoded samples into per-minute buckets |
| `analytics.ts` | The cron brain, `processUser` / `runAnalytics` |
| `queue.ts` | The queue consumer: one bounded `(user, job, day)` unit per invocation, and the wake-time HRV trigger |
| `biometrics.ts` | HRV (RMSSD/SDNN/LF-HF), recovery, stress, relative temp/SpO₂ — re-decoded from the R-R intervals in R2 |
| `steps_imu.ts` | Step counting from the wrist accelerometer (incremental + nightly true-up) |
| `query.ts` | The read endpoints: today, sleep, strain, trends, chart, history |
| `daydetail.ts` | Single-day drill-downs (the strain curve, the hypnogram, the stress band) |
| `history.ts` | Range aggregation and the calendar heatmap |
| `records.ts` | Personal bests, streaks, resting-HR drift |
| `journal.ts` | Your tags and notes, plus a correlation engine that looks for what your tags do to your numbers |
| `notifications.ts` | The notification feed and marking things read |
| `resp.ts` | Respiratory rate from the optical PPG record (gated, only when there's real PPG) |
| `seed.ts` | Synthetic data generator for testing, runs in phases to stay under the free-plan request cap |
| `db/schema.sql` | The whole database, idempotent so you can re-run it |

## The API

Everything except sign-up needs `Authorization: Bearer <access_jwt>`, and you only ever
see your own data, scoped by the user id baked into the token. The `/admin/*` routes need
the admin token instead.

Sign in is passwordless: `POST /auth/register`, then `/auth/request-otp` mails you a
six-digit code, then `/auth/verify-otp` trades the code for an access token (24h) and a
refresh token (30d). `/auth/refresh` rotates them. If email isn't configured the code
comes back in the response so you never get locked out during setup.

Pushing data: `POST /ingest/batch` with `{device_id, records: [hex...]}` and
`/ingest/events` for the device events.

Reading it back: `/today`, `/sleep`, `/strain`, `/sessions`, `/trends`, `/chart`,
`/history`, the `/day/{strain,sleep,timeline,stress}` drill-downs, `/records`, `/journal`
(and `/journal/insights`), and `/notifications`. There's a `/profile` you can GET and
PATCH.

Admin stuff for when you run your own: `/admin/run-analytics`, `/admin/run-resp`,
`/admin/seed-demo`, `/admin/issue-token`, `/admin/wipe-raw`, `/admin/prune`.

## The database

Thirteen tables. The ones you'll care about: `minute` (the running-sum rollups, pruned at
90 days), `daily` and `sleep` and `sessions` (the derived output, mostly JSON columns for
the structured bits like coach plans and HR zones), `baselines` (your resting HR, max HR,
sleep need, the anchors everything else is measured against), and the auth trio (`users`,
`otps`, `refresh_tokens`). Full DDL is in `src/db/schema.sql`, it's commented, go read it.

## Running your own

```bash
npm install
npx wrangler d1 create openstrap-db          # paste the id into wrangler.toml
npx wrangler r2 bucket create openstrap-raw
npx wrangler d1 execute openstrap-db --file src/db/schema.sql
npx wrangler secret put JWT_SECRET           # any long random string
npx wrangler secret put ADMIN_TOKEN          # another one, for /admin/*
npx wrangler deploy
```

If you want sign-in emails to actually send, add a `BREVO_API_KEY` or `RESEND_API_KEY`
and an `EMAIL_FROM`. Without them, sign-in still works, the code just comes back in the
API response. Secrets go through `wrangler secret` or `.dev.vars`, never into the repo.
The `wrangler.toml` in here has a placeholder where the D1 id goes, swap in yours.

## About your data, honestly

You can run all of this yourself. That's the whole point of it being open and the backend
URL being a setting, not a constant. Stand up your own Worker, your own D1, your own R2,
and your health data never touches a machine you don't own.

Or don't, and use mine. If you do: what am I going to do with your heart rate? Nothing. I
promise the only thing I'll ever do with it is make the decoders and the math better over
time. I'm not selling it, I'm not building a profile on you, I genuinely do not care that
you ran 5k on Tuesday. But you don't have to take my word for it, that's why the self-host
path exists.

## It's not finished

There are bugs in here. I know about some of them and not others. The stress thresholds
aren't well calibrated, the sleep stage estimator leans too hard on REM, respiratory rate
only shows up when there's live PPG to work with. If you find something wrong, open an
issue, I'll work through them. This gets better with more people poking at it, not less.
