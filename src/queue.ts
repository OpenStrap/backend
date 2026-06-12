// queue.ts — analytics queue consumer. Batched (≤20), dedup user_ids, per-user
// isolation (one user's failure never blocks others; retried → DLQ).

import { processUser } from './analytics'

export interface AnalyticsMessage {
  user_id: string
  upto?: number
}

interface QueueEnv {
  DB: D1Database
}

export async function handleQueueBatch(
  batch: MessageBatch<AnalyticsMessage>,
  env: QueueEnv,
): Promise<void> {
  // Dedup user_ids within the batch (multiple ingests for one user → one run).
  const byUser = new Map<string, Message<AnalyticsMessage>[]>()
  for (const msg of batch.messages) {
    const uid = msg.body.user_id
    if (!uid) { msg.ack(); continue }
    const arr = byUser.get(uid) ?? []
    arr.push(msg)
    byUser.set(uid, arr)
  }

  for (const [userId, msgs] of byUser) {
    try {
      await processUser(env.DB, userId)
      for (const m of msgs) m.ack()
    } catch (e) {
      console.error('queue: processUser failed for', userId, e)
      // Retry the whole group (Cloudflare re-delivers; max_retries → DLQ).
      for (const m of msgs) m.retry()
    }
  }
}
