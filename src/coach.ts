// coach.ts — self-hosted, OpenAI-compatible LLM endpoint for the in-app AI Coach,
// backed by Cloudflare Workers AI (the `AI` binding). No external provider and no
// OpenAI key: it runs on your own Worker within Cloudflare's free AI allocation.
//
// The app's AI Coach is a BYOK, agentic tool-caller that POSTs OpenAI-shaped
// `/chat/completions` (model, messages, tools, tool_choice) and reads back
// `choices[0].message` (content + tool_calls). Point its base URL at
// `<backend>/coach/v1` and set its API key to COACH_KEY. We translate that contract
// to `env.AI.run(...)` and map Workers AI's `{ response, tool_calls }` back to the
// OpenAI shape the coach expects.

export interface AiBinding {
  run(model: string, input: Record<string, unknown>): Promise<unknown>
}

export interface CoachEnv {
  AI: AiBinding
  COACH_KEY?: string
}

interface OpenAIToolCall {
  id: string
  type: 'function'
  function: { name: string; arguments: string }
}

interface OpenAIMessage {
  role: 'assistant'
  content: string | null
  tool_calls?: OpenAIToolCall[]
}

// Llama 3.1 8B (fast) is the default: ~0.4s/call and it reliably CONVERGES the
// agentic tool loop (answers in prose after a tool result). The 70B fp8 model is
// available but tends to re-call tools instead of converging here, so it's slower
// in practice for the multi-step coach. The client may pin any of these.
const DEFAULT_MODEL = '@cf/meta/llama-3.1-8b-instruct-fast'

const COACH_MODELS = [
  '@cf/meta/llama-3.1-8b-instruct-fast',
  '@cf/meta/llama-3.3-70b-instruct-fp8-fast',
  '@hf/nousresearch/hermes-2-pro-mistral-7b',
]

// Constant-time-ish bearer check against the configured COACH_KEY. Without a
// configured key the endpoint is closed (never open by default).
function authorized(c: any): boolean {
  const want = c.env.COACH_KEY
  if (!want) return false
  const auth = c.req.header('Authorization') ?? ''
  return auth.startsWith('Bearer ') && auth.slice(7) === want
}

// Pass tools through in OpenAI shape (`{ type:'function', function:{...} }`).
// Workers AI's OpenAI-compatible models validate this exact shape (the unwrapped
// Cloudflare form `{name,description,parameters}` is rejected by some models, e.g.
// llama-3.1-8b-instruct-fast). The coach already sends the OpenAI shape, so forward
// it unchanged.
function normalizeTools(tools: unknown): unknown[] | undefined {
  if (!Array.isArray(tools) || tools.length === 0) return undefined
  return tools
}

function extractText(out: any): string {
  if (typeof out === 'string') return out
  if (typeof out?.response === 'string') return out.response
  if (typeof out?.result?.response === 'string') return out.result.response
  return ''
}

function mapToolCalls(out: any): OpenAIToolCall[] | undefined {
  const raw = out?.tool_calls ?? out?.result?.tool_calls
  if (!Array.isArray(raw) || raw.length === 0) return undefined
  return raw.map((tc: any, i: number) => {
    const name = tc?.name ?? tc?.function?.name ?? ''
    const argsRaw = tc?.arguments ?? tc?.function?.arguments ?? {}
    const args = typeof argsRaw === 'string' ? argsRaw : JSON.stringify(argsRaw ?? {})
    return {
      id: tc?.id ?? `call_${Date.now()}_${i}`,
      type: 'function' as const,
      function: { name, arguments: args },
    }
  })
}

// GET /coach/v1/models — the coach lists models from here (OpenAI /models shape).
export async function coachModels(c: any) {
  if (!authorized(c)) return c.json({ error: { message: 'Unauthorized' } }, 401)
  return c.json({
    object: 'list',
    data: COACH_MODELS.map((id) => ({ id, object: 'model', owned_by: 'cloudflare' })),
  })
}

// POST /coach/v1/chat/completions — OpenAI-compatible, backed by Workers AI.
export async function coachChatCompletions(c: any) {
  if (!authorized(c)) return c.json({ error: { message: 'Unauthorized' } }, 401)

  let body: any
  try {
    body = await c.req.json()
  } catch {
    return c.json({ error: { message: 'Invalid JSON body' } }, 400)
  }

  const model = typeof body?.model === 'string' && body.model ? body.model : DEFAULT_MODEL
  const messages = Array.isArray(body?.messages) ? body.messages : []
  if (messages.length === 0) {
    return c.json({ error: { message: 'messages[] is required' } }, 400)
  }

  const input: Record<string, unknown> = {
    messages,
    temperature: typeof body?.temperature === 'number' ? body.temperature : 0.3,
    max_tokens: typeof body?.max_tokens === 'number' ? body.max_tokens : 1024,
  }
  const tools = normalizeTools(body?.tools)
  if (tools) input.tools = tools

  let out: unknown
  try {
    out = await c.env.AI.run(model, input)
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Workers AI request failed'
    return c.json({ error: { message: `Workers AI error: ${msg}` } }, 502)
  }

  const toolCalls = mapToolCalls(out)
  const message: OpenAIMessage = toolCalls
    ? { role: 'assistant', content: extractText(out) || null, tool_calls: toolCalls }
    : { role: 'assistant', content: extractText(out) }

  return c.json({
    id: `chatcmpl-${Date.now()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{
      index: 0,
      message,
      finish_reason: toolCalls ? 'tool_calls' : 'stop',
    }],
  })
}
