// Auth primitives for openstrap-backend: JWT (HMAC-SHA256 via Web Crypto, no deps),
// OTP generation/hashing, and Resend email delivery (with dev fallback).

const enc = new TextEncoder()

// ---------- base64url ----------
function b64urlEncode(bytes: Uint8Array): string {
  let bin = ''
  for (const b of bytes) bin += String.fromCharCode(b)
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}
function b64urlEncodeStr(s: string): string {
  return b64urlEncode(enc.encode(s))
}
function b64urlDecode(s: string): Uint8Array {
  s = s.replace(/-/g, '+').replace(/_/g, '/')
  while (s.length % 4) s += '='
  const bin = atob(s)
  const out = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
  return out
}

// ---------- HMAC key ----------
async function hmacKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    'raw', enc.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify'],
  )
}

// ---------- JWT (HS256) ----------
export interface JwtPayload {
  sub: string // user_id
  iat: number
  exp: number
  typ?: string
}

export async function signJwt(
  payload: Omit<JwtPayload, 'iat' | 'exp'>, secret: string, ttlSeconds: number,
): Promise<string> {
  const now = Math.floor(Date.now() / 1000)
  const full: JwtPayload = { ...payload, iat: now, exp: now + ttlSeconds }
  const header = b64urlEncodeStr(JSON.stringify({ alg: 'HS256', typ: 'JWT' }))
  const body = b64urlEncodeStr(JSON.stringify(full))
  const data = `${header}.${body}`
  const key = await hmacKey(secret)
  const sig = new Uint8Array(await crypto.subtle.sign('HMAC', key, enc.encode(data)))
  return `${data}.${b64urlEncode(sig)}`
}

export async function verifyJwt(token: string, secret: string): Promise<JwtPayload | null> {
  const parts = token.split('.')
  if (parts.length !== 3) return null
  const [header, body, sig] = parts
  const key = await hmacKey(secret)
  const ok = await crypto.subtle.verify(
    'HMAC', key, b64urlDecode(sig), enc.encode(`${header}.${body}`),
  )
  if (!ok) return null
  try {
    const payload = JSON.parse(new TextDecoder().decode(b64urlDecode(body))) as JwtPayload
    if (payload.exp < Math.floor(Date.now() / 1000)) return null
    return payload
  } catch {
    return null
  }
}

// ---------- hashing (OTP codes + refresh tokens) ----------
export async function sha256Hex(s: string): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', enc.encode(s)))
  return [...digest].map((b) => b.toString(16).padStart(2, '0')).join('')
}

// ---------- OTP ----------
export function generateOtp(): string {
  // 6-digit, cryptographically random, no modulo bias worth worrying about here.
  const buf = new Uint32Array(1)
  crypto.getRandomValues(buf)
  return (buf[0] % 1_000_000).toString().padStart(6, '0')
}

export function randomToken(): string {
  const buf = new Uint8Array(32)
  crypto.getRandomValues(buf)
  return b64urlEncode(buf)
}

export function uuid(): string {
  return crypto.randomUUID()
}

// ---------- OTP email (pluggable provider; dev-code fallback on miss/failure) ----------
// Provider selection: Brevo (single-sender, no domain) → Resend → dev fallback.
// On any send failure we return dev_code so sign-in never dead-ends.
export interface EmailEnv {
  BREVO_API_KEY?: string
  RESEND_API_KEY?: string
  EMAIL_FROM?: string      // sender email (Brevo: the verified single sender)
  EMAIL_FROM_NAME?: string
  RESEND_FROM?: string     // legacy "Name <email>" for Resend
}

const SUBJECT = 'Your OpenStrap code'
const bodyText = (code: string) =>
  `Your OpenStrap verification code is ${code}. It expires in 10 minutes.`

export async function sendOtpEmail(
  env: EmailEnv, to: string, code: string,
): Promise<{ delivered: boolean; dev_code?: string }> {
  const fromEmail = env.EMAIL_FROM ?? 'onboarding@resend.dev'
  const fromName = env.EMAIL_FROM_NAME ?? 'OpenStrap'

  try {
    if (env.BREVO_API_KEY) {
      const res = await fetch('https://api.brevo.com/v3/smtp/email', {
        method: 'POST',
        headers: { 'api-key': env.BREVO_API_KEY, 'Content-Type': 'application/json', accept: 'application/json' },
        body: JSON.stringify({
          sender: { email: fromEmail, name: fromName },
          to: [{ email: to }],
          subject: SUBJECT,
          textContent: bodyText(code),
        }),
      })
      if (!res.ok) {
        console.error('Brevo failed', res.status, await res.text())
        return { delivered: false, dev_code: code }
      }
      return { delivered: true }
    }

    if (env.RESEND_API_KEY) {
      const res = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          from: env.RESEND_FROM ?? `${fromName} <${fromEmail}>`,
          to,
          subject: SUBJECT,
          text: bodyText(code),
        }),
      })
      if (!res.ok) {
        console.error('Resend failed', res.status, await res.text())
        return { delivered: false, dev_code: code }
      }
      return { delivered: true }
    }
  } catch (e) {
    console.error('email send threw', e)
    return { delivered: false, dev_code: code }
  }

  // No provider configured — dev fallback.
  console.log(`[dev-otp] ${to} -> ${code}`)
  return { delivered: false, dev_code: code }
}
