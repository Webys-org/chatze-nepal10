/**
 * Chatze Nepal Edition - Independent Federated Edge Messaging Web App
 * 
 * Implements SYSTEM_DESIGN_CLOUDFLARE_ZERO_SETUP.md:
 * - 100% Free-Forever, Zero-Setup, One-Click Deploy on Cloudflare Workers
 * - Independent Node Architecture: Anyone deploys on their own worker/domain
 * - Zero-Config Asymmetric Cryptographic Federation (ECDSA P-256 WebCrypto)
 * - Remote Peer Verification & Handshake:
 *     - Verifies user exists on remote peer (/api/federation/verify-user)
 *     - Exchanges cryptographic identities (/api/federation/identity)
 *     - Signs requests with ECDSA P-256 private key (X-Federation-Signature)
 *     - Real-Time Friend Requests with Waiting for Approval & Accept/Decline via SSE
 * - Cross-Node Real-Time Messaging via Cloudflare Kathmandu (KTM) PoP:
 *     - Direct HTTP dispatch to peer node (/api/federation/receive-message)
 *     - Instant SSE broadcast on recipient's open browser
 * - Cloudflare D1 Native Database (5 GB Free SQLite Edge DB) with auto-migrating tables
 * - Automatic 90-Day Retention Scheduled Cron
 */

let dbMigrated = false

async function ensureColumn(db, table, colName, colDef) {
  try {
    const info = await db.prepare(`PRAGMA table_info("${table}")`).all()
    const cols = new Set((info.results || []).map((r) => (r.name || '').toLowerCase()))
    if (!cols.has(colName.toLowerCase())) {
      await db.prepare(`ALTER TABLE "${table}" ADD COLUMN ${colName} ${colDef}`).run()
    }
  } catch (err) {
    console.warn(`[Auto-Migrate Warn] ${table}.${colName}:`, err.message)
  }
}

async function ensureD1Tables(db) {
  if (dbMigrated || !db) return
  try {
    // 1. System Config (Section 9)
    await db.prepare('CREATE TABLE IF NOT EXISTS system_config (key TEXT PRIMARY KEY, value TEXT NOT NULL)').run().catch(() => {})

    // 2. Users Table (Section 4)
    await db.prepare(`CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      handle TEXT UNIQUE NOT NULL,
      display_name TEXT NOT NULL,
      password_hash TEXT NOT NULL,
      role TEXT DEFAULT 'user',
      created_at INTEGER NOT NULL
    )`).run().catch(() => {})
    await ensureColumn(db, 'users', 'handle', 'TEXT')
    await ensureColumn(db, 'users', 'display_name', 'TEXT')
    await ensureColumn(db, 'users', 'password_hash', 'TEXT')
    await ensureColumn(db, 'users', 'role', "TEXT DEFAULT 'user'")
    await ensureColumn(db, 'users', 'created_at', 'INTEGER')

    // 3. Conversations Table (Section 4)
    await db.prepare(`CREATE TABLE IF NOT EXISTS conversations (
      id TEXT PRIMARY KEY,
      user_a TEXT NOT NULL,
      user_b TEXT NOT NULL,
      last_message_snippet TEXT,
      last_message_at INTEGER NOT NULL,
      status TEXT DEFAULT 'active'
    )`).run().catch(() => {})
    await ensureColumn(db, 'conversations', 'user_a', 'TEXT')
    await ensureColumn(db, 'conversations', 'user_b', 'TEXT')
    await ensureColumn(db, 'conversations', 'userAId', 'TEXT')
    await ensureColumn(db, 'conversations', 'userBId', 'TEXT')
    await ensureColumn(db, 'conversations', 'last_message_snippet', 'TEXT')
    await ensureColumn(db, 'conversations', 'lastMessageSnippet', 'TEXT')
    await ensureColumn(db, 'conversations', 'last_message_at', 'INTEGER')
    await ensureColumn(db, 'conversations', 'lastMessageAt', 'TIMESTAMP')
    await ensureColumn(db, 'conversations', 'status', "TEXT DEFAULT 'active'")
    await ensureColumn(db, 'conversations', 'is_federated', 'INTEGER DEFAULT 0')
    await ensureColumn(db, 'conversations', 'peer_domain', 'TEXT')

    // 4. Messages Table (Section 4)
    await db.prepare(`CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      conversation_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      content TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )`).run().catch(() => {})
    await ensureColumn(db, 'messages', 'conversation_id', 'TEXT')
    await ensureColumn(db, 'messages', 'sender_id', 'TEXT')
    await ensureColumn(db, 'messages', 'content', 'TEXT')
    await ensureColumn(db, 'messages', 'created_at', 'INTEGER')
    await ensureColumn(db, 'messages', 'conversationId', 'TEXT')
    await ensureColumn(db, 'messages', 'senderId', 'TEXT')
    await ensureColumn(db, 'messages', 'body', 'TEXT')
    await ensureColumn(db, 'messages', 'createdAt', 'TIMESTAMP')

    // 5. Sessions Table
    await db.prepare(`CREATE TABLE IF NOT EXISTS session (
      id TEXT PRIMARY KEY NOT NULL,
      expiresAt TIMESTAMP,
      token TEXT NOT NULL UNIQUE,
      userId TEXT NOT NULL,
      createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`).run().catch(() => {})
    await ensureColumn(db, 'session', 'token', 'TEXT')
    await ensureColumn(db, 'session', 'userId', 'TEXT')
    await ensureColumn(db, 'session', 'expiresAt', 'TIMESTAMP')

    // 6. Cryptographic Federation Friendships (Section 9)
    await db.prepare(`CREATE TABLE IF NOT EXISTS federation_friendships (
      id TEXT PRIMARY KEY,
      local_user_id TEXT NOT NULL,
      remote_peer_url TEXT NOT NULL,
      remote_handle TEXT NOT NULL,
      remote_public_key TEXT,
      direction TEXT DEFAULT 'outgoing',
      status TEXT DEFAULT 'pending',
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )`).run().catch(() => {})
    await ensureColumn(db, 'federation_friendships', 'direction', "TEXT DEFAULT 'outgoing'")
    await ensureColumn(db, 'federation_friendships', 'status', "TEXT DEFAULT 'pending'")
    await ensureColumn(db, 'federation_friendships', 'updated_at', 'INTEGER')

    // Legacy table compatibility
    await db.prepare(`CREATE TABLE IF NOT EXISTS "user" (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE)`).run().catch(() => {})
    await ensureColumn(db, 'user', 'passwordHash', 'TEXT')
    await db.prepare(`CREATE TABLE IF NOT EXISTS profiles (userId TEXT PRIMARY KEY, username TEXT NOT NULL UNIQUE, displayName TEXT NOT NULL)`).run().catch(() => {})
    await ensureColumn(db, 'profiles', 'role', "TEXT DEFAULT 'user'")

    dbMigrated = true
  } catch (err) {
    console.warn('[D1 Auto-Migration Error]', err)
  }
}

// In-memory pub/sub for real-time dispatch across worker requests
const activeStreams = new Map()

function broadcastUserEvent(userId, event) {
  const listeners = activeStreams.get(userId)
  if (listeners) {
    const payload = `event: event\ndata: ${JSON.stringify(event)}\n\n`
    for (const send of listeners) {
      try { send(payload) } catch {}
    }
  }
}

function parseCookies(cookieHeader) {
  const list = {}
  if (!cookieHeader) return list
  cookieHeader.split(';').forEach((cookie) => {
    let [name, ...rest] = cookie.split('=')
    name = name?.trim()
    if (!name) return
    const value = rest.join('=').trim()
    list[name] = decodeURIComponent(value)
  })
  return list
}

async function getUserFromRequest(request, env) {
  const cookies = parseCookies(request.headers.get('Cookie'))
  const token = cookies.chatze_session
  if (!token || !env.DB) return null
  try {
    const sess = await env.DB.prepare(
      'SELECT s.userId, u.handle, u.display_name, u.role FROM session s JOIN users u ON s.userId = u.id WHERE s.token = ?'
    ).bind(token).first()
    if (sess) {
      return { id: sess.userId, username: sess.handle, displayName: sess.display_name, role: sess.role }
    }
    const prof = await env.DB.prepare(
      'SELECT s.userId, p.username, p.displayName, p.role FROM session s JOIN profiles p ON s.userId = p.userId WHERE s.token = ?'
    ).bind(token).first()
    if (prof) {
      return { id: prof.userId, username: prof.username, displayName: prof.displayName, role: prof.role }
    }
    return null
  } catch {
    return null
  }
}

// WebCrypto Cryptographic Helpers for Section 9 Asymmetric Federation
async function signPayloadWithInstanceKey(db, payloadObj) {
  try {
    const row = await db.prepare("SELECT value FROM system_config WHERE key = 'federation_private_key_jwk'").first()
    if (!row?.value) return null
    const jwk = JSON.parse(row.value)
    const privKey = await crypto.subtle.importKey(
      'jwk',
      jwk,
      { name: 'ECDSA', namedCurve: 'P-256' },
      false,
      ['sign']
    )
    const encoder = new TextEncoder()
    const dataBytes = encoder.encode(JSON.stringify(payloadObj))
    const sigBuf = await crypto.subtle.sign(
      { name: 'ECDSA', hash: { name: 'SHA-256' } },
      privKey,
      dataBytes
    )
    return btoa(String.fromCharCode(...new Uint8Array(sigBuf)))
  } catch (err) {
    console.error('[WebCrypto Sign Error]', err)
    return null
  }
}

async function verifySignatureWithPublicKey(spkiB64, signatureB64, payloadObj) {
  try {
    if (!spkiB64 || !signatureB64) return false
    const binaryKey = atob(spkiB64)
    const keyBytes = new Uint8Array(binaryKey.length)
    for (let i = 0; i < binaryKey.length; i++) keyBytes[i] = binaryKey.charCodeAt(i)

    const pubKey = await crypto.subtle.importKey(
      'spki',
      keyBytes.buffer,
      { name: 'ECDSA', namedCurve: 'P-256' },
      false,
      ['verify']
    )

    const binarySig = atob(signatureB64)
    const sigBytes = new Uint8Array(binarySig.length)
    for (let i = 0; i < binarySig.length; i++) sigBytes[i] = binarySig.charCodeAt(i)

    const encoder = new TextEncoder()
    const dataBytes = encoder.encode(JSON.stringify(payloadObj))

    return await crypto.subtle.verify(
      { name: 'ECDSA', hash: { name: 'SHA-256' } },
      pubKey,
      sigBytes.buffer,
      dataBytes
    )
  } catch (err) {
    console.error('[WebCrypto Verify Error]', err)
    return false
  }
}

export default {
  async fetch(request, env, ctx) {
    if (env.DB) {
      globalThis.env = env
      await ensureD1Tables(env.DB)
    }

    const url = new URL(request.url)
    const pathname = url.pathname

    // 1. Health & Discovery (/api/health)
    if (pathname === '/api/health') {
      return new Response(JSON.stringify({
        status: 'ok',
        app: 'Chatze Nepal Independent Messenger',
        platform: 'Cloudflare Workers (Kathmandu KTM Edge)',
        d1Ready: Boolean(env.DB),
        federationReady: true,
        timestamp: Date.now()
      }), {
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      })
    }

    // 2. Real-time Server-Sent Events Endpoint (/api/stream)
    if (pathname === '/api/stream' && request.method === 'GET') {
      const cfRay = request.headers.get('cf-ray') || ''
      const edgeRegion = cfRay ? `KTM-CF-${cfRay.slice(-4).toUpperCase()}` : 'Kathmandu (KTM) Edge'
      const since = url.searchParams.get('since')
      const userId = url.searchParams.get('userId') || 'current'

      let pingTimer = null
      let cycleTimer = null
      let sendFn = null

      const stream = new ReadableStream({
        async start(controller) {
          const encoder = new TextEncoder()
          sendFn = (text) => {
            try { controller.enqueue(encoder.encode(text)) } catch {}
          }

          sendFn(`event: ready\ndata: ${JSON.stringify({
            status: 'connected',
            edgeRegion,
            clientUserId: userId,
            maxDurationSec: 100,
            ts: Date.now()
          })}\n\n`)

          // Delta polling on reconnect
          if (since && env.DB) {
            try {
              const rows = await env.DB.prepare(
                'SELECT * FROM messages WHERE (created_at > ? OR createdAt > ?) ORDER BY rowid ASC LIMIT 50'
              ).bind(Number(since), new Date(Number(since)).toISOString()).all()
              if (rows?.results?.length) {
                for (const msg of rows.results) {
                  sendFn(`event: event\ndata: ${JSON.stringify({
                    type: 'message',
                    message: {
                      id: msg.id,
                      conversationId: msg.conversation_id || msg.conversationId,
                      senderId: msg.sender_id || msg.senderId,
                      body: msg.content || msg.body,
                      createdAt: msg.created_at || msg.createdAt
                    },
                    conversationId: msg.conversation_id || msg.conversationId
                  })}\n\n`)
                }
              }
            } catch {}
          }

          if (!activeStreams.has(userId)) activeStreams.set(userId, new Set())
          activeStreams.get(userId).add(sendFn)

          pingTimer = setInterval(() => {
            sendFn(`event: ping\ndata: ${Date.now()}\n\n: ping\n\n`)
          }, 8000)

          cycleTimer = setTimeout(() => {
            sendFn(`event: cycle\ndata: ${JSON.stringify({ reconnect: true, ts: Date.now() })}\n\n`)
            try { controller.close() } catch {}
          }, 95000)
        },
        cancel() {
          if (pingTimer) clearInterval(pingTimer)
          if (cycleTimer) clearTimeout(cycleTimer)
          if (activeStreams.has(userId) && sendFn) {
            activeStreams.get(userId).delete(sendFn)
          }
        }
      })

      return new Response(stream, {
        headers: {
          'Content-Type': 'text/event-stream; charset=utf-8',
          'Cache-Control': 'no-cache, no-transform, no-store',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no',
          'X-Edge-Region': edgeRegion,
          'Access-Control-Allow-Origin': '*'
        }
      })
    }

    // 3. First-Launch Setup Wizard API (/api/setup)
    if (pathname === '/api/setup') {
      if (request.method === 'GET') {
        let isInitialized = false
        let instanceName = 'Chatze Nepal Node'
        let adminHandle = ''
        if (env.DB) {
          try {
            const countUsers = await env.DB.prepare('SELECT COUNT(*) as cnt FROM users').first().catch(() => null)
            const countProfiles = await env.DB.prepare('SELECT COUNT(*) as cnt FROM profiles').first().catch(() => null)
            const total = (countUsers?.cnt || 0) + (countProfiles?.cnt || 0)
            isInitialized = total > 0

            const nameRow = await env.DB.prepare("SELECT value FROM system_config WHERE key = 'instance_name'").first().catch(() => null)
            if (nameRow?.value) instanceName = nameRow.value

            const adminUser = await env.DB.prepare("SELECT handle FROM users WHERE role = 'admin' LIMIT 1").first().catch(() => null)
            if (adminUser?.handle) adminHandle = adminUser.handle
          } catch (e) {
            console.warn('[Setup Check]', e)
          }
        }
        return new Response(JSON.stringify({
          initialized: isInitialized,
          instanceName,
          adminHandle,
          domain: url.host,
          storageEngine: 'Cloudflare D1 Native Database (5 GB Free)',
          edgeRegion: 'Kathmandu (KTM) Edge'
        }), {
          headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
        })
      }

      if (request.method === 'POST') {
        try {
          const body = await request.json().catch(() => ({}))
          const nodeName = (body.nodeName || 'Chatze Nepal Node').trim()
          const adminHandle = (body.adminHandle || '').trim().replace(/^@/, '').toLowerCase()
          const displayName = (body.displayName || nodeName || adminHandle).trim()
          const password = (body.password || '').trim()
          const now = Date.now()

          if (!adminHandle || !password) {
            return new Response(JSON.stringify({ ok: false, error: 'Handle and password are required' }), { status: 400, headers: { 'Content-Type': 'application/json' } })
          }

          if (!env.DB) {
            return new Response(JSON.stringify({ ok: false, error: 'Database binding (DB) is not attached. Please attach D1 binding in Cloudflare Dashboard.' }), {
              status: 500,
              headers: { 'Content-Type': 'application/json' }
            })
          }

          const userId = `usr_${crypto.randomUUID().slice(0, 8)}`

          // Insert into users
          await env.DB.prepare(
            'INSERT OR REPLACE INTO users (id, handle, display_name, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?, ?)'
          ).bind(userId, adminHandle, displayName, password, 'admin', now).run()

          // Sync compatibility
          await env.DB.prepare(
            'INSERT OR REPLACE INTO profiles (userId, username, displayName, role) VALUES (?, ?, ?, ?)'
          ).bind(userId, adminHandle, displayName, 'admin').run().catch(() => {})

          // Set instance name in system_config
          await env.DB.prepare(
            "INSERT OR REPLACE INTO system_config (key, value) VALUES ('instance_name', ?)"
          ).bind(nodeName).run()

          // Generate ECDSA P-256 keypair for Asymmetric Cryptographic Federation
          try {
            const keyPair = await crypto.subtle.generateKey(
              { name: 'ECDSA', namedCurve: 'P-256' },
              true,
              ['sign', 'verify']
            )
            const spki = await crypto.subtle.exportKey('spki', keyPair.publicKey)
            const pubB64 = btoa(String.fromCharCode(...new Uint8Array(spki)))
            const jwk = await crypto.subtle.exportKey('jwk', keyPair.privateKey)
            await env.DB.prepare(
              "INSERT OR REPLACE INTO system_config (key, value) VALUES ('federation_public_key', ?)"
            ).bind(pubB64).run()
            await env.DB.prepare(
              "INSERT OR REPLACE INTO system_config (key, value) VALUES ('federation_private_key_jwk', ?)"
            ).bind(JSON.stringify(jwk)).run()
          } catch (e) {
            console.warn('[ECDSA Keygen]', e)
          }

          // Welcome bot
          const conciergeId = 'chatze_nepal_bot'
          const convId = `conv_welcome_${crypto.randomUUID().slice(0, 8)}`
          await env.DB.prepare(
            'INSERT OR REPLACE INTO users (id, handle, display_name, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?, ?)'
          ).bind(conciergeId, 'chatze_bot', 'Chatze Nepal Concierge', 'bot', 'bot', now).run().catch(() => {})

          await env.DB.prepare(
            'INSERT OR IGNORE INTO conversations (id, user_a, user_b, userAId, userBId, status, last_message_snippet, last_message_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
          ).bind(convId, userId, conciergeId, userId, conciergeId, 'active', '🙏 Namaste! Welcome to your independent node.', now).run()

          await env.DB.prepare(
            'INSERT INTO messages (id, conversation_id, sender_id, content, created_at, conversationId, senderId, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
          ).bind(
            `msg_${crypto.randomUUID().slice(0, 8)}`,
            convId,
            conciergeId,
            `🙏 Namaste and welcome to your independent Chatze Nepal node!\n\n✨ Features Active:\n• 100% Free Edge Messaging on Cloudflare Kathmandu (KTM) PoP\n• 5 GB Free Native D1 Database (~25M messages)\n• Asymmetric ECDSA WebCrypto Federation Active\n• Connect to ANY other Chatze instance in Nepal by clicking "+ Connect Peer" (e.g. @friend at friend.workers.dev)!`,
            now,
            convId,
            conciergeId,
            `🙏 Namaste and welcome to your independent Chatze Nepal node!\n\n✨ Features Active:\n• 100% Free Edge Messaging on Cloudflare Kathmandu (KTM) PoP\n• 5 GB Free Native D1 Database (~25M messages)\n• Asymmetric ECDSA WebCrypto Federation Active\n• Connect to ANY other Chatze instance in Nepal by clicking "+ Connect Peer" (e.g. @friend at friend.workers.dev)!`
          ).run()

          // Create session
          const sessionToken = `sess_${crypto.randomUUID().replace(/-/g, '')}`
          const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString()
          await env.DB.prepare(
            'INSERT INTO session (id, expiresAt, token, userId) VALUES (?, ?, ?, ?)'
          ).bind(`s_${crypto.randomUUID().slice(0, 8)}`, expiresAt, sessionToken, userId).run()

          return new Response(JSON.stringify({
            ok: true,
            message: 'Independent node initialized successfully',
            adminHandle,
            nodeName,
            user: { id: userId, username: adminHandle, displayName, role: 'admin' },
            sessionToken
          }), {
            headers: {
              'Content-Type': 'application/json',
              'Set-Cookie': `chatze_session=${sessionToken}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`
            }
          })
        } catch (err) {
          console.error('[POST /api/setup]', err)
          return new Response(JSON.stringify({ ok: false, error: err.message || 'Setup error' }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          })
        }
      }
    }

    // 4. Authentication Endpoints (/api/auth/sign-in, /api/auth/sign-up, /api/auth/me, /api/auth/sign-out)
    if (pathname === '/api/auth/me') {
      const user = await getUserFromRequest(request, env)
      return new Response(JSON.stringify({ user, host: url.host }), { headers: { 'Content-Type': 'application/json' } })
    }

    if (pathname === '/api/auth/sign-in' && request.method === 'POST') {
      try {
        const body = await request.json().catch(() => ({}))
        const username = (body.username || '').trim().replace(/^@/, '').toLowerCase()
        const password = (body.password || '').trim()

        if (!env.DB) return new Response(JSON.stringify({ ok: false, error: 'Database not ready' }), { status: 500, headers: { 'Content-Type': 'application/json' } })

        const u = await env.DB.prepare(
          'SELECT id, handle, display_name, password_hash, role FROM users WHERE LOWER(handle) = ?'
        ).bind(username).first()

        if (!u) {
          return new Response(JSON.stringify({ ok: false, error: 'User not found. Please register or run setup.' }), { status: 401, headers: { 'Content-Type': 'application/json' } })
        }

        if (u.password_hash && u.password_hash !== password) {
          return new Response(JSON.stringify({ ok: false, error: 'Incorrect password' }), { status: 401, headers: { 'Content-Type': 'application/json' } })
        }

        const sessionToken = `sess_${crypto.randomUUID().replace(/-/g, '')}`
        const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString()
        await env.DB.prepare(
          'INSERT INTO session (id, expiresAt, token, userId) VALUES (?, ?, ?, ?)'
        ).bind(`s_${crypto.randomUUID().slice(0, 8)}`, expiresAt, sessionToken, u.id).run()

        return new Response(JSON.stringify({
          ok: true,
          user: { id: u.id, username: u.handle, displayName: u.display_name, role: u.role },
          sessionToken
        }), {
          headers: {
            'Content-Type': 'application/json',
            'Set-Cookie': `chatze_session=${sessionToken}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`
          }
        })
      } catch (err) {
        return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } })
      }
    }

    if (pathname === '/api/auth/sign-up' && request.method === 'POST') {
      try {
        const body = await request.json().catch(() => ({}))
        const username = (body.username || '').trim().replace(/^@/, '').toLowerCase()
        const displayName = (body.displayName || username).trim()
        const password = (body.password || '').trim()
        const now = Date.now()

        if (!username) return new Response(JSON.stringify({ ok: false, error: 'Username is required' }), { status: 400, headers: { 'Content-Type': 'application/json' } })
        if (!env.DB) return new Response(JSON.stringify({ ok: false, error: 'Database not ready' }), { status: 500, headers: { 'Content-Type': 'application/json' } })

        const existing = await env.DB.prepare('SELECT id FROM users WHERE LOWER(handle) = ?').bind(username).first()
        if (existing) return new Response(JSON.stringify({ ok: false, error: 'Username already taken on this node' }), { status: 400, headers: { 'Content-Type': 'application/json' } })

        const userId = `usr_${crypto.randomUUID().slice(0, 8)}`
        await env.DB.prepare(
          'INSERT INTO users (id, handle, display_name, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?, ?)'
        ).bind(userId, username, displayName, password, 'user', now).run()

        await env.DB.prepare(
          'INSERT OR IGNORE INTO profiles (userId, username, displayName, role) VALUES (?, ?, ?, ?)'
        ).bind(userId, username, displayName, 'user').run().catch(() => {})

        const sessionToken = `sess_${crypto.randomUUID().replace(/-/g, '')}`
        const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString()
        await env.DB.prepare(
          'INSERT INTO session (id, expiresAt, token, userId) VALUES (?, ?, ?, ?)'
        ).bind(`s_${crypto.randomUUID().slice(0, 8)}`, expiresAt, sessionToken, userId).run()

        return new Response(JSON.stringify({
          ok: true,
          user: { id: userId, username, displayName, role: 'user' },
          sessionToken
        }), {
          headers: {
            'Content-Type': 'application/json',
            'Set-Cookie': `chatze_session=${sessionToken}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`
          }
        })
      } catch (err) {
        return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } })
      }
    }

    if (pathname === '/api/auth/sign-out' && request.method === 'POST') {
      return new Response(JSON.stringify({ ok: true }), {
        headers: {
          'Content-Type': 'application/json',
          'Set-Cookie': 'chatze_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0'
        }
      })
    }

    // 5. Asymmetric Cryptographic Federation Endpoints (Section 9)

    // 5A. Public Identity Discovery (/api/federation/identity)
    if (pathname === '/api/federation/identity') {
      let pubKey = ''
      let instanceName = 'Chatze Nepal Node'
      if (env.DB) {
        try {
          const row = await env.DB.prepare("SELECT value FROM system_config WHERE key = 'federation_public_key'").first()
          if (row?.value) pubKey = row.value
          const nameRow = await env.DB.prepare("SELECT value FROM system_config WHERE key = 'instance_name'").first()
          if (nameRow?.value) instanceName = nameRow.value
        } catch {}
      }
      return new Response(JSON.stringify({
        instance_url: url.origin,
        host: url.host,
        name: instanceName,
        public_key: pubKey,
        algorithm: 'ECDSA-P256-SHA256',
        region: 'KTM',
        created_at: Math.floor(Date.now() / 1000)
      }), {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=86400'
        }
      })
    }

    // 5B. Verify User Exists on this Node (/api/federation/verify-user)
    if (pathname === '/api/federation/verify-user') {
      const handle = (url.searchParams.get('handle') || '').trim().replace(/^@/, '').toLowerCase()
      if (!handle || !env.DB) {
        return new Response(JSON.stringify({ exists: false }), {
          headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
        })
      }
      const u = await env.DB.prepare('SELECT id, handle, display_name FROM users WHERE LOWER(handle) = ?').bind(handle).first()
      return new Response(JSON.stringify({
        exists: Boolean(u),
        handle: u?.handle || handle,
        displayName: u?.display_name || handle,
        host: url.host
      }), {
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      })
    }

    // 5C. Remote Node POSTs Friend Request (/api/federation/friend-request)
    if (pathname === '/api/federation/friend-request' && request.method === 'POST') {
      try {
        const body = await request.json().catch(() => ({}))
        const { fromHandle, fromDomain, fromPublicKey, toHandle, timestamp } = body
        const signature = request.headers.get('X-Federation-Signature') || body.signature

        if (!fromHandle || !fromDomain || !toHandle) {
          return new Response(JSON.stringify({ ok: false, error: 'Missing federation parameters' }), { status: 400, headers: { 'Content-Type': 'application/json' } })
        }

        // Verify target user exists locally
        const targetUser = await env.DB.prepare('SELECT id, handle FROM users WHERE LOWER(handle) = ?').bind(toHandle.toLowerCase()).first()
        if (!targetUser) {
          return new Response(JSON.stringify({ ok: false, error: `User @${toHandle} does not exist on this node (${url.host})` }), { status: 404, headers: { 'Content-Type': 'application/json' } })
        }

        // Verify cryptographic signature if provided
        if (fromPublicKey && signature) {
          const isValid = await verifySignatureWithPublicKey(fromPublicKey, signature, { fromHandle, fromDomain, toHandle, timestamp })
          if (!isValid) {
            return new Response(JSON.stringify({ ok: false, error: 'Cryptographic signature verification failed' }), { status: 403, headers: { 'Content-Type': 'application/json' } })
          }
        }

        const friendshipId = `fed_${crypto.randomUUID().slice(0, 8)}`
        const now = Date.now()

        // Check if existing
        const existing = await env.DB.prepare(
          'SELECT id, status FROM federation_friendships WHERE local_user_id = ? AND remote_handle = ? AND remote_peer_url = ?'
        ).bind(targetUser.id, fromHandle, fromDomain).first()

        if (existing) {
          if (existing.status === 'active') {
            return new Response(JSON.stringify({ ok: true, status: 'active', message: 'Already friends' }), { headers: { 'Content-Type': 'application/json' } })
          }
        } else {
          await env.DB.prepare(
            'INSERT INTO federation_friendships (id, local_user_id, remote_peer_url, remote_handle, remote_public_key, direction, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
          ).bind(friendshipId, targetUser.id, fromDomain, fromHandle, fromPublicKey || '', 'incoming', 'pending', now, now).run()
        }

        // Dispatch real-time SSE notification to local user!
        broadcastUserEvent(targetUser.id, {
          type: 'friend_request',
          friendshipId: existing?.id || friendshipId,
          fromHandle,
          fromDomain,
          timestamp: now
        })

        return new Response(JSON.stringify({ ok: true, status: 'pending', message: 'Friend request received and waiting for approval' }), {
          headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
        })
      } catch (err) {
        return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } })
      }
    }

    // 5D. Client sends Friend Request to remote peer (/api/federation/connect)
    if (pathname === '/api/federation/connect' && request.method === 'POST') {
      try {
        const user = await getUserFromRequest(request, env)
        if (!user) return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' } })

        const body = await request.json().catch(() => ({}))
        let rawInput = (body.peerAddress || body.remoteHandle || '').trim()

        let remoteHandle = ''
        let peerDomain = (body.peerDomain || '').trim().replace(/^https?:\/\//, '').replace(/\/$/, '')

        // Support formats: "@suraj@chatze-nepal-new.askme50962.workers.dev" or handle + domain
        if (rawInput.includes('@') && rawInput.lastIndexOf('@') > 0) {
          const parts = rawInput.replace(/^@/, '').split('@')
          remoteHandle = parts[0].toLowerCase().trim()
          peerDomain = parts[1].replace(/^https?:\/\//, '').replace(/\/$/, '').trim()
        } else {
          remoteHandle = rawInput.replace(/^@/, '').toLowerCase().trim()
        }

        if (!remoteHandle || !peerDomain) {
          return new Response(JSON.stringify({ error: 'Please provide both the friend handle and their subdomain/domain.' }), { status: 400, headers: { 'Content-Type': 'application/json' } })
        }

        if (peerDomain.toLowerCase() === url.host.toLowerCase()) {
          return new Response(JSON.stringify({ error: 'This user is on your own local node. Select them from local users.' }), { status: 400, headers: { 'Content-Type': 'application/json' } })
        }

        const peerUrl = `https://${peerDomain}`

        // 1. Fetch remote instance's cryptographic identity
        const identRes = await fetch(`${peerUrl}/api/federation/identity`, { signal: AbortSignal.timeout(6000) }).catch(() => null)
        if (!identRes || !identRes.ok) {
          return new Response(JSON.stringify({ error: `Could not reach peer node at ${peerDomain}. Check that the domain is online and correct.` }), { status: 400, headers: { 'Content-Type': 'application/json' } })
        }
        const peerIdent = await identRes.json().catch(() => ({}))

        // 2. Verify remote user exists on that node
        const verifyRes = await fetch(`${peerUrl}/api/federation/verify-user?handle=${remoteHandle}`, { signal: AbortSignal.timeout(5000) }).catch(() => null)
        if (!verifyRes || !verifyRes.ok) {
          return new Response(JSON.stringify({ error: `Could not verify user @${remoteHandle} on ${peerDomain}.` }), { status: 400, headers: { 'Content-Type': 'application/json' } })
        }
        const verifyData = await verifyRes.json().catch(() => ({}))
        if (!verifyData.exists) {
          return new Response(JSON.stringify({ error: `User @${remoteHandle} does NOT exist on ${peerDomain}. Please double-check their handle.` }), { status: 404, headers: { 'Content-Type': 'application/json' } })
        }

        // 3. Sign the friend request payload with our instance private key
        const myPubKeyRow = await env.DB.prepare("SELECT value FROM system_config WHERE key = 'federation_public_key'").first()
        const myPubKey = myPubKeyRow?.value || ''
        const timestamp = Date.now()
        const reqPayload = {
          fromHandle: user.username,
          fromDomain: url.host,
          fromPublicKey: myPubKey,
          toHandle: remoteHandle,
          timestamp
        }
        const signature = await signPayloadWithInstanceKey(env.DB, reqPayload)

        // 4. Send the signed friend request to remote instance
        const reqRes = await fetch(`${peerUrl}/api/federation/friend-request`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Federation-Signature': signature || ''
          },
          body: JSON.stringify({ ...reqPayload, signature })
        }).catch((e) => ({ ok: false, error: e.message }))

        const reqJson = await reqRes.json?.().catch(() => ({}))
        if (!reqRes.ok || !reqJson?.ok) {
          return new Response(JSON.stringify({ error: reqJson?.error || 'Failed to dispatch friend request to peer node.' }), { status: 500, headers: { 'Content-Type': 'application/json' } })
        }

        // 5. Save outgoing friendship in local database as pending
        const friendshipId = `fed_${crypto.randomUUID().slice(0, 8)}`
        const now = Date.now()
        await env.DB.prepare(
          'INSERT OR REPLACE INTO federation_friendships (id, local_user_id, remote_peer_url, remote_handle, remote_public_key, direction, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
        ).bind(friendshipId, user.id, peerDomain, remoteHandle, peerIdent.public_key || '', 'outgoing', 'pending', now, now).run()

        return new Response(JSON.stringify({
          ok: true,
          status: 'pending',
          message: `Friend request sent to @${remoteHandle}@${peerDomain}. Waiting for approval!`,
          peer: {
            handle: remoteHandle,
            domain: peerDomain,
            name: verifyData.displayName || remoteHandle
          }
        }), { headers: { 'Content-Type': 'application/json' } })
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } })
      }
    }

    // 5E. List Pending & Active Friend Requests (/api/federation/requests)
    if (pathname === '/api/federation/requests' && request.method === 'GET') {
      try {
        const user = await getUserFromRequest(request, env)
        if (!user || !env.DB) return new Response(JSON.stringify({ incoming: [], outgoing: [] }), { headers: { 'Content-Type': 'application/json' } })

        const rows = await env.DB.prepare(
          'SELECT * FROM federation_friendships WHERE local_user_id = ? ORDER BY created_at DESC'
        ).bind(user.id).all()

        const incoming = (rows.results || []).filter(r => r.direction === 'incoming' && r.status === 'pending').map(r => ({
          id: r.id,
          remoteHandle: r.remote_handle,
          remoteDomain: r.remote_peer_url,
          createdAt: r.created_at
        }))

        const outgoing = (rows.results || []).filter(r => r.direction === 'outgoing' && r.status === 'pending').map(r => ({
          id: r.id,
          remoteHandle: r.remote_handle,
          remoteDomain: r.remote_peer_url,
          status: r.status,
          createdAt: r.created_at
        }))

        return new Response(JSON.stringify({ incoming, outgoing }), { headers: { 'Content-Type': 'application/json' } })
      } catch (err) {
        return new Response(JSON.stringify({ incoming: [], outgoing: [] }), { headers: { 'Content-Type': 'application/json' } })
      }
    }

    // 5F. Local User Accepts or Declines Friend Request (/api/federation/respond-request)
    if (pathname === '/api/federation/respond-request' && request.method === 'POST') {
      try {
        const user = await getUserFromRequest(request, env)
        if (!user || !env.DB) return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' } })

        const body = await request.json().catch(() => ({}))
        const { friendshipId, action } = body // action: 'accept' or 'decline'

        const friendship = await env.DB.prepare(
          'SELECT * FROM federation_friendships WHERE id = ? AND local_user_id = ?'
        ).bind(friendshipId, user.id).first()

        if (!friendship) {
          return new Response(JSON.stringify({ error: 'Friendship request not found' }), { status: 404, headers: { 'Content-Type': 'application/json' } })
        }

        const now = Date.now()

        if (action === 'accept') {
          // 1. Update status locally to active
          await env.DB.prepare(
            'UPDATE federation_friendships SET status = ?, updated_at = ? WHERE id = ?'
          ).bind('active', now, friendshipId).run()

          // 2. Create conversation
          const peerAddress = `${friendship.remote_handle}@${friendship.remote_peer_url}`
          const convId = `conv_fed_${crypto.randomUUID().slice(0, 8)}`
          await env.DB.prepare(
            'INSERT OR IGNORE INTO conversations (id, user_a, user_b, userAId, userBId, is_federated, peer_domain, status, last_message_snippet, last_message_at) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)'
          ).bind(convId, user.id, peerAddress, user.id, peerAddress, friendship.remote_peer_url, 'active', `🤝 Connected with @${peerAddress}`, now).run()

          // 3. Notify the remote peer instance
          const myPubKeyRow = await env.DB.prepare("SELECT value FROM system_config WHERE key = 'federation_public_key'").first()
          const acceptPayload = {
            fromHandle: user.username,
            fromDomain: url.host,
            toHandle: friendship.remote_handle,
            timestamp: now
          }
          const sig = await signPayloadWithInstanceKey(env.DB, acceptPayload)

          fetch(`https://${friendship.remote_peer_url}/api/federation/peer-accepted`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Federation-Signature': sig || '' },
            body: JSON.stringify(acceptPayload)
          }).catch(() => {})

          return new Response(JSON.stringify({ ok: true, status: 'active', conversationId: convId }), { headers: { 'Content-Type': 'application/json' } })
        } else {
          await env.DB.prepare('DELETE FROM federation_friendships WHERE id = ?').bind(friendshipId).run()
          return new Response(JSON.stringify({ ok: true, status: 'declined' }), { headers: { 'Content-Type': 'application/json' } })
        }
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } })
      }
    }

    // 5G. Remote Peer Notifies Us that Friend Request was Accepted (/api/federation/peer-accepted)
    if (pathname === '/api/federation/peer-accepted' && request.method === 'POST') {
      try {
        const body = await request.json().catch(() => ({}))
        const { fromHandle, fromDomain, toHandle } = body

        const targetUser = await env.DB.prepare('SELECT id FROM users WHERE LOWER(handle) = ?').bind(toHandle.toLowerCase()).first()
        if (!targetUser) return new Response(JSON.stringify({ error: 'User not found' }), { status: 404, headers: { 'Content-Type': 'application/json' } })

        const now = Date.now()

        // Update local outgoing friendship to active
        await env.DB.prepare(
          'UPDATE federation_friendships SET status = ?, updated_at = ? WHERE local_user_id = ? AND remote_handle = ? AND remote_peer_url = ?'
        ).bind('active', now, targetUser.id, fromHandle, fromDomain).run()

        // Create conversation locally
        const peerAddress = `${fromHandle}@${fromDomain}`
        const convId = `conv_fed_${crypto.randomUUID().slice(0, 8)}`
        await env.DB.prepare(
          'INSERT OR IGNORE INTO conversations (id, user_a, user_b, userAId, userBId, is_federated, peer_domain, status, last_message_snippet, last_message_at) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)'
        ).bind(convId, targetUser.id, peerAddress, targetUser.id, peerAddress, fromDomain, 'active', `🤝 Friend request accepted! You can now message @${peerAddress}`, now).run()

        // Dispatch SSE to notify browser
        broadcastUserEvent(targetUser.id, {
          type: 'friend_accepted',
          fromHandle,
          fromDomain,
          conversationId: convId
        })

        return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } })
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } })
      }
    }

    // 5H. Remote Node Delivers Incoming Message (/api/federation/receive-message)
    if (pathname === '/api/federation/receive-message' && request.method === 'POST') {
      try {
        const body = await request.json().catch(() => ({}))
        const { fromHandle, fromDomain, toHandle, text, messageId, timestamp } = body

        if (!fromHandle || !fromDomain || !toHandle || !text) {
          return new Response(JSON.stringify({ error: 'Missing parameters' }), { status: 400, headers: { 'Content-Type': 'application/json' } })
        }

        const targetUser = await env.DB.prepare('SELECT id, handle FROM users WHERE LOWER(handle) = ?').bind(toHandle.toLowerCase()).first()
        if (!targetUser) {
          return new Response(JSON.stringify({ error: 'Recipient user does not exist on this node' }), { status: 404, headers: { 'Content-Type': 'application/json' } })
        }

        const peerAddress = `${fromHandle}@${fromDomain}`
        const now = timestamp || Date.now()

        // Find or create conversation
        let conv = await env.DB.prepare(
          'SELECT id FROM conversations WHERE (user_a = ? AND user_b = ?) OR (user_a = ? AND user_b = ?)'
        ).bind(targetUser.id, peerAddress, peerAddress, targetUser.id).first()

        let convId = conv?.id
        if (!convId) {
          convId = `conv_fed_${crypto.randomUUID().slice(0, 8)}`
          await env.DB.prepare(
            'INSERT INTO conversations (id, user_a, user_b, userAId, userBId, is_federated, peer_domain, status, last_message_snippet, last_message_at) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)'
          ).bind(convId, targetUser.id, peerAddress, targetUser.id, peerAddress, fromDomain, 'active', text.slice(0, 100), now).run()
        } else {
          await env.DB.prepare(
            'UPDATE conversations SET last_message_snippet = ?, last_message_at = ?, lastMessageSnippet = ?, lastMessageAt = ? WHERE id = ?'
          ).bind(text.slice(0, 100), now, text.slice(0, 100), new Date(now).toISOString(), convId).run()
        }

        const msgId = messageId || `msg_${crypto.randomUUID().slice(0, 8)}`
        await env.DB.prepare(
          'INSERT INTO messages (id, conversation_id, sender_id, content, created_at, conversationId, senderId, body, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
        ).bind(msgId, convId, peerAddress, text, now, convId, peerAddress, text, new Date(now).toISOString()).run()

        const msgObj = {
          id: msgId,
          conversationId: convId,
          senderId: peerAddress,
          senderName: fromHandle,
          senderHandle: peerAddress,
          body: text,
          createdAt: now
        }

        // Deliver through real-time SSE stream!
        broadcastUserEvent(targetUser.id, {
          type: 'message',
          message: msgObj,
          conversationId: convId
        })

        return new Response(JSON.stringify({ ok: true, messageId: msgId }), {
          headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
        })
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } })
      }
    }

    // 6. Messaging & Conversation APIs
    if (pathname === '/api/conversations' && request.method === 'GET') {
      try {
        const user = await getUserFromRequest(request, env)
        const currentUserId = user?.id || url.searchParams.get('userId')
        if (!currentUserId || !env.DB) {
          return new Response(JSON.stringify({ conversations: [] }), { headers: { 'Content-Type': 'application/json' } })
        }

        const rows = await env.DB.prepare(
          `SELECT c.id, c.user_a, c.user_b, c.userAId, c.userBId, c.is_federated, c.peer_domain, c.status,
                  c.last_message_snippet, c.lastMessageSnippet, c.last_message_at, c.lastMessageAt,
                  uA.display_name as uAName, uA.handle as uAHandle,
                  uB.display_name as uBName, uB.handle as uBHandle
           FROM conversations c
           LEFT JOIN users uA ON (c.user_a = uA.id OR c.userAId = uA.id)
           LEFT JOIN users uB ON (c.user_b = uB.id OR c.userBId = uB.id)
           WHERE c.user_a = ? OR c.user_b = ? OR c.userAId = ? OR c.userBId = ?
           ORDER BY c.rowid DESC LIMIT 100`
        ).bind(currentUserId, currentUserId, currentUserId, currentUserId).all()

        const list = (rows.results || []).map((r) => {
          const userA = r.user_a || r.userAId
          const userB = r.user_b || r.userBId
          const isA = userA === currentUserId
          const otherId = isA ? userB : userA
          const otherName = isA ? (r.uBName || r.uBHandle || otherId) : (r.uAName || r.uAHandle || otherId)
          const otherHandle = isA ? (r.uBHandle || otherId) : (r.uAHandle || otherId)
          return {
            id: r.id,
            otherUserId: otherId,
            otherName: otherName,
            otherHandle: otherHandle,
            isFederated: Boolean(r.is_federated),
            peerDomain: r.peer_domain || '',
            lastSnippet: r.last_message_snippet || r.lastMessageSnippet || 'No messages yet',
            status: r.status,
            updatedAt: r.last_message_at || r.lastMessageAt || Date.now()
          }
        })

        return new Response(JSON.stringify({ conversations: list }), { headers: { 'Content-Type': 'application/json' } })
      } catch (err) {
        return new Response(JSON.stringify({ conversations: [], error: err.message }), { headers: { 'Content-Type': 'application/json' } })
      }
    }

    if (pathname === '/api/messages' && request.method === 'GET') {
      try {
        const conversationId = url.searchParams.get('conversationId')
        if (!conversationId || !env.DB) return new Response(JSON.stringify({ messages: [] }), { headers: { 'Content-Type': 'application/json' } })

        const rows = await env.DB.prepare(
          `SELECT m.id, m.conversation_id, m.conversationId, m.sender_id, m.senderId, m.content, m.body, m.created_at, m.createdAt,
                  u.display_name as senderName, u.handle as senderHandle
           FROM messages m
           LEFT JOIN users u ON (m.sender_id = u.id OR m.senderId = u.id)
           WHERE m.conversation_id = ? OR m.conversationId = ?
           ORDER BY m.rowid ASC LIMIT 200`
        ).bind(conversationId, conversationId).all()

        const list = (rows.results || []).map(m => ({
          id: m.id,
          conversationId: m.conversation_id || m.conversationId,
          senderId: m.sender_id || m.senderId,
          senderName: m.senderName || m.sender_id || m.senderId,
          senderHandle: m.senderHandle || '',
          body: m.content || m.body || '',
          createdAt: m.created_at || m.createdAt || Date.now()
        }))

        return new Response(JSON.stringify({ messages: list }), { headers: { 'Content-Type': 'application/json' } })
      } catch (err) {
        return new Response(JSON.stringify({ messages: [], error: err.message }), { headers: { 'Content-Type': 'application/json' } })
      }
    }

    if (pathname === '/api/messaging' && request.method === 'POST') {
      try {
        const user = await getUserFromRequest(request, env)
        const body = await request.json().catch(() => ({}))
        const conversationId = body.conversationId
        const text = (body.body || body.content || '').trim()
        const senderId = user?.id || body.senderId

        if (!conversationId || !text || !senderId) {
          return new Response(JSON.stringify({ error: 'Missing conversationId, text, or sender' }), { status: 400, headers: { 'Content-Type': 'application/json' } })
        }

        if (!env.DB) return new Response(JSON.stringify({ error: 'D1 not ready' }), { status: 500, headers: { 'Content-Type': 'application/json' } })

        const msgId = `msg_${crypto.randomUUID().slice(0, 8)}`
        const now = Date.now()
        const nowIso = new Date().toISOString()

        await env.DB.prepare(
          'INSERT INTO messages (id, conversation_id, sender_id, content, created_at, conversationId, senderId, body, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
        ).bind(msgId, conversationId, senderId, text, now, conversationId, senderId, text, nowIso).run()

        await env.DB.prepare(
          'UPDATE conversations SET last_message_snippet = ?, last_message_at = ?, lastMessageSnippet = ?, lastMessageAt = ? WHERE id = ?'
        ).bind(text.slice(0, 100), now, text.slice(0, 100), nowIso, conversationId).run()

        const msgObj = { id: msgId, conversationId, senderId, body: text, createdAt: now }

        // Find conversation details
        const conv = await env.DB.prepare('SELECT user_a, user_b, userAId, userBId, is_federated, peer_domain FROM conversations WHERE id = ?').bind(conversationId).first()
        if (conv) {
          const uA = conv.user_a || conv.userAId
          const uB = conv.user_b || conv.userBId
          const recipientId = uA === senderId ? uB : uA

          // Real-time SSE dispatch to local sender
          broadcastUserEvent(senderId, { type: 'message', message: msgObj, conversationId })

          // If local recipient, dispatch to local user
          if (!conv.is_federated && !recipientId.includes('@')) {
            broadcastUserEvent(recipientId, { type: 'message', message: msgObj, conversationId })
          }

          // If federated peer, dispatch to remote node via HTTP POST!
          if (conv.is_federated || recipientId.includes('@')) {
            let remoteHandle = ''
            let peerDomain = conv.peer_domain || ''
            if (recipientId.includes('@')) {
              const parts = recipientId.split('@')
              remoteHandle = parts[0]
              if (!peerDomain) peerDomain = parts[1]
            }

            if (peerDomain && remoteHandle) {
              const myHandle = user?.username || 'user'
              const remotePayload = {
                fromHandle: myHandle,
                fromDomain: url.host,
                toHandle: remoteHandle,
                text,
                messageId: msgId,
                timestamp: now
              }
              const sig = await signPayloadWithInstanceKey(env.DB, remotePayload)

              // Asynchronously dispatch to remote instance
              ctx.waitUntil(
                fetch(`https://${peerDomain}/api/federation/receive-message`, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json', 'X-Federation-Signature': sig || '' },
                  body: JSON.stringify(remotePayload)
                }).catch((err) => console.error('[Federated Remote Send Error]', err))
              )
            }
          }
        }

        return new Response(JSON.stringify({ ok: true, message: msgObj }), { headers: { 'Content-Type': 'application/json' } })
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } })
      }
    }

    // 7. Retention Cron Endpoint (/api/cron/retention)
    if (pathname === '/api/cron/retention') {
      let purged = 0
      if (env.DB) {
        try {
          const ninetyDaysAgo = Date.now() - 90 * 24 * 60 * 60 * 1000
          const res = await env.DB.prepare(
            `DELETE FROM messages WHERE created_at < ? AND conversation_id IN 
             (SELECT id FROM conversations WHERE status = 'archived')`
          ).bind(ninetyDaysAgo).run().catch(() => null)
          purged = res?.meta?.changes || 0
        } catch {}
      }
      return new Response(JSON.stringify({ ok: true, purgedCount: purged }), { headers: { 'Content-Type': 'application/json' } })
    }

    // 8. Primary Application UI
    return new Response(renderChatzeAppHtml(), {
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-cache'
      }
    })
  },

  async scheduled(event, env, ctx) {
    if (!env.DB) return
    ctx.waitUntil((async () => {
      try {
        const ninetyDaysAgo = Date.now() - 90 * 24 * 60 * 60 * 1000
        await env.DB.prepare(
          `DELETE FROM messages WHERE created_at < ? AND conversation_id IN 
           (SELECT id FROM conversations WHERE status = 'archived')`
        ).bind(ninetyDaysAgo).run()
      } catch {}
    })())
  }
}

function renderChatzeAppHtml() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>Chatze Nepal - Independent Federated Messenger</title>
  <meta name="description" content="Nepal's Independent Federated Edge Messaging Web App with Asymmetric Cryptographic Handshakes">
  <link rel="icon" href="/icon.svg" type="image/svg+xml">
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');
    body { font-family: 'Plus Jakarta Sans', sans-serif; }
    .scrollbar-thin::-webkit-scrollbar { width: 5px; }
    .scrollbar-thin::-webkit-scrollbar-track { background: transparent; }
    .scrollbar-thin::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
  </style>
</head>
<body class="bg-slate-900 text-slate-100 min-h-screen flex flex-col antialiased select-none">

  <!-- TOP BAR -->
  <header class="bg-slate-950/90 backdrop-blur border-b border-slate-800 px-4 py-2.5 flex items-center justify-between sticky top-0 z-40">
    <div class="flex items-center gap-3">
      <div class="h-8 w-8 rounded-xl bg-gradient-to-tr from-emerald-600 to-teal-500 flex items-center justify-center font-black text-white shadow-md shadow-emerald-950">
        <i class="fa-solid fa-comments text-sm"></i>
      </div>
      <div>
        <div class="flex items-center gap-2">
          <span class="font-extrabold text-white text-base tracking-tight" id="headerTitle">Chatze Nepal</span>
          <span class="bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-[10px] font-semibold px-2 py-0.5 rounded-full flex items-center gap-1">
            <span class="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse"></span> KTM Edge Active
          </span>
        </div>
        <div class="text-[11px] text-slate-400 flex items-center gap-2">
          <span id="latencyBadge"><i class="fa-solid fa-bolt text-amber-400"></i> KTM: 14ms</span>
          <span>•</span>
          <span class="text-slate-400">ECDSA P-256 Federated</span>
        </div>
      </div>
    </div>

    <!-- Header Actions -->
    <div class="flex items-center gap-2" id="headerUserActions"></div>
  </header>

  <!-- MAIN APP CONTAINER -->
  <main class="flex-1 flex flex-col relative overflow-hidden" id="appRoot">
    <div class="flex-1 flex flex-col items-center justify-center p-6 text-center" id="loadingView">
      <div class="h-10 w-10 border-4 border-emerald-500/20 border-t-emerald-500 rounded-full animate-spin mb-3"></div>
      <h2 class="text-base font-bold text-white">Connecting to Kathmandu Node...</h2>
      <p class="text-xs text-slate-400 mt-1">Verifying Cloudflare D1 Native Database</p>
    </div>
  </main>

  <!-- CONNECT FEDERATED PEER MODAL -->
  <div id="peerModal" class="hidden fixed inset-0 z-50 bg-black/70 backdrop-blur-sm flex items-center justify-center p-4">
    <div class="w-full max-w-md bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-2xl">
      <div class="flex items-center justify-between mb-4">
        <div class="flex items-center gap-2 text-white font-bold text-base">
          <i class="fa-solid fa-satellite-dish text-emerald-400"></i>
          <span>Connect Federated Peer</span>
        </div>
        <button onclick="closePeerModal()" class="text-slate-400 hover:text-white text-sm"><i class="fa-solid fa-xmark"></i></button>
      </div>

      <p class="text-xs text-slate-400 mb-4 leading-relaxed">
        Connect to any friend or colleague running Chatze on their own Cloudflare Workers or domain.
      </p>

      <form id="peerConnectForm" class="space-y-3.5">
        <div>
          <label class="block text-xs font-semibold text-slate-300 mb-1">Friend's Federated Address or Handle</label>
          <input type="text" id="peerAddressInput" required placeholder="e.g. @suraj@chatze-nepal-new.askme50962.workers.dev" class="w-full bg-slate-800 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
          <p class="text-[10px] text-slate-400 mt-1">Enter their full address or specify separate handle and domain below.</p>
        </div>

        <div class="grid grid-cols-2 gap-2">
          <div>
            <label class="block text-[11px] font-semibold text-slate-400 mb-1">Handle (optional)</label>
            <input type="text" id="peerHandle" placeholder="suraj" class="w-full bg-slate-800/60 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none">
          </div>
          <div>
            <label class="block text-[11px] font-semibold text-slate-400 mb-1">Domain (optional)</label>
            <input type="text" id="peerDomain" placeholder="peer.workers.dev" class="w-full bg-slate-800/60 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none">
          </div>
        </div>

        <button type="submit" id="peerSubmitBtn" class="w-full py-2.5 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl text-sm transition-all shadow-lg shadow-emerald-950 flex items-center justify-center gap-2 cursor-pointer mt-2">
          <span>Verify & Send Friend Request</span>
          <i class="fa-solid fa-paper-plane text-xs"></i>
        </button>
      </form>
    </div>
  </div>

  <script>
    let currentUser = null;
    let setupInfo = null;
    let conversations = [];
    let activeConversation = null;
    let messages = [];
    let sseEventSource = null;
    let pendingRequests = { incoming: [], outgoing: [] };
    let activeTab = 'chats'; // 'chats' or 'requests'

    async function initApp() {
      try {
        const t0 = performance.now();
        const healthRes = await fetch('/api/health');
        const ping = Math.round(performance.now() - t0);
        document.getElementById('latencyBadge').innerHTML = '<i class="fa-solid fa-bolt text-emerald-400"></i> KTM: ' + ping + 'ms';

        const setupRes = await fetch('/api/setup');
        setupInfo = await setupRes.json();

        if (setupInfo.instanceName) {
          document.getElementById('headerTitle').textContent = setupInfo.instanceName;
        }

        if (!setupInfo.initialized) {
          renderSetupWizard();
          return;
        }

        const authRes = await fetch('/api/auth/me');
        const authData = await authRes.json();
        currentUser = authData.user;

        if (!currentUser) {
          renderAuthView('signin');
          return;
        }

        renderChatWorkspace();
      } catch (err) {
        console.error('Init error:', err);
        renderErrorView(err.message);
      }
    }

    // 1. FIRST BOOT SETUP WIZARD
    function renderSetupWizard() {
      const root = document.getElementById('appRoot');
      document.getElementById('headerUserActions').innerHTML = '';
      root.innerHTML = \`
        <div class="flex-1 flex items-center justify-center p-4 bg-gradient-to-b from-slate-900 via-slate-900 to-slate-950">
          <div class="w-full max-w-md bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-2xl">
            <div class="text-center mb-6">
              <div class="inline-flex p-3 rounded-2xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 mb-3">
                <i class="fa-solid fa-comments text-2xl"></i>
              </div>
              <h2 class="text-xl font-black text-white">First-Launch Node Setup</h2>
              <p class="text-xs text-slate-400 mt-1">Configure your independent Nepal messaging node in 30 seconds.</p>
            </div>

            <form id="setupForm" class="space-y-4">
              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Your Name / Node Name</label>
                <input type="text" id="setupNodeName" required placeholder="e.g. Yogesh or Kathmandu Hub" class="w-full bg-slate-800 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>

              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Your Unique Handle</label>
                <div class="flex rounded-xl bg-slate-800 border border-slate-700 overflow-hidden focus-within:border-emerald-500">
                  <span class="px-3 py-2.5 text-slate-500 text-sm font-bold bg-slate-800/80">@</span>
                  <input type="text" id="setupAdminHandle" required placeholder="yogesh" class="flex-1 bg-transparent px-2 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none">
                </div>
                <p class="text-[11px] text-slate-400 mt-1">Your federated address will be: <b class="text-emerald-400">@<span id="handlePreview">yourname</span>@\${window.location.host}</b></p>
              </div>

              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Your Password</label>
                <input type="password" id="setupAdminPass" required placeholder="••••••••••••" class="w-full bg-slate-800 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>

              <div class="p-3 bg-emerald-950/30 border border-emerald-800/40 rounded-xl text-[11px] text-emerald-300 flex items-start gap-2">
                <i class="fa-solid fa-shield-halved mt-0.5 text-emerald-400"></i>
                <span>Auto-generates your ECDSA P-256 asymmetric cryptographic keypair in Cloudflare D1 for secure peer-to-peer handshakes.</span>
              </div>

              <button type="submit" id="setupSubmitBtn" class="w-full py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl text-sm transition-all shadow-lg shadow-emerald-950 flex items-center justify-center gap-2 cursor-pointer">
                <span>Initialize Node & Start Messaging</span>
                <i class="fa-solid fa-arrow-right text-xs"></i>
              </button>
            </form>
          </div>
        </div>
      \`;

      const handleInput = document.getElementById('setupAdminHandle');
      const handlePreview = document.getElementById('handlePreview');
      handleInput.addEventListener('input', () => {
        handlePreview.textContent = handleInput.value.trim().toLowerCase().replace(/^@/, '') || 'yourname';
      });

      document.getElementById('setupForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const btn = document.getElementById('setupSubmitBtn');
        btn.disabled = true;
        btn.innerHTML = '<i class="fa-solid fa-circle-notch animate-spin"></i> Initializing D1 Tables...';

        try {
          const res = await fetch('/api/setup', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              nodeName: document.getElementById('setupNodeName').value,
              adminHandle: document.getElementById('setupAdminHandle').value,
              password: document.getElementById('setupAdminPass').value
            })
          });
          const data = await res.json();
          if (data.ok) {
            currentUser = data.user;
            initApp();
          } else {
            alert('Setup error: ' + (data.error || 'Failed to initialize D1 database'));
            btn.disabled = false;
            btn.innerHTML = 'Try Again';
          }
        } catch (err) {
          alert('Setup connection error: ' + err.message);
          btn.disabled = false;
          btn.innerHTML = 'Try Again';
        }
      });
    }

    // 2. AUTH VIEW
    function renderAuthView(tab = 'signin') {
      const root = document.getElementById('appRoot');
      document.getElementById('headerUserActions').innerHTML = '';
      root.innerHTML = \`
        <div class="flex-1 flex items-center justify-center p-4 bg-gradient-to-b from-slate-900 via-slate-900 to-slate-950">
          <div class="w-full max-w-sm bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-2xl">
            <div class="flex bg-slate-800 p-1 rounded-xl mb-5">
              <button onclick="renderAuthView('signin')" class="flex-1 py-1.5 text-xs font-bold rounded-lg transition-all \${tab === 'signin' ? 'bg-slate-700 text-white shadow' : 'text-slate-400 hover:text-white'}">Sign In</button>
              <button onclick="renderAuthView('signup')" class="flex-1 py-1.5 text-xs font-bold rounded-lg transition-all \${tab === 'signup' ? 'bg-slate-700 text-white shadow' : 'text-slate-400 hover:text-white'}">New User</button>
            </div>

            <div class="text-center mb-5">
              <h2 class="text-lg font-black text-white">\${tab === 'signin' ? 'Sign In to Your Node' : 'Register Account'}</h2>
              <p class="text-xs text-slate-400 mt-1">\${tab === 'signin' ? 'Enter your handle to access your messages' : 'Create an account on this local node'}</p>
            </div>

            <form id="authForm" class="space-y-4">
              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Handle</label>
                <div class="flex rounded-xl bg-slate-800 border border-slate-700 overflow-hidden focus-within:border-emerald-500">
                  <span class="px-3 py-2 text-slate-500 text-sm font-bold bg-slate-800/80">@</span>
                  <input type="text" id="authUsername" required placeholder="yourname" class="flex-1 bg-transparent px-2 py-2 text-sm text-white placeholder-slate-500 focus:outline-none">
                </div>
              </div>

              \${tab === 'signup' ? \`
              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Display Name</label>
                <input type="text" id="authDisplayName" required placeholder="Yogesh Sharma" class="w-full bg-slate-800 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>
              \` : ''}

              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Password</label>
                <input type="password" id="authPassword" required placeholder="••••••••••••" class="w-full bg-slate-800 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>

              <button type="submit" id="authSubmitBtn" class="w-full py-2.5 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl text-sm transition-all shadow-lg shadow-emerald-950 flex items-center justify-center gap-2 cursor-pointer mt-2">
                <span>\${tab === 'signin' ? 'Open Messenger' : 'Create Account'}</span>
                <i class="fa-solid fa-arrow-right text-xs"></i>
              </button>
            </form>
          </div>
        </div>
      \`;

      document.getElementById('authForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const btn = document.getElementById('authSubmitBtn');
        btn.disabled = true;
        btn.innerHTML = '<i class="fa-solid fa-circle-notch animate-spin"></i> Authenticating...';

        const endpoint = tab === 'signin' ? '/api/auth/sign-in' : '/api/auth/sign-up';
        const payload = {
          username: document.getElementById('authUsername').value,
          password: document.getElementById('authPassword').value
        };
        if (tab === 'signup') {
          payload.displayName = document.getElementById('authDisplayName').value;
        }

        try {
          const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
          const data = await res.json();
          if (data.ok) {
            currentUser = data.user;
            initApp();
          } else {
            alert(data.error || 'Authentication error');
            btn.disabled = false;
            btn.innerHTML = tab === 'signin' ? 'Sign In' : 'Create Account';
          }
        } catch (err) {
          alert('Error: ' + err.message);
          btn.disabled = false;
          btn.innerHTML = 'Try Again';
        }
      });
    }

    // 3. MAIN CHAT WORKSPACE
    async function renderChatWorkspace() {
      const incomingCount = pendingRequests.incoming.length;

      document.getElementById('headerUserActions').innerHTML = \`
        <button onclick="openPeerModal()" class="px-3 py-1.5 bg-emerald-600/30 hover:bg-emerald-600/50 border border-emerald-500/30 text-emerald-300 font-bold text-xs rounded-xl flex items-center gap-1.5 transition-colors cursor-pointer">
          <i class="fa-solid fa-satellite-dish text-[11px]"></i>
          <span>+ Connect Peer</span>
        </button>

        <div class="flex items-center gap-2 bg-slate-800/80 px-2.5 py-1 rounded-xl border border-slate-700 text-xs">
          <span class="h-2 w-2 rounded-full bg-emerald-400"></span>
          <span class="font-bold text-white">@\${currentUser.username}</span>
        </div>
        <button onclick="handleSignOut()" class="p-1.5 text-slate-400 hover:text-rose-400 transition-colors text-xs" title="Sign Out">
          <i class="fa-solid fa-arrow-right-from-bracket"></i>
        </button>
      \`;

      const root = document.getElementById('appRoot');
      root.innerHTML = \`
        <div class="flex-1 flex overflow-hidden">
          <!-- LEFT SIDEBAR -->
          <aside class="w-80 border-r border-slate-800 bg-slate-950 flex flex-col shrink-0">
            <!-- Search & Identity Card -->
            <div class="p-3 border-b border-slate-800/80 space-y-2">
              <div class="relative">
                <i class="fa-solid fa-magnifying-glass absolute left-3 top-2.5 text-slate-500 text-xs"></i>
                <input type="text" id="searchConvInput" oninput="filterConversations(this.value)" placeholder="Search conversations..." class="w-full bg-slate-900 border border-slate-800 rounded-xl pl-8 pr-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>

              <!-- Federated Address Card -->
              <div class="p-2.5 bg-slate-900 border border-slate-800 rounded-xl flex items-center justify-between text-[11px]">
                <div class="truncate text-slate-300">
                  <i class="fa-solid fa-fingerprint text-emerald-400 mr-1"></i> Address: <b>@\${currentUser.username}@\${window.location.host}</b>
                </div>
                <button onclick="copyFederatedAddress()" class="px-2 py-0.5 bg-slate-800 hover:bg-slate-700 text-emerald-300 text-[10px] font-bold rounded transition-colors cursor-pointer shrink-0 ml-1">
                  Copy
                </button>
              </div>

              <!-- Sidebar Tabs (Chats / Pending Requests) -->
              <div class="flex bg-slate-900 p-1 rounded-xl text-xs font-bold border border-slate-800/80">
                <button onclick="switchTab('chats')" id="tabChatsBtn" class="flex-1 py-1 rounded-lg transition-all bg-slate-800 text-white shadow">Chats</button>
                <button onclick="switchTab('requests')" id="tabRequestsBtn" class="flex-1 py-1 rounded-lg transition-all text-slate-400 hover:text-white flex items-center justify-center gap-1">
                  <span>Requests</span>
                  <span id="requestsBadge" class="hidden px-1.5 py-0.2 rounded-full text-[9px] bg-amber-500 text-slate-950 font-black">0</span>
                </button>
              </div>
            </div>

            <!-- List Container -->
            <div class="flex-1 overflow-y-auto scrollbar-thin p-2 space-y-1" id="sidebarListContainer">
              <div class="p-4 text-center text-xs text-slate-500">Loading...</div>
            </div>
          </aside>

          <!-- RIGHT PANE: ACTIVE CHAT -->
          <section class="flex-1 flex flex-col bg-slate-900 overflow-hidden" id="chatPane">
            <div class="flex-1 flex flex-col items-center justify-center p-6 text-center text-slate-400">
              <div class="h-16 w-16 rounded-2xl bg-slate-800/80 border border-slate-700 flex items-center justify-center text-emerald-400 text-2xl mb-3 shadow-inner">
                <i class="fa-regular fa-comment-dots"></i>
              </div>
              <h3 class="text-base font-bold text-white">Select a conversation or Connect a Peer</h3>
              <p class="text-xs text-slate-500 max-w-sm mt-1">Send messages to local users on this node or connect to any friend across Nepal using their subdomain or domain.</p>
              <button onclick="openPeerModal()" class="mt-4 px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs rounded-xl transition-all shadow-md shadow-emerald-950 flex items-center gap-2 cursor-pointer">
                <i class="fa-solid fa-satellite-dish"></i>
                <span>+ Connect Federated Peer</span>
              </button>
            </div>
          </section>
        </div>
      \`;

      await loadConversations();
      await loadPendingRequests();
      initSSE();
    }

    window.switchTab = function(tab) {
      activeTab = tab;
      const cBtn = document.getElementById('tabChatsBtn');
      const rBtn = document.getElementById('tabRequestsBtn');
      if (tab === 'chats') {
        cBtn.className = 'flex-1 py-1 rounded-lg transition-all bg-slate-800 text-white shadow';
        rBtn.className = 'flex-1 py-1 rounded-lg transition-all text-slate-400 hover:text-white flex items-center justify-center gap-1';
        renderConversationList();
      } else {
        rBtn.className = 'flex-1 py-1 rounded-lg transition-all bg-slate-800 text-white shadow flex items-center justify-center gap-1';
        cBtn.className = 'flex-1 py-1 rounded-lg transition-all text-slate-400 hover:text-white';
        renderRequestsList();
      }
    }

    async function loadPendingRequests() {
      try {
        const res = await fetch('/api/federation/requests');
        const data = await res.json();
        pendingRequests = data || { incoming: [], outgoing: [] };

        const badge = document.getElementById('requestsBadge');
        if (badge) {
          const count = pendingRequests.incoming.length;
          if (count > 0) {
            badge.textContent = count;
            badge.classList.remove('hidden');
          } else {
            badge.classList.add('hidden');
          }
        }

        if (activeTab === 'requests') {
          renderRequestsList();
        }
      } catch (err) {
        console.error('Error loading requests:', err);
      }
    }

    function renderRequestsList() {
      const container = document.getElementById('sidebarListContainer');
      if (!container) return;

      const inc = pendingRequests.incoming || [];
      const out = pendingRequests.outgoing || [];

      if (inc.length === 0 && out.length === 0) {
        container.innerHTML = \`
          <div class="p-6 text-center text-slate-500">
            <i class="fa-solid fa-inbox text-2xl mb-2 text-slate-600"></i>
            <p class="text-xs">No pending requests.</p>
            <p class="text-[10px] text-slate-600 mt-1">Connect peers by their handle and domain.</p>
          </div>
        \`;
        return;
      }

      let html = '';

      if (inc.length > 0) {
        html += '<div class="px-2 py-1 text-[11px] font-bold text-amber-400 uppercase tracking-wider">Incoming Requests</div>';
        html += inc.map(r => \`
          <div class="p-3 bg-slate-900 border border-amber-500/30 rounded-xl space-y-2">
            <div class="flex items-center gap-2">
              <div class="h-8 w-8 rounded-lg bg-amber-500/20 text-amber-300 font-bold flex items-center justify-center text-xs">
                \${r.remoteHandle.charAt(0).toUpperCase()}
              </div>
              <div class="min-w-0 flex-1">
                <div class="font-bold text-xs text-white truncate">@\${r.remoteHandle}</div>
                <div class="text-[10px] text-slate-400 truncate">\${r.remoteDomain}</div>
              </div>
            </div>
            <div class="flex items-center gap-2 pt-1">
              <button onclick="respondToRequest('\${r.id}', 'accept')" class="flex-1 py-1 bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs rounded-lg transition-colors cursor-pointer">
                Accept
              </button>
              <button onclick="respondToRequest('\${r.id}', 'decline')" class="flex-1 py-1 bg-slate-800 hover:bg-rose-900/60 text-slate-300 hover:text-rose-200 font-bold text-xs rounded-lg transition-colors cursor-pointer">
                Decline
              </button>
            </div>
          </div>
        \`).join('');
      }

      if (out.length > 0) {
        html += '<div class="px-2 pt-3 pb-1 text-[11px] font-bold text-slate-400 uppercase tracking-wider">Outgoing (Waiting Approval)</div>';
        html += out.map(r => \`
          <div class="p-3 bg-slate-900 border border-slate-800 rounded-xl flex items-center justify-between">
            <div class="min-w-0">
              <div class="font-bold text-xs text-white truncate">@\${r.remoteHandle}</div>
              <div class="text-[10px] text-slate-400 truncate">\${r.remoteDomain}</div>
            </div>
            <span class="text-[10px] text-amber-400 bg-amber-500/10 border border-amber-500/20 px-2 py-0.5 rounded-full flex items-center gap-1 font-semibold">
              <i class="fa-solid fa-hourglass-half text-[9px]"></i> Pending
            </span>
          </div>
        \`).join('');
      }

      container.innerHTML = html;
    }

    window.respondToRequest = async function(friendshipId, action) {
      try {
        const res = await fetch('/api/federation/respond-request', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ friendshipId, action })
        });
        const data = await res.json();
        if (data.ok) {
          await loadPendingRequests();
          await loadConversations();
          if (data.conversationId) {
            selectConversationById(data.conversationId);
          }
          switchTab('chats');
        } else {
          alert('Error: ' + (data.error || 'Failed to update request'));
        }
      } catch (err) {
        alert('Network error: ' + err.message);
      }
    }

    async function loadConversations() {
      try {
        const res = await fetch('/api/conversations');
        const data = await res.json();
        conversations = data.conversations || [];
        if (activeTab === 'chats') {
          renderConversationList();
        }

        if (conversations.length > 0 && !activeConversation) {
          selectConversation(conversations[0]);
        }
      } catch (err) {
        console.error('Error loading conversations:', err);
      }
    }

    function renderConversationList(items = conversations) {
      const container = document.getElementById('sidebarListContainer');
      if (!container) return;

      if (items.length === 0) {
        container.innerHTML = \`
          <div class="p-6 text-center text-slate-500">
            <i class="fa-solid fa-user-group text-2xl mb-2 text-slate-600"></i>
            <p class="text-xs">No active chats yet.</p>
            <button onclick="openPeerModal()" class="mt-3 px-3 py-1 bg-slate-800 hover:bg-slate-700 text-emerald-300 text-xs font-semibold rounded-lg">
              + Connect Peer
            </button>
          </div>
        \`;
        return;
      }

      container.innerHTML = items.map(c => {
        const isActive = activeConversation && activeConversation.id === c.id;
        return \`
          <div onclick="selectConversationById('\${c.id}')" class="p-3 rounded-xl cursor-pointer transition-all flex items-start gap-3 \${isActive ? 'bg-slate-800 border border-slate-700 shadow' : 'hover:bg-slate-900 border border-transparent'}">
            <div class="h-9 w-9 rounded-xl \${c.isFederated ? 'bg-indigo-950 border border-indigo-700/60 text-indigo-400' : 'bg-slate-800 border border-slate-700 text-emerald-400'} flex items-center justify-center font-bold text-xs shrink-0">
              \${(c.otherName || c.otherHandle || 'U').charAt(0).toUpperCase()}
            </div>
            <div class="flex-1 min-w-0">
              <div class="flex items-center justify-between">
                <span class="font-bold text-xs text-white truncate">\${c.otherName || c.otherHandle}</span>
                \${c.isFederated ? '<span class="text-[9px] bg-indigo-900/60 text-indigo-300 border border-indigo-700/50 px-1 py-0.5 rounded font-mono">Peer</span>' : '<span class="text-[9px] text-emerald-400 font-mono">Local</span>'}
              </div>
              <p class="text-[11px] text-slate-400 truncate mt-0.5">\${c.lastSnippet}</p>
            </div>
          </div>
        \`;
      }).join('');
    }

    window.filterConversations = function(query) {
      query = (query || '').toLowerCase().trim();
      if (!query) {
        renderConversationList(conversations);
        return;
      }
      const filtered = conversations.filter(c => 
        (c.otherName && c.otherName.toLowerCase().includes(query)) ||
        (c.otherHandle && c.otherHandle.toLowerCase().includes(query)) ||
        (c.lastSnippet && c.lastSnippet.toLowerCase().includes(query))
      );
      renderConversationList(filtered);
    }

    window.selectConversationById = function(id) {
      const conv = conversations.find(c => c.id === id);
      if (conv) selectConversation(conv);
    }

    async function selectConversation(conv) {
      activeConversation = conv;
      renderConversationList();

      const pane = document.getElementById('chatPane');
      pane.innerHTML = \`
        <!-- Header -->
        <div class="px-4 py-3 bg-slate-950/70 border-b border-slate-800 flex items-center justify-between">
          <div class="flex items-center gap-3">
            <div class="h-9 w-9 rounded-xl \${conv.isFederated ? 'bg-indigo-950 border border-indigo-700/60 text-indigo-400' : 'bg-emerald-500/10 border border-emerald-500/20 text-emerald-400'} flex items-center justify-center font-bold text-sm">
              \${(conv.otherName || conv.otherHandle || 'C').charAt(0).toUpperCase()}
            </div>
            <div>
              <div class="flex items-center gap-2">
                <span class="font-bold text-sm text-white">\${conv.otherName}</span>
                <span class="text-xs text-slate-400 font-mono">@\${conv.otherHandle}</span>
              </div>
              <div class="text-[11px] text-slate-400 flex items-center gap-1.5">
                \${conv.isFederated 
                  ? '<span class="text-indigo-400 font-semibold flex items-center gap-1"><i class="fa-solid fa-satellite-dish text-[10px]"></i> Federated Peer: ' + (conv.peerDomain || conv.otherHandle.split('@')[1] || '') + '</span>' 
                  : '<span class="text-emerald-400 font-semibold flex items-center gap-1"><span class="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse"></span> Local Node Messenger</span>'}
              </div>
            </div>
          </div>

          <div class="flex items-center gap-2">
            <span class="text-[10px] bg-slate-800 border border-slate-700 text-slate-400 px-2 py-1 rounded-lg">
              <i class="fa-solid fa-lock text-emerald-400 mr-1"></i> Asymmetric Verified
            </span>
          </div>
        </div>

        <!-- Messages Feed -->
        <div class="flex-1 overflow-y-auto scrollbar-thin p-4 space-y-3" id="messagesFeed">
          <div class="text-center text-xs text-slate-500 my-4">Loading messages...</div>
        </div>

        <!-- Message Input -->
        <div class="p-3 bg-slate-950 border-t border-slate-800">
          <form id="msgForm" class="flex items-center gap-2">
            <input type="text" id="msgInput" required placeholder="Type message in Nepali or English..." class="flex-1 bg-slate-900 border border-slate-800 rounded-xl px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
            <button type="submit" id="msgSendBtn" class="px-4 py-2.5 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl text-sm transition-all shadow-md shadow-emerald-950 flex items-center gap-2 cursor-pointer">
              <span>Send</span>
              <i class="fa-solid fa-paper-plane text-xs"></i>
            </button>
          </form>
        </div>
      \`;

      document.getElementById('msgForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const input = document.getElementById('msgInput');
        const text = input.value.trim();
        if (!text) return;
        input.value = '';

        try {
          const res = await fetch('/api/messaging', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              conversationId: activeConversation.id,
              body: text,
              senderId: currentUser.id
            })
          });
          const data = await res.json();
          if (data.ok) {
            messages.push(data.message);
            renderMessages();
            loadConversations();
          }
        } catch (err) {
          console.error('Send error:', err);
        }
      });

      await loadMessages(conv.id);
    }

    async function loadMessages(convId) {
      try {
        const res = await fetch('/api/messages?conversationId=' + convId);
        const data = await res.json();
        messages = data.messages || [];
        renderMessages();
      } catch (err) {
        console.error('Error loading messages:', err);
      }
    }

    function renderMessages() {
      const feed = document.getElementById('messagesFeed');
      if (!feed) return;

      if (messages.length === 0) {
        feed.innerHTML = '<div class="text-center text-xs text-slate-500 my-8">No messages in this chat yet. Send a message to start!</div>';
        return;
      }

      feed.innerHTML = messages.map(m => {
        const isMine = m.senderId === currentUser.id || m.senderHandle === currentUser.username;
        return \`
          <div class="flex flex-col \${isMine ? 'items-end' : 'items-start'}">
            <div class="max-w-[75%] rounded-2xl px-4 py-2.5 text-xs \${isMine ? 'bg-emerald-600 text-white rounded-br-none shadow-md shadow-emerald-950' : 'bg-slate-800 text-slate-100 rounded-bl-none border border-slate-700/80'}">
              <p class="whitespace-pre-wrap leading-relaxed">\${escapeHtml(m.body)}</p>
            </div>
            <span class="text-[10px] text-slate-500 mt-1 px-1">\${formatTime(m.createdAt)}</span>
          </div>
        \`;
      }).join('');

      feed.scrollTop = feed.scrollHeight;
    }

    function initSSE() {
      if (sseEventSource) sseEventSource.close();
      const sseUrl = '/api/stream?userId=' + currentUser.id;
      sseEventSource = new EventSource(sseUrl);

      sseEventSource.addEventListener('event', (e) => {
        try {
          const payload = JSON.parse(e.data);

          if (payload.type === 'message') {
            if (activeConversation && activeConversation.id === payload.conversationId) {
              // Avoid duplicate if already rendered
              if (!messages.some(m => m.id === payload.message.id)) {
                messages.push(payload.message);
                renderMessages();
              }
            }
            loadConversations();
          }

          if (payload.type === 'friend_request') {
            loadPendingRequests();
            alert('🔔 Incoming Friend Request from @' + payload.fromHandle + '@' + payload.fromDomain);
          }

          if (payload.type === 'friend_accepted') {
            loadPendingRequests();
            loadConversations();
            alert('🎉 @' + payload.fromHandle + '@' + payload.fromDomain + ' accepted your friend request! You can now message each other.');
            if (payload.conversationId) {
              selectConversationById(payload.conversationId);
            }
          }
        } catch {}
      });

      sseEventSource.addEventListener('cycle', () => {
        sseEventSource.close();
        setTimeout(initSSE, 500);
      });

      sseEventSource.onerror = () => {
        sseEventSource.close();
        setTimeout(initSSE, 3000);
      };
    }

    window.openPeerModal = function() {
      document.getElementById('peerModal').classList.remove('hidden');
    }
    window.closePeerModal = function() {
      document.getElementById('peerModal').classList.add('hidden');
    }

    document.getElementById('peerConnectForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const btn = document.getElementById('peerSubmitBtn');
      btn.disabled = true;
      btn.innerHTML = '<i class="fa-solid fa-circle-notch animate-spin"></i> Verifying Remote Peer...';

      const peerAddress = document.getElementById('peerAddressInput').value;
      const peerHandle = document.getElementById('peerHandle').value;
      const peerDomain = document.getElementById('peerDomain').value;

      try {
        const res = await fetch('/api/federation/connect', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ peerAddress, remoteHandle: peerHandle, peerDomain: peerDomain })
        });
        const data = await res.json();
        if (data.ok) {
          alert('Success: ' + data.message);
          closePeerModal();
          await loadPendingRequests();
          switchTab('requests');
        } else {
          alert('Federation error: ' + (data.error || 'Failed to connect with peer'));
        }
      } catch (err) {
        alert('Network error: ' + err.message);
      } finally {
        btn.disabled = false;
        btn.innerHTML = '<span>Verify & Send Friend Request</span><i class="fa-solid fa-paper-plane text-xs"></i>';
      }
    });

    window.copyFederatedAddress = function() {
      const addr = '@' + currentUser.username + '@' + window.location.host;
      navigator.clipboard.writeText(addr);
      alert('Copied your federated address:\\n' + addr + '\\n\\nGive this to anyone running Chatze in Nepal to connect!');
    }

    window.handleSignOut = async function() {
      if (confirm('Sign out of Chatze?')) {
        await fetch('/api/auth/sign-out', { method: 'POST' });
        window.location.reload();
      }
    }

    function escapeHtml(str) {
      if (!str) return '';
      return str.replace(/[&<>"']/g, m => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;'
      }[m]));
    }

    function formatTime(val) {
      if (!val) return '';
      try {
        const d = typeof val === 'number' ? new Date(val) : new Date(val);
        return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      } catch {
        return '';
      }
    }

    function renderErrorView(msg) {
      document.getElementById('appRoot').innerHTML = \`
        <div class="flex-1 flex flex-col items-center justify-center p-6 text-center">
          <i class="fa-solid fa-triangle-exclamation text-rose-500 text-3xl mb-3"></i>
          <h2 class="text-base font-bold text-white">Node Connection Error</h2>
          <p class="text-xs text-slate-400 mt-1 max-w-md">\${msg}</p>
          <button onclick="window.location.reload()" class="mt-4 px-4 py-2 bg-slate-800 hover:bg-slate-700 text-white text-xs font-semibold rounded-xl">Reload</button>
        </div>
      \`;
    }

    initApp();
  </script>
</body>
</html>`
}
