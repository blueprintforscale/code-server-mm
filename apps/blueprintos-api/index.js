require('dotenv').config();
const fastify = require('fastify')({ logger: true });
const cors = require('@fastify/cors');
const { Pool } = require('pg');

const pool = new Pool({
  user: 'blueprint',
  database: 'blueprint',
  host: 'localhost',
  port: 5432,
});

const ALLOWED_ORIGINS = [
  'https://blueprintos.vercel.app',
  /\.vercel\.app$/,
  'http://localhost:3000',
];

const bcrypt = require('bcrypt');

const API_KEY = process.env.BLUEPRINTOS_API_KEY;
const SALT_ROUNDS = 12;

fastify.register(cors, {
  origin: ALLOWED_ORIGINS,
  methods: ['GET', 'POST', 'PUT', 'OPTIONS'],
});

// API key auth
fastify.addHook('onRequest', async (request, reply) => {
  if (request.url === '/health') return;

  const key = request.headers['x-api-key'];
  if (!API_KEY || key !== API_KEY) {
    reply.code(401).send({ error: 'Unauthorized' });
  }
});

// Health check
fastify.get('/health', async () => ({ status: 'ok' }));

// List clients
fastify.get('/clients', async () => {
  const { rows } = await pool.query(
    'SELECT id, name, google_ads_id, callrail_company_id FROM clients ORDER BY name'
  );
  return rows;
});

// Client detail
fastify.get('/clients/:id', async (request) => {
  const { id } = request.params;
  const { rows } = await pool.query(
    'SELECT * FROM clients WHERE id = $1',
    [id]
  );
  if (rows.length === 0) return { error: 'Not found' };
  return rows[0];
});

// Call stats for a client
fastify.get('/clients/:id/calls', async (request) => {
  const { id } = request.params;
  const { days = 30 } = request.query;
  const { rows } = await pool.query(
    `SELECT date_trunc('day', start_time) AS day,
            COUNT(*) AS total_calls,
            COUNT(*) FILTER (WHERE answered = true) AS answered,
            COUNT(*) FILTER (WHERE classification = 'legitimate') AS legitimate,
            COUNT(*) FILTER (WHERE classification = 'spam') AS spam,
            COUNT(*) FILTER (WHERE first_call = true) AS first_time
     FROM calls
     WHERE client_id = $1
       AND start_time >= NOW() - make_interval(days => $2::int)
     GROUP BY day ORDER BY day`,
    [id, days]
  );
  return rows;
});

// Form submission stats for a client
fastify.get('/clients/:id/forms', async (request) => {
  const { id } = request.params;
  const { days = 30 } = request.query;
  const { rows } = await pool.query(
    `SELECT date_trunc('day', submitted_at) AS day,
            COUNT(*) AS total_forms,
            COUNT(*) FILTER (WHERE classification = 'legitimate') AS legitimate,
            COUNT(*) FILTER (WHERE classification = 'spam') AS spam
     FROM form_submissions
     WHERE client_id = $1
       AND submitted_at >= NOW() - make_interval(days => $2::int)
     GROUP BY day ORDER BY day`,
    [id, days]
  );
  return rows;
});

// Pipeline run history
fastify.get('/pipeline/runs', async (request) => {
  const { limit = 20 } = request.query;
  const { rows } = await pool.query(
    'SELECT * FROM call_pipeline_log ORDER BY started_at DESC LIMIT $1',
    [limit]
  );
  return rows;
});

// --- Auth endpoints ---

// Sign in (validate credentials)
fastify.post('/auth/sign-in', async (request, reply) => {
  const { email, password } = request.body;
  const { rows } = await pool.query(
    'SELECT * FROM app_users WHERE email = $1',
    [email]
  );
  if (rows.length === 0) {
    return reply.code(401).send({ error: 'Invalid email or password' });
  }
  const user = rows[0];
  const valid = await bcrypt.compare(password, user.password_hash);
  if (!valid) {
    return reply.code(401).send({ error: 'Invalid email or password' });
  }
  return formatUser(user);
});

// Sign up
fastify.post('/auth/sign-up', async (request, reply) => {
  const { email, password, displayName } = request.body;
  const existing = await pool.query('SELECT id FROM app_users WHERE email = $1', [email]);
  if (existing.rows.length > 0) {
    return reply.code(409).send({ error: 'Email already registered' });
  }
  const hash = await bcrypt.hash(password, SALT_ROUNDS);
  const role = email.endsWith('@blueprintforscale.com') ? '{admin}' : '{user}';
  const { rows } = await pool.query(
    `INSERT INTO app_users (email, password_hash, display_name, role)
     VALUES ($1, $2, $3, $4) RETURNING *`,
    [email, hash, displayName, role]
  );
  return formatUser(rows[0]);
});

// Get user by email
fastify.get('/auth/user-by-email/:email', async (request, reply) => {
  const { email } = request.params;
  const { rows } = await pool.query('SELECT * FROM app_users WHERE email = $1', [email]);
  if (rows.length === 0) {
    return reply.code(404).send({ error: 'User not found' });
  }
  return formatUser(rows[0]);
});

// Get user by id
fastify.get('/auth/user/:id', async (request, reply) => {
  const { id } = request.params;
  const { rows } = await pool.query('SELECT * FROM app_users WHERE id = $1', [id]);
  if (rows.length === 0) {
    return reply.code(404).send({ error: 'User not found' });
  }
  return formatUser(rows[0]);
});

// Update user
fastify.put('/auth/user/:id', async (request, reply) => {
  const { id } = request.params;
  const { displayName, photoURL, role, shortcuts, settings, loginRedirectUrl } = request.body;
  const { rows } = await pool.query(
    `UPDATE app_users SET
       display_name = COALESCE($2, display_name),
       photo_url = COALESCE($3, photo_url),
       role = COALESCE($4, role),
       shortcuts = COALESCE($5, shortcuts),
       settings = COALESCE($6, settings),
       login_redirect_url = COALESCE($7, login_redirect_url),
       updated_at = NOW()
     WHERE id = $1 RETURNING *`,
    [id, displayName, photoURL, role, shortcuts, settings ? JSON.stringify(settings) : null, loginRedirectUrl]
  );
  if (rows.length === 0) {
    return reply.code(404).send({ error: 'User not found' });
  }
  return formatUser(rows[0]);
});

// Format DB row to match Fuse User type
function formatUser(row) {
  return {
    id: row.id,
    email: row.email,
    displayName: row.display_name,
    photoURL: row.photo_url || '',
    role: row.role,
    shortcuts: row.shortcuts || [],
    settings: row.settings || {},
    loginRedirectUrl: row.login_redirect_url || '/',
  };
}

const start = async () => {
  try {
    await fastify.listen({ port: 3500, host: '0.0.0.0' });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
