const express = require('express');
const session = require('express-session');
const { RedisStore } = require('connect-redis');
const { createClient } = require('redis');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
require('dotenv').config();
const pool = require('./db');

const app = express();
app.set('trust proxy', 1);
const normalizeOrigin = (value) => String(value || '')
    .trim()
    .replace(/^"+|"+$/g, '')
    .replace(/\/+$/, '')
    .toLowerCase();

const allowedOrigins = (process.env.CORS_ORIGIN || '')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);
const allowedOriginSet = new Set(allowedOrigins.map(normalizeOrigin).filter(Boolean));

app.use(cors({
    origin: (origin, callback) => {
        if (!origin) return callback(null, true);
        if (allowedOriginSet.size === 0) return callback(null, true);
        if (allowedOriginSet.has(normalizeOrigin(origin))) return callback(null, true);
        return callback(new Error('Not allowed by CORS'));
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With']
}));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
const rawSameSite = String(process.env.COOKIE_SAMESITE || (process.env.CORS_ORIGIN ? 'none' : 'lax')).toLowerCase();
const cookieSameSite = ['lax', 'strict', 'none'].includes(rawSameSite) ? rawSameSite : 'lax';
const cookieSecure = process.env.NODE_ENV === 'production' || cookieSameSite === 'none';
let redisClient = null;
let sessionStore = null;
const redisUrl = String(process.env.REDIS_URL || '').trim();
if (redisUrl) {
    try {
        redisClient = createClient({ url: redisUrl });
        redisClient.on('error', (err) => {
            console.error('[session][redis] client error:', err?.message || err);
        });
        redisClient.connect()
            .then(() => {
                console.log('[session][redis] connected');
            })
            .catch((err) => {
                console.error('[session][redis] connect failed, fallback may be required:', err?.message || err);
            });
        sessionStore = new RedisStore({
            client: redisClient,
            prefix: 'crm:sess:'
        });
    } catch (err) {
        console.error('[session][redis] init failed:', err?.message || err);
        redisClient = null;
        sessionStore = null;
    }
}

const sessionMiddleware = session({
    name: 'sid',
    secret: process.env.SESSION_SECRET || 'F8v!q2Kz9@Lm4#Nx7$Rp1^Tg6&Hy3*Ud5+Wm8?Sa',
    resave: false,
    saveUninitialized: false,
    rolling: true,
    proxy: true,
    store: sessionStore || undefined,
    cookie: {
        httpOnly: true,
        sameSite: cookieSameSite,
        secure: cookieSecure,
        maxAge: 1000 * 60 * 60 * 4
    }
});
app.use(sessionMiddleware);

const requireAuthApi = (req, res, next) => {
    const tmId = Number(req.session?.user?.id || 0);
    if (!Number.isInteger(tmId) || tmId <= 0) {
        return res.status(401).json({ error: 'login required' });
    }
    return next();
};

app.use('/tm', requireAuthApi);
app.use('/chat', requireAuthApi);

const authRouter = require('./routes/auth');
const ExcelJS = require('exceljs');


app.get('/', (req, res) => {
    res.send('서버 실행중');
});

app.get('/version', (req, res) => {
    const commit =
        process.env.RAILWAY_GIT_COMMIT_SHA ||
        process.env.RAILWAY_GIT_COMMIT ||
        process.env.GITHUB_SHA ||
        'unknown';
    return res.json({
        ok: true,
        commit,
        nodeEnv: process.env.NODE_ENV || 'development',
        startedAt: new Date().toISOString(),
    });
});

app.use('/auth', authRouter);

app.get('/chat/users', async (req, res) => {
    try {
        const resolvedTmId = getSessionTmId(req);
        if (!resolvedTmId) {
            return res.status(401).json({ error: 'login required' });
        }
        const [rows] = resolvedTmId
            ? await pool.query(
                `
                SELECT id, name, isAdmin
                FROM tm
                WHERE id <> ?
                ORDER BY name ASC
                `,
                [resolvedTmId]
            )
            : await pool.query(
                `
                SELECT id, name, isAdmin
                FROM tm
                ORDER BY name ASC
                `
            );
        return res.json(rows || []);
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch chat users failed' });
    }
});

app.get('/chat/messages', async (req, res) => {
    try {
        const resolvedTmId = getSessionTmId(req);
        if (!resolvedTmId) {
            return res.status(401).json({ error: 'login required' });
        }
        await ensureChatSchema();
        const limitRaw = Number(req.query?.limit);
        const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(limitRaw, 1), 300) : 100;
        const beforeIdRaw = Number(req.query?.beforeId || 0);
        const beforeId = Number.isFinite(beforeIdRaw) && beforeIdRaw > 0 ? beforeIdRaw : 0;
        const scope = String(req.query?.scope || '').trim().toLowerCase();
        const targetTmId = Number(req.query?.targetTmId || 0);
        const isGroup = !targetTmId;
        let rows = [];
        if (scope === 'all') {
            const [allRows] = await pool.query(
                `
                SELECT
                    id,
                    sender_tm_id,
                    target_tm_id,
                    is_group,
                    sender_name,
                    sender_role,
                    message,
                    message_type,
                    shared_lead_id,
                    shared_payload,
                    created_at
                FROM tm_chat_messages
                WHERE is_group = 1
                   OR (? > 0 AND is_group = 0 AND (sender_tm_id = ? OR target_tm_id = ?))
                ORDER BY id DESC
                LIMIT ?
                `,
                [resolvedTmId, resolvedTmId, resolvedTmId, limit]
            );
            rows = allRows || [];
        } else if (isGroup) {
            const [groupRows] = await pool.query(
                `
                SELECT
                    id,
                    sender_tm_id,
                    target_tm_id,
                    is_group,
                    sender_name,
                    sender_role,
                    message,
                    message_type,
                    shared_lead_id,
                    shared_payload,
                    created_at
                FROM tm_chat_messages
                WHERE is_group = 1
                  AND (? = 0 OR id < ?)
                ORDER BY id DESC
                LIMIT ?
                `,
                [beforeId, beforeId, limit]
            );
            rows = groupRows || [];
        } else {
            if (!resolvedTmId) {
                return res.json([]);
            }
            const [directRows] = await pool.query(
                `
                SELECT
                    id,
                    sender_tm_id,
                    target_tm_id,
                    is_group,
                    sender_name,
                    sender_role,
                    message,
                    message_type,
                    shared_lead_id,
                    shared_payload,
                    created_at
                FROM tm_chat_messages
                WHERE is_group = 0
                  AND (
                    (sender_tm_id = ? AND target_tm_id = ?)
                    OR
                    (sender_tm_id = ? AND target_tm_id = ?)
                  )
                  AND (? = 0 OR id < ?)
                ORDER BY id DESC
                LIMIT ?
                `,
                [resolvedTmId, targetTmId, targetTmId, resolvedTmId, beforeId, beforeId, limit]
            );
            rows = directRows || [];
        }
        return res.json((rows || []).reverse());
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch chat messages failed' });
    }
});

app.get('/chat/lead/:id', async (req, res) => {
    try {
        const resolvedTmId = getSessionTmId(req);
        if (!resolvedTmId) {
            return res.status(401).json({ error: 'Unauthorized' });
        }
        const leadId = Number(req.params?.id || 0);
        if (!leadId) {
            return res.status(400).json({ error: 'Invalid lead id' });
        }

        const [leadRows] = await pool.query(
            `
            SELECT
                l.id,
                l.\`인입날짜\` AS inbound_at,
                l.\`이름\` AS name,
                l.\`연락처\` AS phone,
                l.\`이벤트\` AS event_name,
                l.\`상담가능시간\` AS available_time,
                l.\`tm\` AS tm_id,
                t.name AS tm_name,
                l.\`상태\` AS status_name,
                l.\`거주지\` AS region_name,
                l.\`콜횟수\` AS call_count,
                l.\`부재중_횟수\` AS missed_count,
                l.\`예약부도_횟수\` AS no_show_count,
                l.\`콜_날짜시간\` AS called_at,
                l.\`예약_내원일시\` AS reservation_at,
                l.\`리콜_예정일시\` AS recall_at
            FROM tm_leads l
            LEFT JOIN tm t ON t.id = l.tm
            WHERE l.id = ?
            LIMIT 1
            `,
            [leadId]
        );
        if (!leadRows || leadRows.length === 0) {
            return res.status(404).json({ error: 'Lead not found' });
        }

        let normalizedPhone = String(leadRows[0]?.phone || '').replace(/\D/g, '');
        if (normalizedPhone.startsWith('82')) {
            normalizedPhone = `0${normalizedPhone.slice(2)}`;
        }
        const normalizePhoneSql = (col) => `
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(LOWER(${col}), 'p:', ''),
                            '-', ''),
                        ' ', ''),
                    '+82', '0'),
                '(', ''),
            ')', '')
        `;
        const memoSql = normalizedPhone
            ? `
              SELECT
                  m.id,
                  m.memo_time,
                  m.memo_content,
                  m.status_tag,
                  m.status_reservation_at,
                  m.tm_id,
                  t.name AS tm_name
              FROM tm_memos m
              LEFT JOIN tm t ON t.id = m.tm_id
              WHERE ${normalizePhoneSql('m.target_phone')} = ?
              ORDER BY m.memo_time DESC, m.id DESC
              `
            : `
              SELECT
                  m.id,
                  m.memo_time,
                  m.memo_content,
                  m.status_tag,
                  m.status_reservation_at,
                  m.tm_id,
                  t.name AS tm_name
              FROM tm_memos m
              LEFT JOIN tm t ON t.id = m.tm_id
              WHERE m.tm_lead_id = ?
              ORDER BY m.memo_time DESC, m.id DESC
              `;
        const [memoRows] = await pool.query(memoSql, [normalizedPhone || leadId]);

        return res.json({
            lead: leadRows[0],
            memos: memoRows || [],
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch shared lead failed' });
    }
});

// NOTE: removed unused /info endpoint (list was undefined)

const pickColumn = (columns, candidates) => {
    const lower = columns.map((col) => col.toLowerCase());
    for (const name of candidates) {
        const idx = lower.indexOf(name.toLowerCase());
        if (idx !== -1) return columns[idx];
    }
    return null;
};

const describeTable = async (table) => {
    const [rows] = await pool.query(`DESCRIBE ${table}`);
    return rows.map((row) => row.Field);
};

const ensureLeadAssignedDateColumn = async () => {
    const [rows] = await pool.query(`
        SELECT COUNT(*) AS cnt
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'tm_leads'
          AND COLUMN_NAME = '배정날짜'
    `);
    if (rows[0]?.cnt === 0) {
        await pool.query('ALTER TABLE tm_leads ADD COLUMN `배정날짜` DATETIME NULL');
    }
};

const ensureRecallColumns = async () => {
    const [rows] = await pool.query(`
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'tm_leads'
          AND COLUMN_NAME IN ('리콜_예정일시', '리콜_완료여부', '리콜_스누즈횟수')
    `);
    const has = new Set((rows || []).map((row) => row.COLUMN_NAME));
    const alterParts = [];
    if (!has.has('리콜_예정일시')) alterParts.push('ADD COLUMN `리콜_예정일시` DATETIME NULL');
    if (!has.has('리콜_완료여부')) alterParts.push('ADD COLUMN `리콜_완료여부` TINYINT(1) NOT NULL DEFAULT 0');
    if (!has.has('리콜_스누즈횟수')) alterParts.push('ADD COLUMN `리콜_스누즈횟수` INT NOT NULL DEFAULT 0');
    if (alterParts.length > 0) {
        await pool.query(`ALTER TABLE tm_leads ${alterParts.join(', ')}`);
    }
};

let ensureTmScheduleSchemaPromise = null;
const ensureTmScheduleSchema = async () => {
    if (!ensureTmScheduleSchemaPromise) {
        ensureTmScheduleSchemaPromise = (async () => {
            const [tables] = await pool.query(`
                SELECT COUNT(*) AS cnt
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'tm_schedule'
            `);
            if (tables[0]?.cnt === 0) {
                await pool.query(`
                    CREATE TABLE tm_schedule (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        tm_id BIGINT NOT NULL,
                        schedule_date DATE NOT NULL,
                        schedule_type VARCHAR(30) NOT NULL,
                        custom_type VARCHAR(100) DEFAULT NULL,
                        memo TEXT,
                        created_by BIGINT DEFAULT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        KEY idx_tm_schedule_date (schedule_date, tm_id),
                        KEY idx_tm_schedule_tm_id (tm_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
                `);
            } else {
                const [columns] = await pool.query(`
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'tm_schedule'
                `);
                const has = new Set((columns || []).map((row) => row.COLUMN_NAME));
                const alterParts = [];
                if (!has.has('tm_id')) alterParts.push('ADD COLUMN tm_id BIGINT NOT NULL');
                if (!has.has('schedule_date')) alterParts.push('ADD COLUMN schedule_date DATE NOT NULL');
                if (!has.has('schedule_type')) alterParts.push('ADD COLUMN schedule_type VARCHAR(30) NOT NULL');
                if (!has.has('custom_type')) alterParts.push('ADD COLUMN custom_type VARCHAR(100) DEFAULT NULL');
                if (!has.has('memo')) alterParts.push('ADD COLUMN memo TEXT');
                if (!has.has('created_by')) alterParts.push('ADD COLUMN created_by BIGINT DEFAULT NULL');
                if (!has.has('created_at')) alterParts.push('ADD COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP');
                if (!has.has('updated_at')) alterParts.push('ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP');
                if (alterParts.length > 0) {
                    await pool.query(`ALTER TABLE tm_schedule ${alterParts.join(', ')}`);
                }
                const [indexes] = await pool.query(`SHOW INDEX FROM tm_schedule`);
                const indexNames = new Set((indexes || []).map((row) => row.Key_name));
                if (!indexNames.has('idx_tm_schedule_date')) {
                    await pool.query('ALTER TABLE tm_schedule ADD INDEX idx_tm_schedule_date (schedule_date, tm_id)');
                }
                if (!indexNames.has('idx_tm_schedule_tm_id')) {
                    await pool.query('ALTER TABLE tm_schedule ADD INDEX idx_tm_schedule_tm_id (tm_id)');
                }
            }
        })().finally(() => {
            ensureTmScheduleSchemaPromise = null;
        });
    }
    return ensureTmScheduleSchemaPromise;
};

let ensureCompanyScheduleSchemaPromise = null;
const ensureCompanyScheduleSchema = async () => {
    if (!ensureCompanyScheduleSchemaPromise) {
        ensureCompanyScheduleSchemaPromise = (async () => {
            const [tables] = await pool.query(`
                SELECT COUNT(*) AS cnt
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'company_schedule'
            `);
            if (tables[0]?.cnt === 0) {
                await pool.query(`
                    CREATE TABLE company_schedule (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        start_date DATE NOT NULL,
                        end_date DATE NOT NULL,
                        content VARCHAR(255) NOT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        KEY idx_company_schedule_range (start_date, end_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
                `);
            }
        })().finally(() => {
            ensureCompanyScheduleSchemaPromise = null;
        });
    }
    return ensureCompanyScheduleSchemaPromise;
};

let ensureChatSchemaPromise = null;
let isChatSchemaEnsured = false;
const ensureChatSchema = async () => {
    if (isChatSchemaEnsured) {
        return;
    }
    if (!ensureChatSchemaPromise) {
        ensureChatSchemaPromise = (async () => {
            const [tables] = await pool.query(`
                SELECT COUNT(*) AS cnt
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'tm_chat_messages'
            `);
            if (tables[0]?.cnt === 0) {
                await pool.query(`
                    CREATE TABLE tm_chat_messages (
                        id BIGINT NOT NULL AUTO_INCREMENT,
                        sender_tm_id BIGINT NOT NULL,
                        target_tm_id BIGINT DEFAULT NULL,
                        is_group TINYINT(1) NOT NULL DEFAULT 1,
                        sender_name VARCHAR(50) NOT NULL,
                        sender_role VARCHAR(20) NOT NULL,
                        message TEXT NOT NULL,
                        message_type VARCHAR(20) NOT NULL DEFAULT 'text',
                        shared_lead_id BIGINT DEFAULT NULL,
                        shared_payload JSON DEFAULT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        KEY idx_tm_chat_created_at (created_at),
                        KEY idx_tm_chat_sender (sender_tm_id, created_at),
                        KEY idx_tm_chat_target (target_tm_id, created_at),
                        KEY idx_tm_chat_room (is_group, sender_tm_id, target_tm_id, created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
                `);
            } else {
                const [columns] = await pool.query(`
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'tm_chat_messages'
                `);
                const has = new Set((columns || []).map((row) => row.COLUMN_NAME));
                const alterParts = [];
                if (!has.has('target_tm_id')) alterParts.push('ADD COLUMN target_tm_id BIGINT DEFAULT NULL AFTER sender_tm_id');
                if (!has.has('is_group')) alterParts.push('ADD COLUMN is_group TINYINT(1) NOT NULL DEFAULT 1 AFTER target_tm_id');
                if (!has.has('message_type')) alterParts.push("ADD COLUMN message_type VARCHAR(20) NOT NULL DEFAULT 'text' AFTER message");
                if (!has.has('shared_lead_id')) alterParts.push('ADD COLUMN shared_lead_id BIGINT DEFAULT NULL AFTER message_type');
                if (!has.has('shared_payload')) alterParts.push('ADD COLUMN shared_payload JSON DEFAULT NULL AFTER shared_lead_id');
                if (alterParts.length > 0) {
                    await pool.query(`ALTER TABLE tm_chat_messages ${alterParts.join(', ')}`);
                }
                const [indexes] = await pool.query(`SHOW INDEX FROM tm_chat_messages`);
                const indexNames = new Set((indexes || []).map((row) => row.Key_name));
                if (!indexNames.has('idx_tm_chat_target')) {
                    await pool.query('ALTER TABLE tm_chat_messages ADD INDEX idx_tm_chat_target (target_tm_id, created_at)');
                }
                if (!indexNames.has('idx_tm_chat_room')) {
                    await pool.query('ALTER TABLE tm_chat_messages ADD INDEX idx_tm_chat_room (is_group, sender_tm_id, target_tm_id, created_at)');
                }
            }
            isChatSchemaEnsured = true;
        })().catch((err) => {
            ensureChatSchemaPromise = null;
            throw err;
        });
    }
    return ensureChatSchemaPromise;
};

let ensureActivityLogSchemaPromise = null;
const ensureActivityLogSchema = async () => {
    if (!ensureActivityLogSchemaPromise) {
        ensureActivityLogSchemaPromise = (async () => {
            await pool.query(
                `
                CREATE TABLE IF NOT EXISTS activity_logs (
                    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    actor_tm_id BIGINT NULL,
                    actor_role ENUM('ADMIN','TM','SYSTEM') NOT NULL,
                    action VARCHAR(64) NOT NULL,
                    target_type VARCHAR(64) NOT NULL,
                    target_id VARCHAR(128) NULL,
                    before_json JSON NULL,
                    after_json JSON NULL,
                    ip_address VARCHAR(64) NULL,
                    user_agent VARCHAR(255) NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    KEY idx_activity_created_at (created_at),
                    KEY idx_activity_actor_tm_id (actor_tm_id),
                    KEY idx_activity_actor_role (actor_role),
                    KEY idx_activity_action (action),
                    KEY idx_activity_target_type (target_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
                `
            );
        })().catch((err) => {
            ensureActivityLogSchemaPromise = null;
            throw err;
        });
    }
    return ensureActivityLogSchemaPromise;
};

const parseLocalDateTimeString = (value) => {
    if (value === undefined || value === null || value === '') return null;
    const raw = String(value).trim().replace('T', ' ');
    const match = raw.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::(\d{2}))?$/);
    if (!match) return null;
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    const hour = Number(match[4]);
    const minute = Number(match[5]);
    const second = Number(match[6] || 0);
    const date = new Date(year, month - 1, day, hour, minute, second);
    if (Number.isNaN(date.getTime())) return null;
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const mi = String(date.getMinutes()).padStart(2, '0');
    const ss = String(date.getSeconds()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
};

const normalizeDateTimeForDb = (value) => {
    if (value === undefined || value === null || value === '') return null;
    if (value instanceof Date) {
        if (Number.isNaN(value.getTime())) return null;
        const yyyy = value.getFullYear();
        const mm = String(value.getMonth() + 1).padStart(2, '0');
        const dd = String(value.getDate()).padStart(2, '0');
        const hh = String(value.getHours()).padStart(2, '0');
        const mi = String(value.getMinutes()).padStart(2, '0');
        const ss = String(value.getSeconds()).padStart(2, '0');
        return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
    }
    const parsedLocal = parseLocalDateTimeString(value);
    if (parsedLocal) return parsedLocal;
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return null;
    const yyyy = parsed.getFullYear();
    const mm = String(parsed.getMonth() + 1).padStart(2, '0');
    const dd = String(parsed.getDate()).padStart(2, '0');
    const hh = String(parsed.getHours()).padStart(2, '0');
    const mi = String(parsed.getMinutes()).padStart(2, '0');
    const ss = String(parsed.getSeconds()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
};

const ensureAdminRequest = async (req) => {
    const sessionUserId = Number(req.session?.user?.id || 0);
    if (!sessionUserId) return false;

    if (Boolean(req.session?.isAdmin)) return true;

    try {
        const [rows] = await pool.query(
            'SELECT id FROM tm WHERE id = ? AND isAdmin = 1 LIMIT 1',
            [sessionUserId]
        );
        const isAdmin = rows.length > 0;
        if (isAdmin) {
            req.session.isAdmin = true;
        }
        return isAdmin;
    } catch (err) {
        console.error(err);
        return false;
    }
};

app.get('/dbdata', async (req, res) => {
    try {
        const { tm, status, callMin, missMin, region, memo, assignedToday } = req.query || {};
        const columns = await describeTable('tm_leads');
        const map = {
            tm: pickColumn(columns, ['tm', 'tm_id', 'assigned_tm_id', 'assigned_tm']),
            status: pickColumn(columns, ['상태', 'status', 'call_status']),
            callCount: pickColumn(columns, ['콜횟수', 'call_count']),
            missCount: pickColumn(columns, ['부재중_횟수', 'miss_count']),
            region: pickColumn(columns, ['거주지', 'region']),
            assignedDate: pickColumn(columns, ['배정날짜', 'assigned_at', 'assigned_date', 'tm_assigned_at']),
        };

        const where = [];
        const params = [];

        if (tm && tm !== 'all' && map.tm) {
            where.push(`l.\`${map.tm}\` = ?`);
            params.push(tm);
        }
        if (status && status !== 'all' && map.status) {
            where.push(`l.\`${map.status}\` LIKE ?`);
            params.push(`%${status}%`);
        }
        if (callMin !== undefined && callMin !== '' && map.callCount) {
            const min = Number(callMin);
            if (!Number.isNaN(min)) {
                where.push(`COALESCE(l.\`${map.callCount}\`, 0) >= ?`);
                params.push(min);
            }
        }
        if (missMin !== undefined && missMin !== '' && map.missCount) {
            const min = Number(missMin);
            if (!Number.isNaN(min)) {
                where.push(`COALESCE(l.\`${map.missCount}\`, 0) >= ?`);
                params.push(min);
            }
        }
        if (region && map.region) {
            where.push(`l.\`${map.region}\` LIKE ?`);
            params.push(`%${region}%`);
        }
        if (memo) {
            where.push('m.memo_content LIKE ?');
            params.push(`%${memo}%`);
        }
        if (assignedToday && map.assignedDate) {
            where.push(`DATE(l.\`${map.assignedDate}\`) = DATE(${KST_NOW_SQL})`);
        }

        const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

        const [rows] = await pool.query(`
            SELECT 
                l.*,
                m.memo_time AS 최근메모시간,
                m.memo_content AS 최근메모내용,
                m.tm_id AS 최근메모작성자
            FROM tm_leads l
            LEFT JOIN (
                SELECT mm.*
                FROM tm_memos mm
                INNER JOIN (
                    SELECT tm_lead_id, MAX(memo_time) AS max_time
                    FROM tm_memos
                    WHERE tm_lead_id IS NOT NULL
                    GROUP BY tm_lead_id
                ) latest
                ON latest.tm_lead_id = mm.tm_lead_id AND latest.max_time = mm.memo_time
            ) m
            ON m.tm_lead_id = l.id
            ${whereSql}
            ORDER BY l.id DESC
        `, params);
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.get('/tmList', async (req, res) => {
    try{
        const [list] = await pool.query('select * from tm');
        res.json(list);
    } catch(err){
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
})

const normalizeLeadRow = (row, map) => {
    return {
        id: row[map.id],
        name: map.name ? row[map.name] : '',
        phone: map.phone ? row[map.phone] : '',
        availableTime: map.availableTime ? row[map.availableTime] : '',
        event: map.event ? row[map.event] : '',
        inboundDate: map.inboundDate ? row[map.inboundDate] : '',
        assignedTm: map.assignedTm ? row[map.assignedTm] : null,
        raw: row,
    };
};

const formatDateTime = (value) => {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const min = String(date.getMinutes()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${min}`;
};

const formatPhone = (value) => {
    if (!value) return '';
    let digits = String(value).replace(/\D/g, '');
    if (digits.startsWith('82')) {
        digits = `0${digits.slice(2)}`;
    }
    if (digits.length === 11) {
        return `${digits.slice(0, 3)}-${digits.slice(3, 7)}-${digits.slice(7)}`;
    }
    if (digits.length === 10) {
        return `${digits.slice(0, 3)}-${digits.slice(3, 6)}-${digits.slice(6)}`;
    }
    return String(value);
};

const getSessionTmId = (req) => {
    const tmId = Number(req.session?.user?.id || 0);
    return Number.isInteger(tmId) && tmId > 0 ? tmId : null;
};

const requireAdminApi = async (req, res) => {
    const sessionTmId = getSessionTmId(req);
    if (!sessionTmId) {
        res.status(401).json({ error: 'login required' });
        return false;
    }
    const ok = await ensureAdminRequest(req);
    if (!ok) {
        res.status(403).json({ error: 'admin only' });
        return false;
    }
    return true;
};

app.use('/admin', async (req, res, next) => {
    if (!(await requireAdminApi(req, res))) return;
    next();
});

const safeJson = (value) => {
    if (value === undefined) return null;
    try {
        return JSON.stringify(value ?? null);
    } catch (_) {
        return JSON.stringify({ error: 'serialize_failed' });
    }
};

const writeActivityLog = async (req, actorRole, action, targetType, targetId, beforeValue, afterValue) => {
    try {
        await ensureActivityLogSchema();
        const actorTmId = getSessionTmId(req);
        if (!actorTmId && String(actorRole) !== 'SYSTEM') return;
        const colNames = ['actor_tm_id', 'actor_role', 'action', 'target_type', 'target_id', 'before_json', 'after_json', 'ip_address', 'user_agent', 'created_at'];
        const values = ['?', '?', '?', '?', '?', '?', '?', '?', '?', KST_NOW_SQL];
        const params = [
            actorTmId,
            String(actorRole || 'SYSTEM'),
            String(action || ''),
            String(targetType || ''),
            targetId === undefined || targetId === null ? null : String(targetId),
            safeJson(beforeValue),
            safeJson(afterValue),
            String(req.headers['x-forwarded-for'] || req.ip || '').slice(0, 64),
            String(req.headers['user-agent'] || '').slice(0, 255),
        ];
        await pool.query(
            `
            INSERT INTO activity_logs (
                ${colNames.join(', ')}
            ) VALUES (${values.join(', ')})
            `,
            params
        );
    } catch (err) {
        console.error('[activity-log] write failed:', err?.message || err);
    }
};

const writeAdminAuditLog = async (req, action, targetType, targetId, beforeValue, afterValue) =>
    writeActivityLog(req, 'ADMIN', action, targetType, targetId, beforeValue, afterValue);

const writeTmAuditLog = async (req, action, targetType, targetId, beforeValue, afterValue) =>
    writeActivityLog(req, 'TM', action, targetType, targetId, beforeValue, afterValue);

const normalizePhoneDigits = (value) => {
    if (!value) return '';
    let digits = String(value).replace(/\D/g, '');
    if (digits.startsWith('82')) {
        digits = `0${digits.slice(2)}`;
    }
    return digits;
};

const KST_NOW_SQL = 'DATE_ADD(UTC_TIMESTAMP(), INTERVAL 9 HOUR)';

const toKstDateTimeString = (value) => {
    if (!value) return null;
    const raw = String(value).trim();
    const parsed = new Date(raw);
    if (!Number.isNaN(parsed.getTime())) {
        const kst = new Date(parsed.getTime() + 9 * 60 * 60 * 1000);
        const yyyy = kst.getUTCFullYear();
        const mm = String(kst.getUTCMonth() + 1).padStart(2, '0');
        const dd = String(kst.getUTCDate()).padStart(2, '0');
        const hh = String(kst.getUTCHours()).padStart(2, '0');
        const min = String(kst.getUTCMinutes()).padStart(2, '0');
        const ss = String(kst.getUTCSeconds()).padStart(2, '0');
        return `${yyyy}-${mm}-${dd} ${hh}:${min}:${ss}`;
    }
    const plain = raw.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::(\d{2}))?$/);
    if (plain) {
        return `${plain[1]} ${plain[2]}:${plain[3] || '00'}`;
    }
    return raw;
};

const REPORT_METRIC_TYPES = new Set(['MISSED', 'RESERVED', 'VISIT_TODAY', 'VISIT_NEXTDAY', 'FAILED', 'RECALL_WAIT']);

const toDateKey = (value) => {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
};

const normalizeReportDate = (value) => {
    if (!value) return toDateKey(new Date());
    const str = String(value).trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(str)) return null;
    const date = new Date(`${str}T00:00:00`);
    if (Number.isNaN(date.getTime())) return null;
    return toDateKey(date);
};

const nextDateKey = (dateKey) => {
    const date = new Date(`${dateKey}T00:00:00`);
    date.setDate(date.getDate() + 1);
    return toDateKey(date);
};

const latestMemoJoinSql = `
    LEFT JOIN (
        SELECT mm.*
        FROM tm_memos mm
        INNER JOIN (
            SELECT tm_lead_id, MAX(memo_time) AS max_time
            FROM tm_memos
            WHERE tm_lead_id IS NOT NULL
            GROUP BY tm_lead_id
        ) latest
        ON latest.tm_lead_id = mm.tm_lead_id AND latest.max_time = mm.memo_time
    ) m
    ON m.tm_lead_id = l.id
`;

let ensureReportSchemaPromise = null;
const ensureReportSchema = async () => {
    if (!ensureReportSchemaPromise) {
        ensureReportSchemaPromise = (async () => {
            const [cols] = await pool.query(`
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'tm_daily_report'
            `);
            const has = new Set((cols || []).map((row) => row.COLUMN_NAME));
            const alterParts = [];
            if (!has.has('manual_reserved_count')) alterParts.push('ADD COLUMN manual_reserved_count int DEFAULT NULL');
            if (!has.has('manual_failed_count')) alterParts.push('ADD COLUMN manual_failed_count int DEFAULT NULL');
            if (!has.has('manual_visit_today_count')) alterParts.push('ADD COLUMN manual_visit_today_count int DEFAULT NULL');
            if (!has.has('manual_visit_nextday_count')) alterParts.push('ADD COLUMN manual_visit_nextday_count int DEFAULT NULL');
            if (!has.has('manual_call_count')) alterParts.push('ADD COLUMN manual_call_count int DEFAULT NULL');
            if (!has.has('failed_count')) alterParts.push('ADD COLUMN failed_count int NOT NULL DEFAULT 0');
            if (!has.has('check_db_crm')) alterParts.push('ADD COLUMN check_db_crm tinyint(1) NOT NULL DEFAULT 0');
            if (!has.has('check_inhouse_crm')) alterParts.push('ADD COLUMN check_inhouse_crm tinyint(1) NOT NULL DEFAULT 0');
            if (!has.has('check_sheet')) alterParts.push('ADD COLUMN check_sheet tinyint(1) NOT NULL DEFAULT 0');
            if (!has.has('is_submitted')) alterParts.push('ADD COLUMN is_submitted tinyint(1) NOT NULL DEFAULT 0');
            if (!has.has('submitted_at')) alterParts.push('ADD COLUMN submitted_at datetime DEFAULT NULL');

            if (alterParts.length > 0) {
                await pool.query(`ALTER TABLE tm_daily_report ${alterParts.join(', ')}`);
            }
        })().catch((err) => {
            ensureReportSchemaPromise = null;
            throw err;
        });
    }
    return ensureReportSchemaPromise;
};

const normalizeBool = (value) => {
    if (value === true || value === 1 || value === '1') return 1;
    return 0;
};

const normalizeNullableInt = (value) => {
    if (value === undefined || value === null || value === '') return null;
    const n = Number(value);
    if (Number.isNaN(n)) return null;
    return Math.max(0, Math.floor(n));
};

const resolveTmId = (req) => getSessionTmId(req);

const getDailySummaryRows = async (conn, tmId, reportDate) => {
    const nextDay = nextDateKey(reportDate);
    const [callRows] = await conn.query(
        `
        SELECT
            l.id,
            l.\`이름\` AS name,
            l.\`연락처\` AS phone,
            l.\`상태\` AS status,
            COALESCE(l.\`콜횟수\`, 0) AS call_count,
            l.\`예약_내원일시\` AS reservation_at,
            m.memo_content AS latest_memo
        FROM tm_leads l
        ${latestMemoJoinSql}
        WHERE l.tm = ?
          AND DATE(l.\`콜_날짜시간\`) = ?
        ORDER BY l.id DESC
        `,
        [String(tmId), reportDate]
    );

    const [dailyStatusRows] = await conn.query(
        `
        SELECT
            m.tm_lead_id AS id,
            COALESCE(l.\`이름\`, '') AS name,
            COALESCE(l.\`연락처\`, m.target_phone, '') AS phone,
            TRIM(COALESCE(m.status_tag, '')) AS status,
            m.status_reservation_at AS reservation_at,
            m.memo_content AS latest_memo
        FROM tm_memos m
        INNER JOIN (
            SELECT tm_lead_id, MAX(id) AS max_id
            FROM tm_memos
            WHERE tm_id = ?
              AND DATE(memo_time) = ?
              AND tm_lead_id IS NOT NULL
              AND TRIM(COALESCE(status_tag, '')) <> ''
            GROUP BY tm_lead_id
        ) latest
          ON latest.max_id = m.id
        LEFT JOIN tm_leads l
          ON l.id = m.tm_lead_id
        WHERE m.tm_id = ?
        ORDER BY m.tm_lead_id DESC
        `,
        [String(tmId), reportDate, String(tmId)]
    );

    const statusText = (row) => String(row.status || '').trim();
    const statusEq = (row, value) => statusText(row) === value;
    const statusRows = Array.isArray(dailyStatusRows) ? dailyStatusRows : [];
    const missed = statusRows.filter((row) => statusEq(row, '부재중'));
    const failed = statusRows.filter((row) => statusEq(row, '실패'));
    const reserved = statusRows.filter((row) => statusEq(row, '예약'));
    const visitTodayReserved = reserved.filter((row) => toDateKey(row.reservation_at) === reportDate);
    const visitTodayCompleted = statusRows.filter(
        (row) => statusEq(row, '내원완료') && toDateKey(row.reservation_at) === reportDate
    );
    const visitTodayMap = new Map();
    [...visitTodayReserved, ...visitTodayCompleted].forEach((row) => {
        if (!row?.id) return;
        if (!visitTodayMap.has(row.id)) visitTodayMap.set(row.id, row);
    });
    const visitToday = Array.from(visitTodayMap.values());
    const visitNextdayByCall = reserved.filter((row) => toDateKey(row.reservation_at) === nextDay);
    const visitNextdayMap = new Map();
    visitNextdayByCall.forEach((row) => {
        if (!row?.id) return;
        if (!visitNextdayMap.has(row.id)) visitNextdayMap.set(row.id, row);
    });
    const visitNextday = Array.from(visitNextdayMap.values());
    const totalCallCount = callRows.reduce((sum, row) => {
        const n = Number(row.call_count);
        return sum + (Number.isNaN(n) ? 0 : Math.max(0, Math.floor(n)));
    }, 0);

    return {
        callRows,
        totalCallCount,
        missed,
        failed,
        reserved,
        visitToday,
        visitNextday,
    };
};

const upsertReportBase = async (conn, tmId, reportDate, summary) => {
    const totalCallCount = Number(summary.totalCallCount || 0);
    const missedCount = summary.missed.length;
    const failedCount = summary.failed.length;
    const reservedCount = summary.reserved.length;
    const visitTodayCount = summary.visitToday.length;
    const visitNextdayCount = summary.visitNextday.length;

    await conn.query(
        `
        INSERT INTO tm_daily_report (
            tm_id,
            report_date,
            total_call_count,
            missed_count,
            failed_count,
            reserved_count,
            visit_today_count,
            visit_nextday_count,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ${KST_NOW_SQL}, ${KST_NOW_SQL})
        ON DUPLICATE KEY UPDATE
            total_call_count = VALUES(total_call_count),
            missed_count = VALUES(missed_count),
            failed_count = VALUES(failed_count),
            reserved_count = VALUES(reserved_count),
            visit_today_count = VALUES(visit_today_count),
            visit_nextday_count = VALUES(visit_nextday_count),
            updated_at = ${KST_NOW_SQL}
        `,
        [String(tmId), reportDate, totalCallCount, missedCount, failedCount, reservedCount, visitTodayCount, visitNextdayCount]
    );

    const [rows] = await conn.query(
        'SELECT id FROM tm_daily_report WHERE tm_id = ? AND report_date = ? LIMIT 1',
        [String(tmId), reportDate]
    );
    const reportId = rows[0]?.id;
    if (!reportId) throw new Error('Failed to load report id');

    return {
        reportId,
        totalCallCount,
        missedCount,
        failedCount,
        reservedCount,
        visitTodayCount,
        visitNextdayCount,
    };
};

const replaceReportLeads = async (conn, reportId, summary) => {
    await conn.query('DELETE FROM tm_daily_report_leads WHERE report_id = ?', [reportId]);
    const detailValues = [];
    const pushDetail = (metricType, rows) => {
        rows.forEach((row) => {
            detailValues.push([
                reportId,
                metricType,
                row.id,
                row.name || null,
                row.phone || null,
                row.status || null,
                row.reservation_at || null,
                row.latest_memo || null,
            ]);
        });
    };
    pushDetail('MISSED', summary.missed);
    pushDetail('FAILED', summary.failed);
    pushDetail('RESERVED', summary.reserved);
    pushDetail('VISIT_TODAY', summary.visitToday);
    pushDetail('VISIT_NEXTDAY', summary.visitNextday);

    if (detailValues.length > 0) {
        await conn.query(
            `
            INSERT INTO tm_daily_report_leads (
                report_id,
                metric_type,
                lead_id,
                name_snapshot,
                phone_snapshot,
                status_snapshot,
                reservation_at_snapshot,
                memo_snapshot
            ) VALUES ?
            `,
            [detailValues]
        );
        await conn.query(
            `UPDATE tm_daily_report_leads SET created_at = ${KST_NOW_SQL} WHERE report_id = ?`,
            [reportId]
        );
    }
};

app.get('/tm/leads', async (req, res) => {
    try {
        const columns = await describeTable('tm_leads');
        const map = {
            id: pickColumn(columns, ['id', 'lead_id', 'tm_lead_id']),
            name: pickColumn(columns, ['name', 'customer_name', 'client_name', 'user_name', '이름']),
            phone: pickColumn(columns, ['phone', 'phone_number', 'tel', 'mobile', '연락처']),
            availableTime: pickColumn(columns, ['available_time', 'availabletime', 'call_time', 'time_range', 'available_hour', '상담가능시간']),
            event: pickColumn(columns, ['event', 'event_name', 'campaign', 'source', '이벤트']),
            inboundDate: pickColumn(columns, ['inbound_date', 'in_date', 'created_at', 'createdat', 'reg_date', 'created', '인입날짜']),
            assignedTm: pickColumn(columns, ['tm_id', 'tmid', 'assigned_tm_id', 'assigned_tm', 'tm']),
        };

        if (!map.id) {
            return res.status(500).json({ error: 'tm_leads id column not found' });
        }

        const [rows] = await pool.query('SELECT * FROM tm_leads');
        let leads = rows.map((row) => normalizeLeadRow(row, map));

        if (map.assignedTm) {
            leads = leads.filter((lead) => {
                const val = lead.assignedTm;
                return val === null || val === undefined || val === '';
            });
        }

        if (map.inboundDate) {
            leads.sort((a, b) => {
                const aTime = new Date(a.inboundDate).getTime();
                const bTime = new Date(b.inboundDate).getTime();
                if (Number.isNaN(aTime) || Number.isNaN(bTime)) return 0;
                return bTime - aTime;
            });
        }

        const phoneKeys = Array.from(
            new Set(
                (leads || [])
                    .map((lead) => normalizePhoneDigits(lead?.phone))
                    .filter(Boolean)
            )
        );

        if (phoneKeys.length > 0) {
            const normalizePhoneSql = (col) => `
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(LOWER(${col}), 'p:', ''),
                                '-', ''),
                            ' ', ''),
                        '+82', '0'),
                    '(', ''),
                ')', '')
            `;
            const placeholders = phoneKeys.map(() => '?').join(', ');
            const [memoRows] = await pool.query(
                `
                SELECT
                  ${normalizePhoneSql('m.target_phone')} AS phone_key,
                  m.memo_content,
                  m.memo_time,
                  m.id,
                  t.name AS tm_name
                FROM tm_memos m
                LEFT JOIN tm t ON t.id = m.tm_id
                WHERE ${normalizePhoneSql('m.target_phone')} IN (${placeholders})
                ORDER BY m.memo_time DESC, m.id DESC
                `,
                phoneKeys
            );

            const phoneMemoMap = new Map();
            for (const row of (memoRows || [])) {
                const key = String(row.phone_key || '');
                if (!key || phoneMemoMap.has(key)) continue;
                phoneMemoMap.set(key, {
                    tmName: row.tm_name || '',
                    memoContent: row.memo_content || '',
                    memoTime: row.memo_time || null,
                });
            }

            const phoneColumnExpr = map.phone ? `l.\`${map.phone}\`` : 'NULL';
            const eventColumnExpr = map.event ? `l.\`${map.event}\`` : 'NULL';
            const idColumnExpr = map.id ? `l.\`${map.id}\`` : 'NULL';
            const inboundColumnExpr = map.inboundDate ? `l.\`${map.inboundDate}\`` : null;
            const eventOrderExpr = inboundColumnExpr
                ? `${inboundColumnExpr} DESC, ${idColumnExpr} DESC`
                : `${idColumnExpr} DESC`;
            const [eventRows] = await pool.query(
                `
                SELECT
                  ${normalizePhoneSql(phoneColumnExpr)} AS phone_key,
                  ${idColumnExpr} AS lead_id,
                  ${eventColumnExpr} AS event_name
                FROM tm_leads l
                WHERE ${normalizePhoneSql(phoneColumnExpr)} IN (${placeholders})
                ORDER BY ${eventOrderExpr}
                `,
                phoneKeys
            );

            const phoneEventMap = new Map();
            for (const row of (eventRows || [])) {
                const key = String(row.phone_key || '');
                if (!key) continue;
                const bucket = phoneEventMap.get(key) || [];
                bucket.push({
                    leadId: row.lead_id,
                    eventName: row.event_name || '',
                });
                phoneEventMap.set(key, bucket);
            }

            leads = leads.map((lead) => {
                const key = normalizePhoneDigits(lead?.phone);
                const dup = phoneMemoMap.get(key);
                const eventCandidates = phoneEventMap.get(key) || [];
                const previousEvent = (
                    eventCandidates.find((row) => String(row.leadId) !== String(lead.id) && String(row.eventName || '').trim())
                    || { eventName: '' }
                ).eventName;
                return {
                    ...lead,
                    duplicateMemoTmName: dup?.tmName || '',
                    duplicateMemoContent: dup?.memoContent || '',
                    duplicateMemoTime: dup?.memoTime || null,
                    duplicatePreviousEvent: previousEvent || '',
                };
            });
        }

        res.json({ columns: map, leads });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.get('/tm/assign/summary', async (req, res) => {
    if (!(await requireAdminApi(req, res))) return;
    try {
        await ensureLeadAssignedDateColumn();
        const columns = await describeTable('tm_leads');
        const assignCol = pickColumn(columns, ['tm_id', 'tmid', 'assigned_tm_id', 'assigned_tm', 'tm']);
        const assignedAtCol = pickColumn(columns, ['배정날짜', 'assigned_at', 'assigned_date', 'tm_assigned_at']);

        if (!assignCol) {
            return res.status(500).json({ error: 'tm_leads assignment column not found' });
        }

        const [agentRows] = await pool.query(
            'SELECT id, name FROM tm WHERE COALESCE(isAdmin, 0) = 0 ORDER BY name ASC'
        );

        const todayExpr = assignedAtCol
            ? `SUM(CASE WHEN \`${assignedAtCol}\` IS NOT NULL
                          AND DATE(\`${assignedAtCol}\`) = DATE(${KST_NOW_SQL})
                        THEN 1 ELSE 0 END) AS today_count`
            : '0 AS today_count';

        const [countRows] = await pool.query(
            `
            SELECT
              CAST(\`${assignCol}\` AS CHAR) AS tm_key,
              COUNT(*) AS total_count,
              ${todayExpr}
            FROM tm_leads
            WHERE \`${assignCol}\` IS NOT NULL
              AND TRIM(CAST(\`${assignCol}\` AS CHAR)) <> ''
            GROUP BY CAST(\`${assignCol}\` AS CHAR)
            `
        );

        const countMap = new Map(
            (countRows || []).map((row) => [
                String(row.tm_key),
                {
                    totalCount: Number(row.total_count || 0),
                    todayCount: Number(row.today_count || 0),
                },
            ])
        );

        const rows = (agentRows || []).map((agent) => {
            const key = String(agent.id);
            const hit = countMap.get(key) || { totalCount: 0, todayCount: 0 };
            return {
                tmId: agent.id,
                name: agent.name,
                totalCount: hit.totalCount,
                todayCount: hit.todayCount,
            };
        });

        const holdHit = countMap.get('0') || { totalCount: 0, todayCount: 0 };
        rows.push({
            tmId: 0,
            name: '보류',
            totalCount: holdHit.totalCount,
            todayCount: holdHit.todayCount,
        });

        return res.json({ rows });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch assignment summary failed' });
    }
});

app.get('/tm/agents', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT id, name, phone, last_login_at, isAdmin FROM tm ORDER BY name');
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.post('/tm/agents', async (req, res) => {
    if (!(await requireAdminApi(req, res))) return;
    const { name, phone, password } = req.body || {};
    if (!name || !phone || !password) {
        return res.status(400).json({ error: 'name, phone, password are required' });
    }
    try {
        const [result] = await pool.query(
            'INSERT INTO tm (name, phone, password, isAdmin) VALUES (?, ?, ?, 0)',
            [name, phone, password]
        );
        await writeAdminAuditLog(
            req,
            'TM_AGENT_CREATE',
            'tm',
            result.insertId,
            null,
            { id: result.insertId, name, phone, isAdmin: 0 }
        );
        res.json({ ok: true, id: result.insertId });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.patch('/tm/agents/:id', async (req, res) => {
    if (!(await requireAdminApi(req, res))) return;
    const { id } = req.params;
    const { name, phone, password } = req.body || {};
    if (!name || !phone) {
        return res.status(400).json({ error: 'name and phone are required' });
    }
    try {
        const [beforeRows] = await pool.query(
            'SELECT id, name, phone, isAdmin FROM tm WHERE id = ? LIMIT 1',
            [id]
        );
        const before = beforeRows[0] || null;
        let result;
        if (password) {
            [result] = await pool.query(
                'UPDATE tm SET name = ?, phone = ?, password = ? WHERE id = ?',
                [name, phone, password, id]
            );
        } else {
            [result] = await pool.query(
                'UPDATE tm SET name = ?, phone = ? WHERE id = ?',
                [name, phone, id]
            );
        }
        if (result.affectedRows === 0) {
            return res.status(404).json({ error: 'TM not found' });
        }
        const [afterRows] = await pool.query(
            'SELECT id, name, phone, isAdmin FROM tm WHERE id = ? LIMIT 1',
            [id]
        );
        await writeAdminAuditLog(req, 'TM_AGENT_UPDATE', 'tm', id, before, afterRows[0] || null);
        res.json({ ok: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.get('/tm/schedules', async (req, res) => {
    const { from, to } = req.query || {};
    try {
        await ensureTmScheduleSchema();
        const where = [];
        const params = [];
        if (from) {
            where.push('s.schedule_date >= ?');
            params.push(from);
        }
        if (to) {
            where.push('s.schedule_date <= ?');
            params.push(to);
        }
        const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
        const [rows] = await pool.query(
            `
            SELECT
                s.id,
                s.tm_id,
                s.schedule_date,
                s.schedule_type,
                s.custom_type,
                s.memo,
                s.created_by,
                s.created_at,
                s.updated_at,
                t.name AS tm_name
            FROM tm_schedule s
            LEFT JOIN tm t ON t.id = s.tm_id
            ${whereSql}
            ORDER BY s.schedule_date ASC, s.id ASC
            `,
            params
        );
        return res.json(rows);
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed', detail: err.message });
    }
});

app.post('/tm/schedules', async (req, res) => {
    const {
        tmId,
        scheduleDate,
        scheduleType,
        customType,
        memo,
    } = req.body || {};
    const sessionTmId = getSessionTmId(req);
    const isAdmin = await ensureAdminRequest(req);
    const requestedTmId = Number(tmId);
    const targetTmId = isAdmin
        ? (Number.isNaN(requestedTmId) ? null : requestedTmId)
        : sessionTmId;

    if (!targetTmId || !scheduleDate || !scheduleType) {
        return res.status(400).json({ error: 'tmId, scheduleDate, scheduleType are required' });
    }

    const type = String(scheduleType).trim();
    const allowedTypes = new Set(['휴무', '근무', '반차', '교육', '기타']);
    if (!allowedTypes.has(type)) {
        return res.status(400).json({ error: 'invalid scheduleType' });
    }

    const dateMatch = String(scheduleDate).trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!dateMatch) {
        return res.status(400).json({ error: 'scheduleDate must be YYYY-MM-DD' });
    }

    const trimmedCustom = customType ? String(customType).trim() : '';
    const normalizedCustom = type === '기타' ? trimmedCustom : '';
    if (type === '기타' && !normalizedCustom) {
        return res.status(400).json({ error: 'customType is required when scheduleType is 기타' });
    }

    try {
        await ensureTmScheduleSchema();
        const [result] = await pool.query(
            `
            INSERT INTO tm_schedule (
                tm_id, schedule_date, schedule_type, custom_type, memo, created_by, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ${KST_NOW_SQL}, ${KST_NOW_SQL})
            `,
            [
                targetTmId,
                String(scheduleDate).trim(),
                type,
                normalizedCustom || null,
                memo ? String(memo).trim() : null,
                sessionTmId || null,
            ]
        );
        const [afterRows] = await pool.query('SELECT * FROM tm_schedule WHERE id = ? LIMIT 1', [result.insertId]);
        if (isAdmin) {
            await writeAdminAuditLog(req, 'TM_SCHEDULE_CREATE', 'tm_schedule', result.insertId, null, afterRows?.[0] || null);
        } else {
            await writeTmAuditLog(req, 'TM_SCHEDULE_CREATE', 'tm_schedule', result.insertId, null, afterRows?.[0] || null);
        }
        return res.json({ ok: true, id: result.insertId });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed', detail: err.message });
    }
});

app.get('/company/schedules', async (req, res) => {
    const { from, to } = req.query || {};
    try {
        await ensureCompanyScheduleSchema();
        const where = [];
        const params = [];
        if (from) {
            where.push('end_date >= ?');
            params.push(String(from).trim());
        }
        if (to) {
            where.push('start_date <= ?');
            params.push(String(to).trim());
        }
        const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
        const [rows] = await pool.query(
            `
            SELECT id, start_date, end_date, content, created_at, updated_at
            FROM company_schedule
            ${whereSql}
            ORDER BY start_date ASC, end_date ASC, id ASC
            `,
            params
        );
        return res.json(rows);
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed', detail: err.message });
    }
});

app.post('/company/schedules', async (req, res) => {
    const { startDate, endDate, content } = req.body || {};
    if (!startDate || !endDate || !String(content || '').trim()) {
        return res.status(400).json({ error: 'startDate, endDate, content are required' });
    }
    const start = String(startDate).trim();
    const end = String(endDate).trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end)) {
        return res.status(400).json({ error: 'startDate/endDate must be YYYY-MM-DD' });
    }
    if (end < start) {
        return res.status(400).json({ error: 'endDate must be greater than or equal to startDate' });
    }
    try {
        await ensureCompanyScheduleSchema();
        const [result] = await pool.query(
            `
            INSERT INTO company_schedule (start_date, end_date, content, created_at, updated_at)
            VALUES (?, ?, ?, ${KST_NOW_SQL}, ${KST_NOW_SQL})
            `,
            [start, end, String(content).trim()]
        );
        return res.json({ ok: true, id: result.insertId });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed', detail: err.message });
    }
});

app.patch('/company/schedules/:id', async (req, res) => {
    const { id } = req.params;
    const { startDate, endDate, content } = req.body || {};
    try {
        await ensureCompanyScheduleSchema();
        const [rows] = await pool.query(
            'SELECT id, start_date, end_date, content FROM company_schedule WHERE id = ? LIMIT 1',
            [id]
        );
        const current = rows[0];
        if (!current) {
            return res.status(404).json({ error: 'Schedule not found' });
        }

        const nextStart = startDate !== undefined ? String(startDate || '').trim() : String(current.start_date || '').slice(0, 10);
        const nextEnd = endDate !== undefined ? String(endDate || '').trim() : String(current.end_date || '').slice(0, 10);
        const nextContent = content !== undefined ? String(content || '').trim() : String(current.content || '');

        if (!/^\d{4}-\d{2}-\d{2}$/.test(nextStart) || !/^\d{4}-\d{2}-\d{2}$/.test(nextEnd)) {
            return res.status(400).json({ error: 'startDate/endDate must be YYYY-MM-DD' });
        }
        if (nextEnd < nextStart) {
            return res.status(400).json({ error: 'endDate must be greater than or equal to startDate' });
        }
        if (!nextContent) {
            return res.status(400).json({ error: 'content is required' });
        }

        await pool.query(
            `
            UPDATE company_schedule
            SET start_date = ?, end_date = ?, content = ?, updated_at = ${KST_NOW_SQL}
            WHERE id = ?
            `,
            [nextStart, nextEnd, nextContent, id]
        );
        return res.json({ ok: true });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed', detail: err.message });
    }
});

app.delete('/company/schedules/:id', async (req, res) => {
    const { id } = req.params;
    try {
        await ensureCompanyScheduleSchema();
        await pool.query('DELETE FROM company_schedule WHERE id = ?', [id]);
        return res.json({ ok: true });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed', detail: err.message });
    }
});

app.patch('/tm/schedules/:id', async (req, res) => {
    const { id } = req.params;
    const {
        tmId,
        scheduleDate,
        scheduleType,
        customType,
        memo,
    } = req.body || {};
    const sessionTmId = getSessionTmId(req);
    const ownerTmId = sessionTmId;
    const isAdmin = await ensureAdminRequest(req);
    if (!ownerTmId && !isAdmin) {
        return res.status(401).json({ error: 'login required' });
    }

    try {
        await ensureTmScheduleSchema();
        const [rows] = await pool.query(
            'SELECT * FROM tm_schedule WHERE id = ? LIMIT 1',
            [id]
        );
        const current = rows[0];
        if (!current) {
            return res.status(404).json({ error: 'Schedule not found' });
        }
        if (!isAdmin && String(current.tm_id || '') !== String(ownerTmId || '')) {
            return res.status(403).json({ error: 'Only owner can edit this schedule' });
        }

        const setParts = [];
        const params = [];

        if (tmId !== undefined && tmId !== null && tmId !== '') {
            const nextTmId = Number(tmId);
            if (Number.isNaN(nextTmId) || nextTmId <= 0) {
                return res.status(400).json({ error: 'invalid tmId' });
            }
            if (isAdmin) {
                setParts.push('tm_id = ?');
                params.push(nextTmId);
            } else if (String(nextTmId) !== String(current.tm_id || '')) {
                return res.status(403).json({ error: 'Only owner can edit this schedule' });
            }
        }

        if (scheduleDate !== undefined) {
            const dateRaw = String(scheduleDate || '').trim();
            const dateMatch = dateRaw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
            if (!dateMatch) {
                return res.status(400).json({ error: 'scheduleDate must be YYYY-MM-DD' });
            }
            setParts.push('schedule_date = ?');
            params.push(dateRaw);
        }

        if (scheduleType !== undefined) {
            const type = String(scheduleType || '').trim();
            const allowedTypes = new Set(['휴무', '근무', '반차', '교육', '기타']);
            if (!allowedTypes.has(type)) {
                return res.status(400).json({ error: 'invalid scheduleType' });
            }
            setParts.push('schedule_type = ?');
            params.push(type);

            const trimmedCustom = customType ? String(customType).trim() : '';
            const normalizedCustom = type === '기타' ? trimmedCustom : '';
            if (type === '기타' && !normalizedCustom) {
                return res.status(400).json({ error: 'customType is required when scheduleType is 기타' });
            }
            setParts.push('custom_type = ?');
            params.push(normalizedCustom || null);
        } else if (customType !== undefined) {
            const trimmedCustom = customType ? String(customType).trim() : '';
            setParts.push('custom_type = ?');
            params.push(trimmedCustom || null);
        }

        if (memo !== undefined) {
            const nextMemo = String(memo || '').trim();
            setParts.push('memo = ?');
            params.push(nextMemo || null);
        }

        if (setParts.length === 0) {
            return res.status(400).json({ error: 'No fields to update' });
        }

        setParts.push(`updated_at = ${KST_NOW_SQL}`);
        params.push(id);
        await pool.query(
            `UPDATE tm_schedule SET ${setParts.join(', ')} WHERE id = ?`,
            params
        );
        const [afterRows] = await pool.query('SELECT * FROM tm_schedule WHERE id = ? LIMIT 1', [id]);
        if (isAdmin) {
            await writeAdminAuditLog(req, 'TM_SCHEDULE_UPDATE', 'tm_schedule', id, current, afterRows?.[0] || null);
        } else {
            await writeTmAuditLog(req, 'TM_SCHEDULE_UPDATE', 'tm_schedule', id, current, afterRows?.[0] || null);
        }
        return res.json({ ok: true });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed', detail: err.message });
    }
});

app.delete('/tm/schedules/:id', async (req, res) => {
    const { id } = req.params;
    const ownerTmId = getSessionTmId(req);
    const isAdmin = await ensureAdminRequest(req);
    if (!ownerTmId && !isAdmin) {
        return res.status(401).json({ error: 'login required' });
    }
    try {
        await ensureTmScheduleSchema();
        const [rows] = await pool.query(
            'SELECT * FROM tm_schedule WHERE id = ? LIMIT 1',
            [id]
        );
        const current = rows[0];
        if (!current) {
            return res.status(404).json({ error: 'Schedule not found' });
        }
        if (!isAdmin && String(current.tm_id || '') !== String(ownerTmId || '')) {
            return res.status(403).json({ error: 'Only owner can delete this schedule' });
        }
        await pool.query('DELETE FROM tm_schedule WHERE id = ?', [id]);
        if (isAdmin) await writeAdminAuditLog(req, 'TM_SCHEDULE_DELETE', 'tm_schedule', id, current, null);
        else await writeTmAuditLog(req, 'TM_SCHEDULE_DELETE', 'tm_schedule', id, current, null);
        return res.json({ ok: true });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed', detail: err.message });
    }
});

app.post('/tm/assign', async (req, res) => {
    if (!(await requireAdminApi(req, res))) return;
    const { leadId, tmId } = req.body || {};
    if (!leadId || tmId === undefined || tmId === null || tmId === '') {
        return res.status(400).json({ error: 'leadId and tmId are required' });
    }

    try {
        await ensureLeadAssignedDateColumn();
        const columns = await describeTable('tm_leads');
        const idCol = pickColumn(columns, ['id', 'lead_id', 'tm_lead_id']);
        const assignCol = pickColumn(columns, ['tm_id', 'tmid', 'assigned_tm_id', 'assigned_tm', 'tm']);
        const assignedAtCol = pickColumn(columns, ['배정날짜', 'assigned_at', 'assigned_date', 'tm_assigned_at']);

        if (!idCol || !assignCol) {
            return res.status(500).json({ error: 'tm_leads columns not found' });
        }

        const setSql = assignedAtCol
            ? `\`${assignCol}\` = ?, \`${assignedAtCol}\` = ${KST_NOW_SQL}`
            : `\`${assignCol}\` = ?`;
        const [result] = await pool.query(
            `UPDATE tm_leads
             SET ${setSql}
             WHERE \`${idCol}\` = ?
               AND (\`${assignCol}\` IS NULL OR \`${assignCol}\` = 0 OR \`${assignCol}\` = '')`,
            [tmId, leadId]
        );

        if (result.affectedRows === 0) {
            return res.status(409).json({ error: 'Already assigned or lead not found' });
        }

        return res.json({ ok: true });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed' });
    }
});

app.get('/tm/memos', async (req, res) => {
    const { phone, detailed, leadId, limit } = req.query || {};
    if (!phone) {
        return res.status(400).json({ error: 'phone is required' });
    }
    let normalizedPhone = String(phone).replace(/\D/g, '');
    if (normalizedPhone.startsWith('82')) {
        normalizedPhone = `0${normalizedPhone.slice(2)}`;
    }
    if (!normalizedPhone) {
        return res.status(400).json({ error: 'valid phone is required' });
    }
    const detailedMode = detailed === '1' || detailed === 'true' || detailed === true;
    const parsedLimit = Number(limit);
    const memoLimit = Number.isNaN(parsedLimit) || parsedLimit <= 0
        ? 0
        : Math.min(parsedLimit, 200);
    const excludeLeadId = Number(leadId);
    const hasExcludeLeadId = !Number.isNaN(excludeLeadId);

    try {
        const normalizePhoneSql = (col) => `
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(LOWER(${col}), 'p:', ''),
                            '-', ''),
                        ' ', ''),
                    '+82', '0'),
                '(', ''),
            ')', '')
        `;

        const sql = `
            SELECT
                m.id,
                m.memo_time,
                m.memo_content,
                m.status_tag,
                m.status_reservation_at,
                m.tm_id,
                t.name AS tm_name
            FROM tm_memos m
            LEFT JOIN tm t ON t.id = m.tm_id
            WHERE ${normalizePhoneSql('m.target_phone')} = ?
            ORDER BY m.memo_time DESC
            ${memoLimit > 0 ? 'LIMIT ?' : ''}
            `;
        const params = memoLimit > 0 ? [normalizedPhone, memoLimit] : [normalizedPhone];
        const [rows] = await pool.query(sql, params);

        if (!detailedMode) {
            return res.json(rows);
        }

        const eventSql = hasExcludeLeadId
            ? `
              SELECT DISTINCT l.\`이벤트\` AS event_name
              FROM tm_leads l
              WHERE ${normalizePhoneSql('l.연락처')} = ?
                AND l.id <> ?
                AND l.\`이벤트\` IS NOT NULL
                AND TRIM(l.\`이벤트\`) <> ''
              ORDER BY event_name ASC
              `
            : `
              SELECT DISTINCT l.\`이벤트\` AS event_name
              FROM tm_leads l
              WHERE ${normalizePhoneSql('l.연락처')} = ?
                AND l.\`이벤트\` IS NOT NULL
                AND TRIM(l.\`이벤트\`) <> ''
              ORDER BY event_name ASC
              `;
        const eventParams = hasExcludeLeadId ? [normalizedPhone, excludeLeadId] : [normalizedPhone];
        const [eventRows] = await pool.query(eventSql, eventParams);

        return res.json({
            memos: rows,
            events: (eventRows || []).map((row) => row.event_name).filter(Boolean),
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.patch('/tm/memos/:id', async (req, res) => {
    const { id } = req.params;
    const { memoContent, tmId } = req.body || {};
    const sessionTmId = getSessionTmId(req);

    if (!sessionTmId) {
        return res.status(401).json({ error: 'login required' });
    }
    if (tmId !== undefined && String(tmId) !== String(sessionTmId)) {
        return res.status(403).json({ error: 'tmId mismatch' });
    }
    if (!memoContent || !String(memoContent).trim()) {
        return res.status(400).json({ error: 'memoContent is required' });
    }

    try {
        const [rows] = await pool.query('SELECT * FROM tm_memos WHERE id = ? LIMIT 1', [id]);
        const memo = rows[0];
        if (!memo) {
            return res.status(404).json({ error: 'Memo not found' });
        }
        if (String(memo.tm_id || '') !== String(sessionTmId)) {
            return res.status(403).json({ error: 'Only author can edit this memo' });
        }

        await pool.query('UPDATE tm_memos SET memo_content = ? WHERE id = ?', [String(memoContent).trim(), id]);
        const [afterRows] = await pool.query('SELECT * FROM tm_memos WHERE id = ? LIMIT 1', [id]);
        await writeTmAuditLog(req, 'TM_MEMO_UPDATE', 'tm_memos', id, memo, afterRows?.[0] || null);
        return res.json({ ok: true });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed' });
    }
});

app.post('/tm/leads/:id/update', async (req, res) => {
    const { id } = req.params;
    const { status, region, memo, tmId, reservationAt, name, recallAt } = req.body || {};
    const sessionTmId = getSessionTmId(req);
    if (!sessionTmId) {
        return res.status(401).json({ error: 'login required' });
    }
    if (tmId !== undefined && String(tmId) !== String(sessionTmId)) {
        return res.status(403).json({ error: 'tmId mismatch' });
    }
    if (status === undefined && region === undefined && !memo && reservationAt === undefined && name === undefined && recallAt === undefined) {
        return res.status(400).json({ error: 'no changes provided' });
    }

    try {
        await ensureRecallColumns();
        const [rows] = await pool.query('SELECT * FROM tm_leads WHERE id = ?', [id]);
        const beforeLead = rows[0] || null;
        const currentStatus = beforeLead?.상태 ?? null;
        const currentReservationAt = beforeLead?.예약_내원일시 ?? null;
        const currentPhone = beforeLead?.연락처 ?? '';
        const statusProvided = status !== undefined;
        const statusChanged = statusProvided && status !== currentStatus;
        const nextStatus = statusProvided ? status : currentStatus;
        const callStatuses = ['부재중', '리콜대기', '예약', '실패'];
        const isMissed = nextStatus === '부재중';
        const isNoShow = nextStatus === '예약부도';
        const shouldApplyCallMetrics = statusChanged || isMissed;
        const incCall = shouldApplyCallMetrics && callStatuses.includes(nextStatus);
        const normalizedRecallAt = parseLocalDateTimeString(recallAt);
        if (recallAt !== undefined && recallAt !== null && recallAt !== '' && !normalizedRecallAt) {
            return res.status(400).json({ error: 'recallAt must be YYYY-MM-DD HH:mm[:ss]' });
        }

        const updates = [];
        const params = [];

        if (name !== undefined) {
            updates.push('이름 = COALESCE(?, 이름)');
            params.push(name || null);
        }
        if (status !== undefined) {
            updates.push('상태 = ?');
            params.push(status || null);
        }
        if (region !== undefined) {
            updates.push('거주지 = ?');
            params.push(region || null);
        }
        if (shouldApplyCallMetrics) {
            updates.push(`콜_날짜시간 = ${KST_NOW_SQL}`);
            updates.push('콜횟수 = COALESCE(콜횟수, 0) + ?');
            params.push(incCall ? 1 : 0);
            updates.push('부재중_횟수 = COALESCE(부재중_횟수, 0) + ?');
            params.push(isMissed ? 1 : 0);
            updates.push('예약부도_횟수 = COALESCE(예약부도_횟수, 0) + ?');
            params.push(isNoShow ? 1 : 0);
        }
        const shouldUpdateReservationAt =
            reservationAt !== undefined &&
            (reservationAt !== null && String(reservationAt).trim() !== '');
        const normalizedReservationAt = shouldUpdateReservationAt
            ? parseLocalDateTimeString(reservationAt)
            : null;
        if (shouldUpdateReservationAt && !normalizedReservationAt) {
            return res.status(400).json({ error: 'reservationAt must be YYYY-MM-DD HH:mm[:ss]' });
        }
        if (shouldUpdateReservationAt) {
            updates.push('예약_내원일시 = ?');
            params.push(normalizedReservationAt);
        }
        if (statusChanged && nextStatus === '리콜대기') {
            updates.push('리콜_예정일시 = ?');
            params.push(normalizedRecallAt);
            updates.push('리콜_완료여부 = 0');
        } else if (recallAt !== undefined) {
            updates.push('리콜_예정일시 = ?');
            params.push(normalizedRecallAt);
            if (normalizedRecallAt) {
                updates.push('리콜_완료여부 = 0');
            }
        }
        if (statusChanged && currentStatus === '리콜대기' && nextStatus !== '리콜대기') {
            updates.push('리콜_완료여부 = 1');
        }

        if (updates.length === 0) {
            return res.json({ ok: true, skipped: true });
        }

        const [result] = await pool.query(
            `UPDATE tm_leads
             SET ${updates.join(', ')}
             WHERE id = ?`,
            [
                ...params,
                id
            ]
        );

        if (result.affectedRows === 0) {
            return res.status(404).json({ error: 'Lead not found' });
        }
        const [afterLeadRows] = await pool.query('SELECT * FROM tm_leads WHERE id = ?', [id]);
        await writeTmAuditLog(req, 'TM_LEAD_UPDATE', 'tm_leads', id, beforeLead, afterLeadRows?.[0] || null);

        const shouldAutoStatusMemo = statusChanged && ['예약부도', '내원완료'].includes(String(nextStatus || '').trim());
        const finalMemoText = String(memo || '').trim() || (shouldAutoStatusMemo ? String(nextStatus || '').trim() : '');
        if (finalMemoText) {
            const memoStatusTag = String(nextStatus || '').trim() || null;
            const effectiveReservationAt = normalizedReservationAt || currentReservationAt;
            const memoStatusReservationAt = ['예약', '예약부도', '내원완료'].includes(memoStatusTag)
                ? (normalizeDateTimeForDb(effectiveReservationAt) || null)
                : null;
            await pool.query(
                `INSERT INTO tm_memos (
                    memo_time,
                    created_at,
                    memo_content,
                    status_tag,
                    status_reservation_at,
                    target_phone,
                    tm_id,
                    tm_lead_id
                ) VALUES (
                    ${KST_NOW_SQL},
                    ${KST_NOW_SQL},
                    ?, ?, ?, ?, ?, ?
                )`,
                [finalMemoText, memoStatusTag, memoStatusReservationAt, req.body.phone || currentPhone || '', sessionTmId, id]
            );
        }

        res.json({ ok: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.get('/tm/recalls', async (req, res) => {
    const tmId = getSessionTmId(req);
    const mode = String(req.query?.mode || 'all').toLowerCase();
    if (!tmId) {
        return res.status(401).json({ error: 'login required' });
    }
    if (!['all', 'due', 'upcoming'].includes(mode)) {
        return res.status(400).json({ error: 'mode must be all, due, or upcoming' });
    }
    try {
        await ensureRecallColumns();
        const where = ['tm = ?', "TRIM(COALESCE(`상태`, '')) = '리콜대기'", '`리콜_예정일시` IS NOT NULL'];
        const params = [String(tmId)];
        if (mode === 'due') where.push(`\`리콜_예정일시\` <= ${KST_NOW_SQL}`);
        if (mode === 'upcoming') where.push(`\`리콜_예정일시\` > ${KST_NOW_SQL}`);
        const [rows] = await pool.query(
            `
            SELECT *
            FROM tm_leads
            WHERE ${where.join(' AND ')}
            ORDER BY \`리콜_예정일시\` ASC, id ASC
            `
            , params
        );
        return res.json(rows || []);
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed' });
    }
});

app.post('/tm/reports/close', async (req, res) => {
    const { tmId, reportDate } = req.body || {};
    const sessionTmId = getSessionTmId(req);
    if (tmId !== undefined && String(tmId) !== String(sessionTmId || '')) {
        return res.status(403).json({ error: 'tmId mismatch' });
    }
    const targetTmId = sessionTmId;
    const targetDate = normalizeReportDate(reportDate);

    if (!targetTmId) {
        return res.status(401).json({ error: 'login required' });
    }
    if (!targetDate) {
        return res.status(400).json({ error: 'reportDate must be YYYY-MM-DD' });
    }

    const conn = await pool.getConnection();
    try {
        await ensureReportSchema();
        await conn.beginTransaction();
        const summary = await getDailySummaryRows(conn, targetTmId, targetDate);
        const upsert = await upsertReportBase(conn, targetTmId, targetDate, summary);
        await conn.query(
            `
            UPDATE tm_daily_report
            SET submitted_at = ${KST_NOW_SQL},
                updated_at = ${KST_NOW_SQL}
            WHERE id = ?
            `,
            [upsert.reportId]
        );
        await replaceReportLeads(conn, upsert.reportId, summary);

        await conn.commit();

        return res.json({
            ok: true,
            reportId: upsert.reportId,
            reportDate: targetDate,
            summary: {
                totalCallCount: upsert.totalCallCount,
                missedCount: upsert.missedCount,
                failedCount: upsert.failedCount,
                reservedCount: upsert.reservedCount,
                visitTodayCount: upsert.visitTodayCount,
                visitNextdayCount: upsert.visitNextdayCount,
            },
        });
    } catch (err) {
        await conn.rollback();
        console.error(err);
        return res.status(500).json({ error: 'Close report failed' });
    } finally {
        conn.release();
    }
});

app.get('/tm/reports/mine', async (req, res) => {
    const tmId = resolveTmId(req);
    if (!tmId) return res.status(401).json({ error: 'login required' });

    try {
        await ensureReportSchema();
        const [rows] = await pool.query(
            `
            SELECT
                id,
                tm_id,
                report_date,
                total_call_count,
                missed_count,
                failed_count,
                reserved_count,
                visit_today_count,
                visit_nextday_count,
                manual_reserved_count,
                manual_failed_count,
                manual_visit_today_count,
                manual_visit_nextday_count,
                manual_call_count,
                check_db_crm,
                check_inhouse_crm,
                check_sheet,
                is_submitted,
                submitted_at,
                updated_at
            FROM tm_daily_report
            WHERE tm_id = ?
            ORDER BY report_date DESC, id DESC
            LIMIT 90
            `,
            [String(tmId)]
        );
        return res.json(rows);
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch my reports failed' });
    }
});

app.get('/tm/reports/me', async (req, res) => {
    const tmId = resolveTmId(req);
    const reportDate = normalizeReportDate(req.query?.date);
    if (!tmId) return res.status(401).json({ error: 'login required' });
    if (!reportDate) return res.status(400).json({ error: 'date must be YYYY-MM-DD' });

    try {
        await ensureReportSchema();
        const [rows] = await pool.query(
            `
            SELECT
                id,
                tm_id,
                report_date,
                total_call_count,
                missed_count,
                failed_count,
                reserved_count,
                visit_today_count,
                visit_nextday_count,
                manual_reserved_count,
                manual_failed_count,
                manual_visit_today_count,
                manual_visit_nextday_count,
                manual_call_count,
                check_db_crm,
                check_inhouse_crm,
                check_sheet,
                is_submitted,
                submitted_at,
                updated_at
            FROM tm_daily_report
            WHERE tm_id = ?
              AND report_date = ?
            LIMIT 1
            `,
            [String(tmId), reportDate]
        );
        return res.json(rows[0] || null);
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch my report failed' });
    }
});

app.post('/tm/reports/draft', async (req, res) => {
    const tmId = resolveTmId(req);
    const {
        reportDate,
        manualReservedCount,
        manualFailedCount,
        manualVisitTodayCount,
        manualVisitNextdayCount,
        manualCallCount,
        checkDbCrm,
        checkInhouseCrm,
        checkSheet,
    } = req.body || {};

    const targetDate = normalizeReportDate(reportDate);
    if (!tmId) return res.status(401).json({ error: 'login required' });
    if (!targetDate) return res.status(400).json({ error: 'reportDate must be YYYY-MM-DD' });

    const conn = await pool.getConnection();
    try {
        await ensureReportSchema();
        await conn.beginTransaction();
          const summary = await getDailySummaryRows(conn, tmId, targetDate);
          const upsert = await upsertReportBase(conn, tmId, targetDate, summary);
          await replaceReportLeads(conn, upsert.reportId, summary);

        await conn.query(
            `
            UPDATE tm_daily_report
            SET
                manual_reserved_count = COALESCE(?, manual_reserved_count),
                manual_failed_count = COALESCE(?, manual_failed_count),
                manual_visit_today_count = COALESCE(?, manual_visit_today_count),
                manual_visit_nextday_count = COALESCE(?, manual_visit_nextday_count),
                manual_call_count = COALESCE(?, manual_call_count),
                check_db_crm = COALESCE(?, check_db_crm),
                check_inhouse_crm = COALESCE(?, check_inhouse_crm),
                check_sheet = COALESCE(?, check_sheet),
                updated_at = ${KST_NOW_SQL}
            WHERE id = ?
            `,
            [
                normalizeNullableInt(manualReservedCount),
                normalizeNullableInt(manualFailedCount),
                normalizeNullableInt(manualVisitTodayCount),
                normalizeNullableInt(manualVisitNextdayCount),
                normalizeNullableInt(manualCallCount),
                checkDbCrm === undefined ? null : normalizeBool(checkDbCrm),
                checkInhouseCrm === undefined ? null : normalizeBool(checkInhouseCrm),
                checkSheet === undefined ? null : normalizeBool(checkSheet),
                upsert.reportId,
            ]
        );

        const [rows] = await conn.query(
            `
            SELECT
                id,
                tm_id,
                report_date,
                total_call_count,
                missed_count,
                failed_count,
                reserved_count,
                visit_today_count,
                visit_nextday_count,
                manual_reserved_count,
                manual_failed_count,
                manual_visit_today_count,
                manual_visit_nextday_count,
                manual_call_count,
                check_db_crm,
                check_inhouse_crm,
                check_sheet,
                is_submitted,
                submitted_at,
                updated_at
            FROM tm_daily_report
            WHERE id = ?
            LIMIT 1
            `,
            [upsert.reportId]
        );

        await conn.commit();
        return res.json({ ok: true, report: rows[0] || null });
    } catch (err) {
        await conn.rollback();
        console.error(err);
        return res.status(500).json({ error: 'Save draft failed' });
    } finally {
        conn.release();
    }
});

app.get('/tm/reports/draft', async (req, res) => {
    const tmId = resolveTmId(req);
    const targetDate = normalizeReportDate(req.query?.reportDate || req.query?.date);
    if (!tmId) return res.status(401).json({ error: 'login required' });
    if (!targetDate) return res.status(400).json({ error: 'reportDate must be YYYY-MM-DD' });

    const conn = await pool.getConnection();
    try {
        await ensureReportSchema();
        await conn.beginTransaction();
          const summary = await getDailySummaryRows(conn, tmId, targetDate);
          const upsert = await upsertReportBase(conn, tmId, targetDate, summary);
          await replaceReportLeads(conn, upsert.reportId, summary);

        const [rows] = await conn.query(
            `
            SELECT
                id,
                tm_id,
                report_date,
                total_call_count,
                missed_count,
                failed_count,
                reserved_count,
                visit_today_count,
                visit_nextday_count,
                manual_reserved_count,
                manual_failed_count,
                manual_visit_today_count,
                manual_visit_nextday_count,
                manual_call_count,
                check_db_crm,
                check_inhouse_crm,
                check_sheet,
                is_submitted,
                submitted_at,
                updated_at
            FROM tm_daily_report
            WHERE id = ?
            LIMIT 1
            `,
            [upsert.reportId]
        );

        await conn.commit();
        return res.json({ ok: true, report: rows[0] || null });
    } catch (err) {
        await conn.rollback();
        console.error(err);
        return res.status(500).json({ error: 'Fetch draft failed' });
    } finally {
        conn.release();
    }
});

app.post('/tm/reports/submit', async (req, res) => {
    const tmId = resolveTmId(req);
    const {
        reportDate,
        manualReservedCount,
        manualFailedCount,
        manualVisitTodayCount,
        manualVisitNextdayCount,
        manualCallCount,
        checkDbCrm,
        checkInhouseCrm,
        checkSheet,
    } = req.body || {};

    const targetDate = normalizeReportDate(reportDate);
    if (!tmId) return res.status(401).json({ error: 'login required' });
    if (!targetDate) return res.status(400).json({ error: 'reportDate must be YYYY-MM-DD' });

    const checklist = {
        checkDbCrm: normalizeBool(checkDbCrm),
        checkInhouseCrm: normalizeBool(checkInhouseCrm),
        checkSheet: normalizeBool(checkSheet),
    };
    if (!(checklist.checkDbCrm && checklist.checkInhouseCrm && checklist.checkSheet)) {
        return res.status(400).json({ error: '모든 당일 기입 항목을 완료해야 제출할 수 있습니다.' });
    }

    const conn = await pool.getConnection();
    try {
        await ensureReportSchema();
        await conn.beginTransaction();
        const summary = await getDailySummaryRows(conn, tmId, targetDate);
        const upsert = await upsertReportBase(conn, tmId, targetDate, summary);

        await conn.query(
            `
            UPDATE tm_daily_report
            SET
                manual_reserved_count = ?,
                manual_failed_count = ?,
                manual_visit_today_count = ?,
                manual_visit_nextday_count = ?,
                manual_call_count = ?,
                check_db_crm = ?,
                check_inhouse_crm = ?,
                check_sheet = ?,
                is_submitted = 1,
                submitted_at = ${KST_NOW_SQL},
                updated_at = ${KST_NOW_SQL}
            WHERE id = ?
            `,
            [
                normalizeNullableInt(manualReservedCount),
                normalizeNullableInt(manualFailedCount),
                normalizeNullableInt(manualVisitTodayCount),
                normalizeNullableInt(manualVisitNextdayCount),
                normalizeNullableInt(manualCallCount),
                checklist.checkDbCrm,
                checklist.checkInhouseCrm,
                checklist.checkSheet,
                upsert.reportId,
            ]
        );

        await replaceReportLeads(conn, upsert.reportId, summary);
        await conn.commit();

        return res.json({
            ok: true,
            reportId: upsert.reportId,
            reportDate: targetDate,
            summary: {
                totalCallCount: upsert.totalCallCount,
                missedCount: upsert.missedCount,
                failedCount: upsert.failedCount,
                reservedCount: upsert.reservedCount,
                visitTodayCount: upsert.visitTodayCount,
                visitNextdayCount: upsert.visitNextdayCount,
            },
        });
    } catch (err) {
        await conn.rollback();
        console.error(err);
        return res.status(500).json({ error: 'Submit report failed' });
    } finally {
        conn.release();
    }
});

app.get('/tm/reports/:reportId/full', async (req, res) => {
    const tmId = resolveTmId(req);
    const reportId = Number(req.params?.reportId);
    if (!tmId) return res.status(401).json({ error: 'login required' });
    if (Number.isNaN(reportId)) return res.status(400).json({ error: 'valid reportId is required' });

    try {
        await ensureReportSchema();
        const [reportRows] = await pool.query(
            `
            SELECT
                r.*,
                t.name AS tm_name
            FROM tm_daily_report r
            INNER JOIN tm t ON t.id = r.tm_id
            WHERE r.id = ?
              AND r.tm_id = ?
            LIMIT 1
            `,
            [reportId, String(tmId)]
        );
        const report = reportRows[0];
        if (!report) return res.status(404).json({ error: 'report not found' });

        const [leadRows] = await pool.query(
            `
            SELECT
                metric_type,
                lead_id,
                name_snapshot,
                phone_snapshot,
                status_snapshot,
                reservation_at_snapshot,
                memo_snapshot
            FROM tm_daily_report_leads
            WHERE report_id = ?
            ORDER BY id DESC
            `,
            [reportId]
        );
        const grouped = {
            MISSED: [],
            FAILED: [],
            RESERVED: [],
            VISIT_TODAY: [],
            VISIT_NEXTDAY: [],
            RECALL_WAIT: [],
        };
        leadRows.forEach((row) => {
            if (!grouped[row.metric_type]) grouped[row.metric_type] = [];
            grouped[row.metric_type].push(row);
        });
        const [recallRows] = await pool.query(
            `
            SELECT
                id AS lead_id,
                이름 AS name_snapshot,
                연락처 AS phone_snapshot,
                상태 AS status_snapshot,
                리콜_예정일시 AS recall_at_snapshot
            FROM tm_leads
            WHERE TRIM(COALESCE(tm, '')) = ?
              AND DATE(콜_날짜시간) = ?
              AND TRIM(COALESCE(상태, '')) = '리콜대기'
            ORDER BY 리콜_예정일시 ASC, id DESC
            `,
            [String(report.tm_id), report.report_date]
        );
        grouped.RECALL_WAIT = recallRows || [];
        return res.json({ report, leads: grouped });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch report full failed' });
    }
});

app.get('/admin/reports/daily', async (req, res) => {
    const targetDate = normalizeReportDate(req.query?.date);
    if (!targetDate) {
        return res.status(400).json({ error: 'date must be YYYY-MM-DD' });
    }

    try {
        await ensureReportSchema();
        const [rows] = await pool.query(
            `
            SELECT
                r.id,
                r.tm_id,
                t.name AS tm_name,
                r.report_date,
                r.total_call_count,
                r.missed_count,
                r.failed_count,
                r.reserved_count,
                r.visit_today_count,
                r.visit_nextday_count,
                r.manual_reserved_count,
                r.manual_failed_count,
                r.manual_visit_today_count,
                r.manual_visit_nextday_count,
                r.manual_call_count,
                r.check_db_crm,
                r.check_inhouse_crm,
                r.check_sheet,
                r.is_submitted,
                r.submitted_at
            FROM tm_daily_report r
            INNER JOIN tm t ON t.id = r.tm_id
            WHERE r.report_date = ?
            ORDER BY t.name ASC, r.id DESC
            `,
            [targetDate]
        );
        return res.json({ date: targetDate, reports: rows });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch daily reports failed' });
    }
});

app.get('/admin/reports/:reportId/leads', async (req, res) => {
    const { reportId } = req.params;
    const metric = String(req.query?.metric || '').toUpperCase();

    if (!reportId || Number.isNaN(Number(reportId))) {
        return res.status(400).json({ error: 'valid reportId is required' });
    }
    if (!REPORT_METRIC_TYPES.has(metric)) {
        return res.status(400).json({ error: 'metric must be one of MISSED, FAILED, RESERVED, VISIT_TODAY, VISIT_NEXTDAY, RECALL_WAIT' });
    }

    try {
        if (metric === 'RECALL_WAIT') {
            const [reportRows] = await pool.query(
                `
                SELECT id, tm_id, report_date
                FROM tm_daily_report
                WHERE id = ?
                LIMIT 1
                `,
                [reportId]
            );
            const report = reportRows[0];
            if (!report) return res.status(404).json({ error: 'report not found' });

            const [recallRows] = await pool.query(
                `
                SELECT
                    id AS lead_id,
                    이름 AS name_snapshot,
                    연락처 AS phone_snapshot,
                    상태 AS status_snapshot,
                    리콜_예정일시 AS recall_at_snapshot
                FROM tm_leads
                WHERE TRIM(COALESCE(tm, '')) = ?
                  AND DATE(콜_날짜시간) = ?
                  AND TRIM(COALESCE(상태, '')) = '리콜대기'
                ORDER BY 리콜_예정일시 ASC, id DESC
                `,
                [String(report.tm_id), report.report_date]
            );
            return res.json({ reportId: Number(reportId), metric, leads: recallRows || [] });
        }

        const [rows] = await pool.query(
            `
            SELECT
                id,
                lead_id,
                name_snapshot,
                phone_snapshot,
                status_snapshot,
                reservation_at_snapshot,
                memo_snapshot,
                created_at
            FROM tm_daily_report_leads
            WHERE report_id = ?
              AND metric_type = ?
            ORDER BY id DESC
            `,
            [reportId, metric]
        );
        return res.json({ reportId: Number(reportId), metric, leads: rows });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch report leads failed' });
    }
});

app.get('/admin/reports/:reportId/full', async (req, res) => {
    const reportId = Number(req.params?.reportId);
    if (Number.isNaN(reportId)) return res.status(400).json({ error: 'valid reportId is required' });

    try {
        await ensureReportSchema();
        const [reportRows] = await pool.query(
            `
            SELECT
                r.*,
                t.name AS tm_name
            FROM tm_daily_report r
            INNER JOIN tm t ON t.id = r.tm_id
            WHERE r.id = ?
            LIMIT 1
            `,
            [reportId]
        );
        const report = reportRows[0];
        if (!report) return res.status(404).json({ error: 'report not found' });

        const [leadRows] = await pool.query(
            `
            SELECT
                metric_type,
                lead_id,
                name_snapshot,
                phone_snapshot,
                status_snapshot,
                reservation_at_snapshot,
                memo_snapshot
            FROM tm_daily_report_leads
            WHERE report_id = ?
            ORDER BY id DESC
            `,
            [reportId]
        );
        const grouped = {
            MISSED: [],
            FAILED: [],
            RESERVED: [],
            VISIT_TODAY: [],
            VISIT_NEXTDAY: [],
            RECALL_WAIT: [],
        };
        leadRows.forEach((row) => {
            if (!grouped[row.metric_type]) grouped[row.metric_type] = [];
            grouped[row.metric_type].push(row);
        });
        const [recallRows] = await pool.query(
            `
            SELECT
                id AS lead_id,
                이름 AS name_snapshot,
                연락처 AS phone_snapshot,
                상태 AS status_snapshot,
                리콜_예정일시 AS recall_at_snapshot
            FROM tm_leads
            WHERE TRIM(COALESCE(tm, '')) = ?
              AND DATE(콜_날짜시간) = ?
              AND TRIM(COALESCE(상태, '')) = '리콜대기'
            ORDER BY 리콜_예정일시 ASC, id DESC
            `,
            [String(report.tm_id), report.report_date]
        );
        grouped.RECALL_WAIT = recallRows || [];
        return res.json({ report, leads: grouped });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Fetch report full failed' });
    }
});

app.post('/admin/sync-meta-leads', async (req, res) => {
    try {
        const [colRows] = await pool.query(`
            SELECT COUNT(*) AS cnt
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'tm_leads'
              AND COLUMN_NAME = 'meta_id'
        `);
        if (colRows[0]?.cnt === 0) {
            await pool.query('ALTER TABLE tm_leads ADD COLUMN meta_id varchar(50) NULL');
        }

        const [ruleRows] = await pool.query(`
            SELECT id, name, keywords
            FROM event_rules
            ORDER BY LENGTH(keywords) DESC, id ASC
        `);
        const rules = (ruleRows || []).map((row) => {
            const keywords = String(row.keywords || '')
                .split(',')
                .map((part) => part.trim())
                .filter(Boolean)
                .map((part) => part.replace(/\s+/g, ''));
            return {
                id: row.id,
                name: row.name,
                keywords,
            };
        });

        const [metaRows] = await pool.query(`
            SELECT
                id,
                created_time,
                full_name,
                ad_name,
                adset_name,
                phone_number,
                상담_희망_시간을_선택해주세요
            FROM meta_leads m
            WHERE NOT EXISTS (
                SELECT 1 FROM tm_leads t WHERE t.meta_id = m.id
            )
        `);

        if (!metaRows.length) {
            return res.json({ inserted: 0 });
        }

        const normalize = (value) => String(value || '').replace(/\s+/g, '');
        const pickEvent = (adsetName, adName) => {
            const target = normalize(adsetName || adName);
            for (const rule of rules) {
                if (!rule.keywords.length) continue;
                const ok = rule.keywords.every((kw) => target.includes(kw));
                if (ok) return rule.name;
            }
            return null;
        };

        const conn = await pool.getConnection();
        try {
            await conn.beginTransaction();
            const insertSql = `
                INSERT INTO tm_leads (
                    meta_id,
                    인입날짜,
                    이름,
                    연락처,
                    상담가능시간,
                    이벤트
                ) VALUES (?, ?, ?, ?, ?, ?)
            `;
            let inserted = 0;
            for (const row of metaRows) {
                const rawPhone = String(row.phone_number || '');
                const digitsOnly = rawPhone.replace(/\D/g, '');
                let phone = '';
                if (digitsOnly) {
                    phone = digitsOnly.startsWith('82')
                        ? `0${digitsOnly.slice(2)}`
                        : digitsOnly;
                }
                if (phone.length > 30) {
                    phone = phone.slice(0, 30);
                }
                const eventName = pickEvent(row.adset_name, row.ad_name);
                const [result] = await conn.query(insertSql, [
                    row.id,
                    toKstDateTimeString(row.created_time) || null,
                    row.full_name || null,
                    phone || null,
                    row.상담_희망_시간을_선택해주세요 || null,
                    eventName,
                ]);
                inserted += result.affectedRows || 0;
            }
            await conn.commit();
            res.json({ inserted });
        } catch (innerErr) {
            await conn.rollback();
            throw innerErr;
        } finally {
            conn.release();
        }
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Sync failed' });
    }
});

app.post('/admin/leads', async (req, res) => {
    const { name, phone, event, tmId } = req.body || {};
    const normalizedName = String(name || '').trim();
    const normalizedPhone = String(phone || '').trim();
    const normalizedEvent = String(event || '').trim();

    if (!normalizedName || !normalizedPhone || !normalizedEvent) {
        return res.status(400).json({ error: 'name, phone, event are required' });
    }

    try {
        await ensureLeadAssignedDateColumn();
        const normalizedTm = (tmId === undefined || tmId === null || String(tmId).trim() === '')
            ? null
            : String(tmId).trim();
        const hasTm = normalizedTm !== null;

        const [result] = await pool.query(
            `
            INSERT INTO tm_leads (
                \`인입날짜\`,
                \`이름\`,
                \`연락처\`,
                \`이벤트\`,
                \`tm\`,
                \`배정날짜\`
            ) VALUES (
                ${KST_NOW_SQL},
                ?, ?, ?, ?,
                ${hasTm ? KST_NOW_SQL : 'NULL'}
            )
            `,
            [normalizedName, normalizedPhone, normalizedEvent, normalizedTm]
        );

        return res.json({ ok: true, id: result.insertId });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Create lead failed' });
    }
});

app.post('/admin/leads/:id/update', async (req, res) => {
    const { id } = req.params;
    const { status, region, memo, tmId, reservationAt, name, event } = req.body || {};
    if (!status && region === undefined && !memo && tmId === undefined && reservationAt === undefined && name === undefined && event === undefined) {
        return res.status(400).json({ error: 'no changes provided' });
    }

    try {
        await ensureLeadAssignedDateColumn();
        const [leadRows] = await pool.query('SELECT * FROM tm_leads WHERE id = ?', [id]);
        const lead = leadRows?.[0];
        if (!lead) {
            return res.status(404).json({ error: 'Lead not found' });
        }
        const beforeLead = lead;
        const currentStatus = lead.상태 ?? null;
        const currentTm = lead.tm ?? null;
        const currentReservationAt = lead.예약_내원일시 ?? null;
        const currentPhone = lead.연락처 ?? '';
        const statusProvided = status !== undefined;
        const statusChanged = statusProvided && status !== currentStatus;

        const updates = [];
        const params = [];

        const shouldUpdateReservationAt =
            reservationAt !== undefined &&
            (reservationAt !== null && String(reservationAt).trim() !== '');
        const normalizedReservationAt = shouldUpdateReservationAt
            ? parseLocalDateTimeString(reservationAt)
            : null;
        if (shouldUpdateReservationAt && !normalizedReservationAt) {
            return res.status(400).json({ error: 'reservationAt must be YYYY-MM-DD HH:mm[:ss]' });
        }

        if (statusChanged) {
            updates.push('상태 = ?');
            params.push(status);
            if (shouldUpdateReservationAt) {
                updates.push('예약_내원일시 = ?');
                params.push(normalizedReservationAt);
            }
        } else if (shouldUpdateReservationAt) {
            updates.push('예약_내원일시 = ?');
            params.push(normalizedReservationAt);
        }

        const callStatuses = ['부재중', '리콜대기', '예약', '실패'];
        const isMissed = status === '부재중';
        const isNoShow = status === '예약부도';
        const shouldApplyCallMetrics = statusChanged;
        const incCall = shouldApplyCallMetrics && callStatuses.includes(status);
        if (shouldApplyCallMetrics) {
            updates.push(`콜_날짜시간 = ${KST_NOW_SQL}`);
            updates.push('콜횟수 = COALESCE(콜횟수, 0) + ?');
            params.push(incCall ? 1 : 0);
            updates.push('부재중_횟수 = COALESCE(부재중_횟수, 0) + ?');
            params.push(isMissed ? 1 : 0);
            updates.push('예약부도_횟수 = COALESCE(예약부도_횟수, 0) + ?');
            params.push(isNoShow ? 1 : 0);
        }

        if (region !== undefined) {
            updates.push('거주지 = ?');
            params.push(region || null);
        }

        if (name !== undefined) {
            updates.push('이름 = ?');
            params.push(name || null);
        }

        if (event !== undefined) {
            updates.push('이벤트 = ?');
            params.push(event || null);
        }

        if (tmId !== undefined) {
            updates.push('tm = ?');
            params.push(tmId || null);
            if (tmId && String(tmId) !== String(currentTm || '')) {
                updates.push(`배정날짜 = ${KST_NOW_SQL}`);
            }
        }

        if (updates.length === 0) {
            return res.json({ ok: true, skipped: true });
        }

        const [result] = await pool.query(
            `UPDATE tm_leads SET ${updates.join(', ')} WHERE id = ?`,
            [...params, id]
        );

        if (result.affectedRows === 0) {
            return res.status(404).json({ error: 'Lead not found' });
        }
        const [afterLeadRows] = await pool.query('SELECT * FROM tm_leads WHERE id = ?', [id]);
        const afterLead = afterLeadRows?.[0] || null;
        await writeAdminAuditLog(req, 'LEAD_UPDATE', 'tm_leads', id, beforeLead, afterLead);

        const nextStatus = statusProvided ? status : currentStatus;
        const shouldAutoStatusMemo = statusChanged && ['예약부도', '내원완료'].includes(String(nextStatus || '').trim());
        const finalMemoText = String(memo || '').trim() || (shouldAutoStatusMemo ? String(nextStatus || '').trim() : '');
        if (finalMemoText) {
            const memoStatusTag = String(nextStatus || '').trim() || null;
            const effectiveReservationAt = normalizedReservationAt || currentReservationAt;
            const memoStatusReservationAt = ['예약', '예약부도', '내원완료'].includes(memoStatusTag)
                ? (normalizeDateTimeForDb(effectiveReservationAt) || null)
                : null;
            await pool.query(
                `INSERT INTO tm_memos (
                    memo_time,
                    created_at,
                    memo_content,
                    status_tag,
                    status_reservation_at,
                    target_phone,
                    tm_id,
                    tm_lead_id
                ) VALUES (
                    ${KST_NOW_SQL},
                    ${KST_NOW_SQL},
                    ?, ?, ?, ?, ?, ?
                )`,
                [finalMemoText, memoStatusTag, memoStatusReservationAt, req.body.phone || currentPhone || '', tmId || 0, id]
            );
        }

        res.json({ ok: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.post('/admin/leads/reassign-bulk', async (req, res) => {
    const { leadIds, tmId } = req.body || {};
    if (!Array.isArray(leadIds) || leadIds.length === 0 || tmId === undefined || tmId === null || tmId === '') {
        return res.status(400).json({ error: 'leadIds and tmId are required' });
    }

    try {
        const columns = await describeTable('tm_leads');
        const idCol = pickColumn(columns, ['id', 'lead_id', 'tm_lead_id']);
        const assignCol = pickColumn(columns, ['tm_id', 'tmid', 'assigned_tm_id', 'assigned_tm', 'tm']);
        const assignedAtCol = pickColumn(columns, ['배정날짜', 'assigned_at', 'assigned_date', 'tm_assigned_at']);

        if (!idCol || !assignCol || !assignedAtCol) {
            return res.status(500).json({ error: 'tm_leads columns not found' });
        }

        const normalizedLeadIds = Array.from(
            new Set(
                leadIds
                    .map((v) => Number(v))
                    .filter((v) => Number.isInteger(v) && v > 0)
            )
        );

        if (normalizedLeadIds.length === 0) {
            return res.status(400).json({ error: 'valid leadIds are required' });
        }

        const placeholders = normalizedLeadIds.map(() => '?').join(', ');
        const [beforeRows] = await pool.query(
            `SELECT \`${idCol}\` AS id, \`${assignCol}\` AS tm, \`${assignedAtCol}\` AS assigned_at
             FROM tm_leads
             WHERE \`${idCol}\` IN (${placeholders})`,
            normalizedLeadIds
        );
        const [result] = await pool.query(
            `UPDATE tm_leads
             SET \`${assignCol}\` = ?, \`${assignedAtCol}\` = ${KST_NOW_SQL}
             WHERE \`${idCol}\` IN (${placeholders})`,
            [tmId, ...normalizedLeadIds]
        );
        const [afterRows] = await pool.query(
            `SELECT \`${idCol}\` AS id, \`${assignCol}\` AS tm, \`${assignedAtCol}\` AS assigned_at
             FROM tm_leads
             WHERE \`${idCol}\` IN (${placeholders})`,
            normalizedLeadIds
        );
        await writeAdminAuditLog(
            req,
            'LEAD_REASSIGN_BULK',
            'tm_leads',
            normalizedLeadIds.join(','),
            { tmId, rows: beforeRows },
            { tmId, rows: afterRows, updated: result.affectedRows || 0 }
        );

        return res.json({ ok: true, updated: result.affectedRows || 0 });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed' });
    }
});

app.get('/admin/event-rules', async (req, res) => {
    try {
        const [rows] = await pool.query(
            'SELECT id, name, keywords, created_at FROM event_rules ORDER BY id DESC'
        );
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Fetch failed' });
    }
});

app.post('/admin/event-rules', async (req, res) => {
    const { name, keywords } = req.body || {};
    if (!name || !keywords) {
        return res.status(400).json({ error: 'name and keywords are required' });
    }
    try {
        const [result] = await pool.query(
            'INSERT INTO event_rules (name, keywords) VALUES (?, ?)',
            [name, keywords]
        );
        await writeAdminAuditLog(
            req,
            'EVENT_RULE_CREATE',
            'event_rules',
            result.insertId,
            null,
            { id: result.insertId, name, keywords }
        );
        res.json({ ok: true, id: result.insertId });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Create failed' });
    }
});

app.delete('/admin/event-rules/:id', async (req, res) => {
    const { id } = req.params;
    try {
        const [beforeRows] = await pool.query('SELECT * FROM event_rules WHERE id = ? LIMIT 1', [id]);
        const before = beforeRows?.[0] || null;
        const [result] = await pool.query('DELETE FROM event_rules WHERE id = ?', [id]);
        if (result.affectedRows === 0) {
            return res.status(404).json({ error: 'Rule not found' });
        }
        await writeAdminAuditLog(req, 'EVENT_RULE_DELETE', 'event_rules', id, before, null);
        res.json({ ok: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Delete failed' });
    }
});

app.get('/admin/audit-logs', async (req, res) => {
    if (!(await requireAdminApi(req, res))) return;
    try {
        await ensureActivityLogSchema();
        const {
            action = '',
            targetType = '',
            adminTmId = '',
            actorRole = '',
            limit = '100',
        } = req.query || {};
        const limitNum = Math.max(1, Math.min(500, Number(limit) || 100));
        const where = [];
        const params = [];
        if (String(action).trim()) {
            where.push('l.action = ?');
            params.push(String(action).trim());
        }
        if (String(targetType).trim()) {
            where.push('l.target_type = ?');
            params.push(String(targetType).trim());
        }
        if (String(adminTmId).trim()) {
            where.push('l.actor_tm_id = ?');
            params.push(String(adminTmId).trim());
        }
        if (String(actorRole).trim()) {
            where.push('l.actor_role = ?');
            params.push(String(actorRole).trim().toUpperCase());
        }
        const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
        const [rows] = await pool.query(
            `
            SELECT
                l.id,
                l.actor_tm_id AS admin_tm_id,
                t.name AS admin_name,
                l.actor_role,
                l.action,
                l.target_type,
                l.target_id,
                l.before_json,
                l.after_json,
                l.ip_address AS ip_address,
                l.user_agent,
                l.created_at
            FROM activity_logs l
            LEFT JOIN tm t ON t.id = l.actor_tm_id
            ${whereSql}
            ORDER BY l.id DESC
            LIMIT ?
            `,
            [...params, limitNum]
        );
        return res.json(rows);
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed' });
    }
});

app.get('/tm/leads/export', async (req, res) => {
    if (!(await requireAdminApi(req, res))) return;
    try {
        const columns = await describeTable('tm_leads');
        const map = {
            id: pickColumn(columns, ['id', 'lead_id', 'tm_lead_id']),
            name: pickColumn(columns, ['name', 'customer_name', 'client_name', 'user_name', '이름']),
            phone: pickColumn(columns, ['phone', 'phone_number', 'tel', 'mobile', '연락처']),
            availableTime: pickColumn(columns, ['available_time', 'availabletime', 'call_time', 'time_range', 'available_hour', '상담가능시간']),
            event: pickColumn(columns, ['event', 'event_name', 'campaign', 'source', '이벤트']),
            inboundDate: pickColumn(columns, ['inbound_date', 'in_date', 'created_at', 'createdat', 'reg_date', 'created', '인입날짜']),
            assignedTm: pickColumn(columns, ['tm_id', 'tmid', 'assigned_tm_id', 'assigned_tm', 'tm']),
        };

        const [rows] = await pool.query('SELECT * FROM tm_leads');
        let leads = rows.map((row) => normalizeLeadRow(row, map));

        if (map.assignedTm) {
            leads = leads.filter((lead) => {
                const val = lead.assignedTm;
                return val === null || val === undefined || val === '';
            });
        }

        if (map.inboundDate) {
            leads.sort((a, b) => {
                const aTime = new Date(a.inboundDate).getTime();
                const bTime = new Date(b.inboundDate).getTime();
                if (Number.isNaN(aTime) || Number.isNaN(bTime)) return 0;
                return bTime - aTime;
            });
        }

        const workbook = new ExcelJS.Workbook();
        const sheet = workbook.addWorksheet('TM배정');
        sheet.columns = [
            { header: '인입시간', key: 'inboundDate', width: 20 },
            { header: '이름', key: 'name', width: 18 },
            { header: '연락처', key: 'phone', width: 18 },
            { header: '상담가능시간', key: 'availableTime', width: 20 },
            { header: '이벤트', key: 'event', width: 16 },
        ];

        leads.forEach((lead) => {
            sheet.addRow({
                inboundDate: lead.inboundDate ? formatDateTime(lead.inboundDate) : '',
                name: lead.name || '',
                phone: lead.phone ? formatPhone(lead.phone) : '',
                availableTime: lead.availableTime || '',
                event: lead.event || '',
            });
        });

        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', 'attachment; filename="tm_leads.xlsx"');
        await workbook.xlsx.write(res);
        res.end();
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Export failed' });
    }
});

app.get('/dbdata/export', async (req, res) => {
    try {
        const { tm, status, callMin, missMin, region, memo, event, name, phone, assignedTodayOnly } = req.query || {};
        const where = [];
        const params = [];

        if (tm && tm !== 'all') {
            where.push('l.`tm` = ?');
            params.push(tm);
        }
        if (status && status !== 'all') {
            where.push('l.`상태` LIKE ?');
            params.push(`%${status}%`);
        }
        if (name) {
            where.push('l.`이름` LIKE ?');
            params.push(`%${name}%`);
        }
        if (phone) {
            const normalizedPhone = String(phone).replace(/\D/g, '');
            if (normalizedPhone) {
                where.push('REPLACE(REPLACE(REPLACE(COALESCE(l.`연락처`, \'\'), \'-\', \'\'), \' \', \'\'), \'+\', \'\') LIKE ?');
                params.push(`%${normalizedPhone}%`);
            }
        }
        if (event) {
            where.push('l.`이벤트` = ?');
            params.push(event);
        }
        if (callMin !== undefined && callMin !== '') {
            const min = Number(callMin);
            if (!Number.isNaN(min)) {
                where.push('COALESCE(l.`콜횟수`, 0) >= ?');
                params.push(min);
            }
        }
        if (missMin !== undefined && missMin !== '') {
            const min = Number(missMin);
            if (!Number.isNaN(min)) {
                where.push('COALESCE(l.`부재중_횟수`, 0) >= ?');
                params.push(min);
            }
        }
        if (region) {
            where.push('l.`거주지` LIKE ?');
            params.push(`%${region}%`);
        }
        if (memo) {
            where.push('m.memo_content LIKE ?');
            params.push(`%${memo}%`);
        }
        if (assignedTodayOnly === '1' || assignedTodayOnly === 'true') {
            where.push(`DATE(l.\`배정날짜\`) = DATE(${KST_NOW_SQL})`);
        }

        const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

        const [rows] = await pool.query(`
            SELECT 
                l.*,
                t.name AS tm_name,
                m.memo_time AS 최근메모시간,
                m.memo_content AS 최근메모내용,
                m.tm_id AS 최근메모작성자
            FROM tm_leads l
            LEFT JOIN tm t ON t.id = l.tm
            LEFT JOIN (
                SELECT mm.*
                FROM tm_memos mm
                INNER JOIN (
                    SELECT tm_lead_id, MAX(memo_time) AS max_time
                    FROM tm_memos
                    WHERE tm_lead_id IS NOT NULL
                    GROUP BY tm_lead_id
                ) latest
                ON latest.tm_lead_id = mm.tm_lead_id AND latest.max_time = mm.memo_time
            ) m
            ON m.tm_lead_id = l.id
            ${whereSql}
            ORDER BY l.id DESC
        `, params);

        const workbook = new ExcelJS.Workbook();
        const sheet = workbook.addWorksheet('DB목록');
        const visibleColumns = [
            '인입날짜',
            '배정날짜',
            '이름',
            '연락처',
            '이벤트',
            'tm',
            '상태',
            '최근메모내용',
            '콜_날짜시간',
            '예약_내원일시',
            '거주지',
            '최근메모시간',
            '콜횟수',
        ];
        sheet.columns = visibleColumns.map((key) => ({
            header: key,
            key,
            width: key === '최근메모내용' ? 60 : 18
        }));

        rows.forEach((row) => {
            const formatted = {};
            visibleColumns.forEach((key) => {
                if (key === '연락처') {
                    formatted[key] = formatPhone(row[key]);
                    return;
                }
                if (key === 'tm') {
                    formatted[key] = row.tm_name || row[key] || '';
                    return;
                }
                if (key === '예약_내원일시') {
                    formatted[key] = row[key] ? formatDateTime(row[key]) : '';
                    return;
                }
                if (key === '인입날짜' || key === '배정날짜' || key === '콜_날짜시간' || key === '최근메모시간') {
                    formatted[key] = row[key] ? formatDateTime(row[key]) : '';
                    return;
                }
                formatted[key] = row[key] ?? '';
            });
            sheet.addRow(formatted);
        });

        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', 'attachment; filename=\"db_list.xlsx\"');
        await workbook.xlsx.write(res);
        res.end();
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Export failed' });
    }
});

app.delete('/tm/memos/:id', async (req, res) => {
    const { id } = req.params;
    const { tmId } = req.body || {};
    const actorTmId = getSessionTmId(req);

    if (!actorTmId) {
        return res.status(401).json({ error: 'login required' });
    }
    if (tmId !== undefined && String(tmId) !== String(actorTmId)) {
        return res.status(403).json({ error: 'tmId mismatch' });
    }

    try {
        const [rows] = await pool.query('SELECT * FROM tm_memos WHERE id = ? LIMIT 1', [id]);
        const memo = rows[0];
        if (!memo) {
            return res.status(404).json({ error: 'Memo not found' });
        }
        if (String(memo.tm_id || '') !== String(actorTmId)) {
            return res.status(403).json({ error: 'Only author can delete this memo' });
        }

        await pool.query('DELETE FROM tm_memos WHERE id = ?', [id]);
        await writeTmAuditLog(req, 'TM_MEMO_DELETE', 'tm_memos', id, memo, null);
        return res.json({ ok: true });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed' });
    }
});

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: (origin, callback) => {
            if (!origin) return callback(null, true);
            if (allowedOriginSet.size === 0) return callback(null, true);
            if (allowedOriginSet.has(normalizeOrigin(origin))) return callback(null, true);
            return callback(new Error('Not allowed by CORS'));
        },
        credentials: true,
    },
    transports: ['websocket'],
    allowUpgrades: false,
});

io.use((socket, next) => {
    sessionMiddleware(socket.request, {}, next);
});

io.use((socket, next) => {
    const sessionUser = socket.request?.session?.user;
    const authTmId = Number(socket.handshake?.auth?.tmId || 0);
    if (!sessionUser?.id && !authTmId) {
        return next(new Error('Unauthorized'));
    }
    return next();
});

const getSocketActor = (socket) => {
    const sessionUser = socket.request?.session?.user;
    const authTmId = Number(socket.handshake?.auth?.tmId || 0);
    const senderTmId = Number(sessionUser?.id || 0) || authTmId;
    const senderName = String(
        sessionUser?.username ||
        socket.handshake?.auth?.username ||
        'Unknown'
    );
    const senderRole = socket.request?.session?.isAdmin || socket.handshake?.auth?.isAdmin
        ? 'admin'
        : 'tm';
    return { senderTmId, senderName, senderRole };
};

io.on('connection', (socket) => {
    const actor = getSocketActor(socket);
    if (actor.senderTmId) {
        socket.join('chat:group');
        socket.join(`chat:user:${actor.senderTmId}`);
    }

    socket.on('chat:send', async (payload, ack) => {
        try {
            const currentActor = getSocketActor(socket);
            if (!currentActor.senderTmId) {
                if (typeof ack === 'function') ack({ ok: false, error: 'Unauthorized' });
                return;
            }

            const message = String(payload?.message || '').trim();
            const messageTypeRaw = String(payload?.messageType || 'text').trim().toLowerCase();
            const messageType = messageTypeRaw === 'lead_share' ? 'lead_share' : 'text';
            const sharedLeadId = Number(payload?.sharedLeadId || 0);
            const sharedPayload = payload?.sharedPayload && typeof payload.sharedPayload === 'object'
                ? payload.sharedPayload
                : null;

            if (messageType === 'text' && !message) {
                if (typeof ack === 'function') ack({ ok: false, error: 'message is required' });
                return;
            }
            if (messageType === 'lead_share' && !sharedLeadId) {
                if (typeof ack === 'function') ack({ ok: false, error: 'sharedLeadId is required' });
                return;
            }
            if (message.length > 2000) {
                if (typeof ack === 'function') ack({ ok: false, error: 'message too long' });
                return;
            }

            const targetTmIdRaw = Number(payload?.targetTmId || 0);
            const isGroup = !targetTmIdRaw;
            const targetTmId = isGroup ? null : targetTmIdRaw;
            if (!isGroup && targetTmId <= 0) {
                if (typeof ack === 'function') ack({ ok: false, error: 'targetTmId is invalid' });
                return;
            }

            await ensureChatSchema();
            const [result] = await pool.query(
                `
                INSERT INTO tm_chat_messages (
                    sender_tm_id, target_tm_id, is_group, sender_name, sender_role, message, message_type, shared_lead_id, shared_payload, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ${KST_NOW_SQL})
                `,
                [
                    currentActor.senderTmId,
                    targetTmId,
                    isGroup ? 1 : 0,
                    currentActor.senderName,
                    currentActor.senderRole,
                    message,
                    messageType,
                    sharedLeadId || null,
                    sharedPayload ? JSON.stringify(sharedPayload) : null,
                ]
            );

            const [rows] = await pool.query(
                `
                SELECT
                    id,
                    sender_tm_id,
                    target_tm_id,
                    is_group,
                    sender_name,
                    sender_role,
                    message,
                    message_type,
                    shared_lead_id,
                    shared_payload,
                    created_at
                FROM tm_chat_messages
                WHERE id = ?
                LIMIT 1
                `,
                [result.insertId]
            );
            const saved = rows?.[0];
            if (saved) {
                if (Number(saved.is_group) === 1) {
                    io.to('chat:group').emit('chat:new', saved);
                } else {
                    io.to(`chat:user:${saved.sender_tm_id}`).emit('chat:new', saved);
                    io.to(`chat:user:${saved.target_tm_id}`).emit('chat:new', saved);
                }
            }
            if (typeof ack === 'function') ack({ ok: true, data: saved || null });
        } catch (err) {
            console.error(err);
            if (typeof ack === 'function') ack({ ok: false, error: 'send failed' });
        }
    });
});

server.listen(3000, () => {
    console.log('서버가 3000번 포트에서 실행중입니다.');
});





