const express = require('express');
const session = require('express-session');
const cors = require('cors');
require('dotenv').config();
const pool = require('./db');

const app = express();
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
app.use(session({
    name: 'sid',
    secret: process.env.SESSION_SECRET || 'change-me',
    resave: false,
    saveUninitialized: false,
    cookie: {
        httpOnly: true,
        sameSite: process.env.COOKIE_SAMESITE || (process.env.CORS_ORIGIN ? 'none' : 'lax'),
        secure: process.env.NODE_ENV === 'production' || process.env.COOKIE_SAMESITE === 'none',
        maxAge: 1000 * 60 * 60
    }
}));

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

app.listen(3000, () => {
    console.log('서버가 3000번 포트에서 실행중입니다.');
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
            // `배정날짜` is stored in UTC. Compare by KST calendar date.
            where.push(`DATE(DATE_ADD(l.\`${map.assignedDate}\`, INTERVAL 9 HOUR)) = DATE(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 9 HOUR))`);
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

const resolveTmId = (req) => {
    return req.session?.user?.id || req.body?.tmId || req.query?.tmId || null;
};

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

    const [nextdayReservedRows] = await conn.query(
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
          AND TRIM(COALESCE(l.\`상태\`, '')) = '예약'
          AND DATE(l.\`예약_내원일시\`) = ?
        ORDER BY l.id DESC
        `,
        [String(tmId), nextDay]
    );

    const statusText = (row) => String(row.status || '').trim();
    const statusEq = (row, value) => statusText(row) === value;
    const statusIncludes = (row, value) => statusText(row).includes(value);
    const missed = callRows.filter((row) => statusEq(row, '부재중'));
    const failed = callRows.filter((row) => statusEq(row, '실패'));
    const reserved = callRows.filter((row) => statusEq(row, '예약'));
    const visitTodayReserved = reserved.filter((row) => toDateKey(row.reservation_at) === reportDate);
    const visitTodayCompleted = callRows.filter(
        (row) => statusIncludes(row, '내원완료') && toDateKey(row.reservation_at) === reportDate
    );
    const visitToday = [...visitTodayReserved, ...visitTodayCompleted];
    const visitNextdayByCall = reserved.filter((row) => toDateKey(row.reservation_at) === nextDay);
    const visitNextdayMap = new Map();
    [...visitNextdayByCall, ...(nextdayReservedRows || [])].forEach((row) => {
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            total_call_count = VALUES(total_call_count),
            missed_count = VALUES(missed_count),
            failed_count = VALUES(failed_count),
            reserved_count = VALUES(reserved_count),
            visit_today_count = VALUES(visit_today_count),
            visit_nextday_count = VALUES(visit_nextday_count),
            updated_at = NOW()
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

        res.json({ columns: map, leads });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
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
    const { name, phone, password } = req.body || {};
    if (!name || !phone || !password) {
        return res.status(400).json({ error: 'name, phone, password are required' });
    }
    try {
        const [result] = await pool.query(
            'INSERT INTO tm (name, phone, password, isAdmin) VALUES (?, ?, ?, 0)',
            [name, phone, password]
        );
        res.json({ ok: true, id: result.insertId });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.patch('/tm/agents/:id', async (req, res) => {
    const { id } = req.params;
    const { name, phone, password } = req.body || {};
    if (!name || !phone) {
        return res.status(400).json({ error: 'name and phone are required' });
    }
    try {
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
        res.json({ ok: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.post('/tm/assign', async (req, res) => {
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
            ? `\`${assignCol}\` = ?, \`${assignedAtCol}\` = NOW()`
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
    const { phone, detailed, leadId } = req.query || {};
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

        const [rows] = await pool.query(
            `
            SELECT
                m.id,
                m.memo_time,
                m.memo_content,
                m.tm_id,
                t.name AS tm_name
            FROM tm_memos m
            LEFT JOIN tm t ON t.id = m.tm_id
            WHERE ${normalizePhoneSql('m.target_phone')} = ?
            ORDER BY m.memo_time DESC
            `,
            [normalizedPhone]
        );

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
    const sessionTmId = req.session?.user?.id;
    const editorTmId = tmId || sessionTmId;

    if (!editorTmId) {
        return res.status(400).json({ error: 'tmId is required' });
    }
    if (!memoContent || !String(memoContent).trim()) {
        return res.status(400).json({ error: 'memoContent is required' });
    }

    try {
        const [rows] = await pool.query(
            'SELECT id, tm_id FROM tm_memos WHERE id = ? LIMIT 1',
            [id]
        );
        const memo = rows[0];
        if (!memo) {
            return res.status(404).json({ error: 'Memo not found' });
        }
        if (String(memo.tm_id || '') !== String(editorTmId)) {
            return res.status(403).json({ error: 'Only author can edit this memo' });
        }

        await pool.query(
            'UPDATE tm_memos SET memo_content = ? WHERE id = ?',
            [String(memoContent).trim(), id]
        );

        return res.json({ ok: true });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'DB query failed' });
    }
});

app.post('/tm/leads/:id/update', async (req, res) => {
    const { id } = req.params;
    const { status, region, memo, tmId, reservationAt, name, recallAt } = req.body || {};
    if (!tmId) {
        return res.status(400).json({ error: 'tmId is required' });
    }
    if (status === undefined && region === undefined && !memo && reservationAt === undefined && name === undefined && recallAt === undefined) {
        return res.status(400).json({ error: 'no changes provided' });
    }

    try {
        await ensureRecallColumns();
        const [rows] = await pool.query('SELECT 상태 FROM tm_leads WHERE id = ?', [id]);
        const currentStatus = rows[0]?.상태 ?? null;
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
            updates.push('콜_날짜시간 = NOW()');
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
        if (shouldUpdateReservationAt) {
            updates.push('예약_내원일시 = ?');
            params.push(reservationAt);
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

        if (memo && String(memo).trim().length > 0) {
            await pool.query(
                'INSERT INTO tm_memos (memo_content, target_phone, tm_id, tm_lead_id) VALUES (?, ?, ?, ?)',
                [memo, req.body.phone || '', tmId, id]
            );
        }

        res.json({ ok: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.get('/tm/recalls', async (req, res) => {
    const tmId = req.session?.user?.id || req.query?.tmId;
    const mode = String(req.query?.mode || 'all').toLowerCase();
    if (!tmId) {
        return res.status(401).json({ error: 'login required or tmId required' });
    }
    if (!['all', 'due', 'upcoming'].includes(mode)) {
        return res.status(400).json({ error: 'mode must be all, due, or upcoming' });
    }
    try {
        await ensureRecallColumns();
        const where = ['tm = ?', "TRIM(COALESCE(`상태`, '')) = '리콜대기'", '`리콜_예정일시` IS NOT NULL'];
        const params = [String(tmId)];
        if (mode === 'due') where.push('`리콜_예정일시` <= NOW()');
        if (mode === 'upcoming') where.push('`리콜_예정일시` > NOW()');
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
    const sessionTmId = req.session?.user?.id;
    const targetTmId = tmId || sessionTmId;
    const targetDate = normalizeReportDate(reportDate);

    if (!targetTmId) {
        return res.status(400).json({ error: 'tmId is required' });
    }
    if (!targetDate) {
        return res.status(400).json({ error: 'reportDate must be YYYY-MM-DD' });
    }

    const normalizedTmId = String(targetTmId);
    const nextDay = nextDateKey(targetDate);

    const conn = await pool.getConnection();
    try {
        await ensureReportSchema();
        await conn.beginTransaction();

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
            [normalizedTmId, targetDate]
        );

        const [nextdayReservedRows] = await conn.query(
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
              AND TRIM(COALESCE(l.\`상태\`, '')) = '예약'
              AND DATE(l.\`예약_내원일시\`) = ?
            ORDER BY l.id DESC
            `,
            [normalizedTmId, nextDay]
        );

        const statusText = (row) => String(row.status || '').trim();
        const statusEq = (row, value) => statusText(row) === value;
        const statusIncludes = (row, value) => statusText(row).includes(value);
        const missed = callRows.filter((row) => statusEq(row, '부재중'));
        const failed = callRows.filter((row) => statusEq(row, '실패'));
        const reserved = callRows.filter((row) => statusEq(row, '예약'));
        const visitTodayReserved = reserved.filter((row) => toDateKey(row.reservation_at) === targetDate);
        const visitTodayCompleted = callRows.filter(
            (row) => statusIncludes(row, '내원완료') && toDateKey(row.reservation_at) === targetDate
        );
        const visitToday = [...visitTodayReserved, ...visitTodayCompleted];
        const visitNextdayByCall = reserved.filter((row) => toDateKey(row.reservation_at) === nextDay);
        const visitNextdayMap = new Map();
        [...visitNextdayByCall, ...(nextdayReservedRows || [])].forEach((row) => {
            if (!row?.id) return;
            if (!visitNextdayMap.has(row.id)) visitNextdayMap.set(row.id, row);
        });
        const visitNextday = Array.from(visitNextdayMap.values());
        const totalCallCount = callRows.reduce((sum, row) => {
            const n = Number(row.call_count);
            return sum + (Number.isNaN(n) ? 0 : Math.max(0, Math.floor(n)));
        }, 0);

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
                submitted_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                total_call_count = VALUES(total_call_count),
                missed_count = VALUES(missed_count),
                failed_count = VALUES(failed_count),
                reserved_count = VALUES(reserved_count),
                visit_today_count = VALUES(visit_today_count),
                visit_nextday_count = VALUES(visit_nextday_count),
                submitted_at = NOW(),
                updated_at = NOW()
            `,
            [
                normalizedTmId,
                targetDate,
                totalCallCount,
                missed.length,
                failed.length,
                reserved.length,
                visitToday.length,
                visitNextday.length
            ]
        );

        const [reportRows] = await conn.query(
            'SELECT id FROM tm_daily_report WHERE tm_id = ? AND report_date = ? LIMIT 1',
            [normalizedTmId, targetDate]
        );
        const reportId = reportRows[0]?.id;

        if (!reportId) {
            throw new Error('Failed to load report id after upsert');
        }

        await conn.query('DELETE FROM tm_daily_report_leads WHERE report_id = ?', [reportId]);

        const pushDetail = (metricType, rows, bucket) => {
            rows.forEach((row) => {
                bucket.push([
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

        const detailValues = [];
        pushDetail('MISSED', missed, detailValues);
        pushDetail('FAILED', failed, detailValues);
        pushDetail('RESERVED', reserved, detailValues);
        pushDetail('VISIT_TODAY', visitToday, detailValues);
        pushDetail('VISIT_NEXTDAY', visitNextday, detailValues);

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
        }

        await conn.commit();

        return res.json({
            ok: true,
            reportId,
            reportDate: targetDate,
            summary: {
                totalCallCount,
                missedCount: missed.length,
                failedCount: failed.length,
                reservedCount: reserved.length,
                visitTodayCount: visitToday.length,
                visitNextdayCount: visitNextday.length,
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
    if (!tmId) return res.status(401).json({ error: 'login required or tmId required' });

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
    if (!tmId) return res.status(401).json({ error: 'login required or tmId required' });
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
    if (!tmId) return res.status(401).json({ error: 'login required or tmId required' });
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
                updated_at = NOW()
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
    if (!tmId) return res.status(401).json({ error: 'login required or tmId required' });
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
    if (!tmId) return res.status(401).json({ error: 'login required or tmId required' });
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
                submitted_at = NOW(),
                updated_at = NOW()
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
    if (!tmId) return res.status(401).json({ error: 'login required or tmId required' });
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
                    row.created_time || null,
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
                NOW(),
                ?, ?, ?, ?,
                ${hasTm ? 'NOW()' : 'NULL'}
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
    const { status, region, memo, tmId, reservationAt, name } = req.body || {};
    if (!status && region === undefined && !memo && tmId === undefined && reservationAt === undefined && name === undefined) {
        return res.status(400).json({ error: 'no changes provided' });
    }

    try {
        await ensureLeadAssignedDateColumn();
        let currentStatus = null;
        let currentTm = null;
        if (status !== undefined) {
            const [rows] = await pool.query('SELECT 상태 FROM tm_leads WHERE id = ?', [id]);
            currentStatus = rows[0]?.상태 ?? null;
        }
        if (tmId !== undefined) {
            const [rows] = await pool.query('SELECT tm FROM tm_leads WHERE id = ?', [id]);
            currentTm = rows[0]?.tm ?? null;
        }
        const statusProvided = status !== undefined;
        const statusChanged = statusProvided && status !== currentStatus;

        const updates = [];
        const params = [];

        const shouldUpdateReservationAt =
            reservationAt !== undefined &&
            (reservationAt !== null && String(reservationAt).trim() !== '');

        if (statusChanged) {
            updates.push('상태 = ?');
            params.push(status);
            if (shouldUpdateReservationAt) {
                updates.push('예약_내원일시 = ?');
                params.push(reservationAt);
            }
        } else if (shouldUpdateReservationAt) {
            updates.push('예약_내원일시 = ?');
            params.push(reservationAt);
        }

        const callStatuses = ['부재중', '리콜대기', '예약', '실패'];
        const isMissed = status === '부재중';
        const isNoShow = status === '예약부도';
        const shouldApplyCallMetrics = statusChanged || isMissed;
        const incCall = shouldApplyCallMetrics && callStatuses.includes(status);
        if (shouldApplyCallMetrics) {
            updates.push('콜_날짜시간 = NOW()');
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

        if (tmId !== undefined) {
            updates.push('tm = ?');
            params.push(tmId || null);
            if (tmId && String(tmId) !== String(currentTm || '')) {
                updates.push('배정날짜 = NOW()');
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

        if (memo && String(memo).trim().length > 0) {
            await pool.query(
                'INSERT INTO tm_memos (memo_content, target_phone, tm_id, tm_lead_id) VALUES (?, ?, ?, ?)',
                [memo, req.body.phone || '', tmId || 0, id]
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
        const [result] = await pool.query(
            `UPDATE tm_leads
             SET \`${assignCol}\` = ?, \`${assignedAtCol}\` = NOW()
             WHERE \`${idCol}\` IN (${placeholders})`,
            [tmId, ...normalizedLeadIds]
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
        res.json({ ok: true, id: result.insertId });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Create failed' });
    }
});

app.delete('/admin/event-rules/:id', async (req, res) => {
    const { id } = req.params;
    try {
        const [result] = await pool.query('DELETE FROM event_rules WHERE id = ?', [id]);
        if (result.affectedRows === 0) {
            return res.status(404).json({ error: 'Rule not found' });
        }
        res.json({ ok: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Delete failed' });
    }
});

app.get('/tm/leads/export', async (req, res) => {
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
        const { tm, status, callMin, missMin, region, memo } = req.query || {};
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
        sheet.columns = visibleColumns.map((key) => ({ header: key, key, width: 18 }));

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
                if (key === '인입날짜' || key === '콜_날짜시간' || key === '예약_내원일시' || key === '최근메모시간') {
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




