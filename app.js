const express = require('express');
const session = require('express-session');
const cors = require('cors');
require('dotenv').config();
const pool = require('./db');

const app = express();
const allowedOrigins = (process.env.CORS_ORIGIN || '')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);

app.use(cors({
    origin: (origin, callback) => {
        if (!origin) return callback(null, true);
        if (allowedOrigins.length === 0) return callback(null, true);
        if (allowedOrigins.includes(origin)) return callback(null, true);
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
            where.push(`DATE(l.\`${map.assignedDate}\`) = CURDATE()`);
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
                return val === null || val === undefined || val === '' || Number(val) === 0;
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
    if (!leadId || !tmId) {
        return res.status(400).json({ error: 'leadId and tmId are required' });
    }

    try {
        const columns = await describeTable('tm_leads');
        const idCol = pickColumn(columns, ['id', 'lead_id', 'tm_lead_id']);
        const assignCol = pickColumn(columns, ['tm_id', 'tmid', 'assigned_tm_id', 'assigned_tm', 'tm']);
        const assignedAtCol = pickColumn(columns, ['배정날짜', 'assigned_at', 'assigned_date', 'tm_assigned_at']);

        if (!idCol || !assignCol) {
            return res.status(500).json({ error: 'tm_leads columns not found' });
        }

        const setSql = assignedAtCol
            ? `${assignCol} = ?, ${assignedAtCol} = NOW()`
            : `${assignCol} = ?`;
        const [result] = await pool.query(
            `UPDATE tm_leads SET ${setSql} WHERE ${idCol} = ? AND (${assignCol} IS NULL OR ${assignCol} = 0 OR ${assignCol} = '')`,
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
    const { phone } = req.query || {};
    if (!phone) {
        return res.status(400).json({ error: 'phone is required' });
    }
    try {
        const [rows] = await pool.query(
            'SELECT memo_time, memo_content, tm_id FROM tm_memos WHERE target_phone = ? ORDER BY memo_time DESC',
            [phone]
        );
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'DB query failed' });
    }
});

app.post('/tm/leads/:id/update', async (req, res) => {
    const { id } = req.params;
    const { status, region, memo, tmId, reservationAt } = req.body || {};
    if (!status || !tmId) {
        return res.status(400).json({ error: 'status and tmId are required' });
    }

    const callStatuses = ['부재중', '리콜대기', '예약'];
    const isMissed = status === '부재중';
    const isNoShow = status === '예약부도';
    const incCall = callStatuses.includes(status) || isNoShow;

    try {
        const [result] = await pool.query(
            `UPDATE tm_leads
             SET
                상태 = ?,
                거주지 = ?,
                콜_날짜시간 = NOW(),
                예약_내원일시 = ?,
                콜횟수 = COALESCE(콜횟수, 0) + ?,
                부재중_횟수 = COALESCE(부재중_횟수, 0) + ?,
                예약부도_횟수 = COALESCE(예약부도_횟수, 0) + ?
             WHERE id = ?`,
            [
                status,
                region || null,
                reservationAt || null,
                incCall ? 1 : 0,
                isMissed ? 1 : 0,
                isNoShow ? 1 : 0,
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

app.post('/admin/leads/:id/update', async (req, res) => {
    const { id } = req.params;
    const { status, region, memo, tmId, reservationAt } = req.body || {};
    if (!status && region === undefined && !memo && tmId === undefined && reservationAt === undefined) {
        return res.status(400).json({ error: 'no changes provided' });
    }

    try {
        let currentStatus = null;
        let currentTm = null;
        if (status) {
            const [rows] = await pool.query('SELECT 상태 FROM tm_leads WHERE id = ?', [id]);
            currentStatus = rows[0]?.상태 ?? null;
        }
        if (tmId !== undefined) {
            const [rows] = await pool.query('SELECT tm FROM tm_leads WHERE id = ?', [id]);
            currentTm = rows[0]?.tm ?? null;
        }
        const statusChanged = status && status !== currentStatus;

        const updates = [];
        const params = [];

        if (statusChanged) {
            updates.push('상태 = ?');
            params.push(status);
            updates.push('콜_날짜시간 = NOW()');
            updates.push('예약_내원일시 = ?');
            params.push(reservationAt || null);

            const callStatuses = ['부재중', '리콜대기', '예약'];
            const isMissed = status === '부재중';
            const isNoShow = status === '예약부도';
            const incCall = callStatuses.includes(status) || isNoShow;
            updates.push('콜횟수 = COALESCE(콜횟수, 0) + ?');
            params.push(incCall ? 1 : 0);
            updates.push('부재중_횟수 = COALESCE(부재중_횟수, 0) + ?');
            params.push(isMissed ? 1 : 0);
            updates.push('예약부도_횟수 = COALESCE(예약부도_횟수, 0) + ?');
            params.push(isNoShow ? 1 : 0);
        } else if (reservationAt !== undefined) {
            updates.push('예약_내원일시 = ?');
            params.push(reservationAt || null);
        }

        if (region !== undefined) {
            updates.push('거주지 = ?');
            params.push(region || null);
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
                return val === null || val === undefined || val === '' || Number(val) === 0;
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




