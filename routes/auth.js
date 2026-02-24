const express = require('express');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const pool = require('../db');

const router = express.Router();

const ADMIN_COOKIE_NAME = process.env.ADMIN_COOKIE_NAME || 'admin_token';

const wantsJson = (req) => {
    const accept = req.headers.accept || '';
    return req.xhr || accept.includes('application/json');
};

const parseCookies = (cookieHeader) => {
    if (!cookieHeader) return {};
    return cookieHeader.split(';').reduce((acc, part) => {
        const [k, v] = part.trim().split('=');
        if (!k) return acc;
        acc[k] = decodeURIComponent(v || '');
        return acc;
    }, {});
};

const requireAdmin = (req, res, next) => {
    if (!req.session || !req.session.user || !req.session.isAdmin) {
        return res.status(401).send('로그인이 필요합니다.');
    }

    const cookies = parseCookies(req.headers.cookie || '');
    const cookieVal = cookies[ADMIN_COOKIE_NAME];

    if (!cookieVal || cookieVal !== req.session.adminCookie) {
        return res.status(403).send('관리자 쿠키가 필요합니다.');
    }

    return next();
};

router.get('/login', (req, res) => {
    res.send(`
        <form method="POST" action="/auth/login">
            <input name="username" placeholder="username" required />
            <input name="password" type="password" placeholder="password" required />
            <button type="submit">로그인</button>
        </form>
    `);
});

router.post('/login', async (req, res) => {
    const { username, password } = req.body;

    if (!username || !password) {
        if (wantsJson(req)) {
            return res.status(400).json({ message: 'Username and password are required.' });
        }
        return res.status(400).send('Username and password are required.');
    }

    try {
        const [rows] = await pool.query(
            'SELECT id, name, password, isAdmin FROM tm WHERE name = ? LIMIT 1',
            [username]
        );

        if (rows.length === 0) {
            if (wantsJson(req)) {
                return res.status(401).json({ message: 'Login failed.' });
            }
            return res.status(401).send('Login failed.');
        }

        const user = rows[0];
        const stored = String(user.password || '');
        let match = false;
        if (stored.startsWith('$2')) {
            match = await bcrypt.compare(password, stored);
        } else {
            match = stored === password;
        }

        if (!match) {
            if (wantsJson(req)) {
                return res.status(401).json({ message: 'Login failed.' });
            }
            return res.status(401).send('Login failed.');
        }

        req.session.user = { id: user.id, username: user.name };
        // mysql tinyint(1) may be returned as number/boolean/string by env/driver settings.
        req.session.isAdmin = Number(user.isAdmin) === 1;

        try {
            await pool.query('UPDATE tm SET last_login_at = NOW() WHERE id = ?', [user.id]);
        } catch (updateErr) {
            console.error(updateErr);
        }

        if (req.session.isAdmin) {
            const token = crypto.randomBytes(16).toString('hex');
            req.session.adminCookie = token;
            res.cookie(ADMIN_COOKIE_NAME, token, {
                httpOnly: true,
                sameSite: 'lax',
                secure: process.env.NODE_ENV === 'production',
                maxAge: 1000 * 60 * 60
            });
        }

        if (!stored.startsWith('$2')) {
            try {
                const hashed = await bcrypt.hash(password, 10);
                await pool.query('UPDATE tm SET password = ? WHERE id = ?', [hashed, user.id]);
            } catch (updateErr) {
                console.error(updateErr);
            }
        }

        if (wantsJson(req)) {
            return res.json({
                ok: true,
                id: user.id,
                username: user.name,
                isAdmin: req.session.isAdmin,
            });
        }
        return res.redirect('/auth/secure');
    } catch (err) {
        console.error(err);
        if (wantsJson(req)) {
            return res.status(500).json({ message: 'Server error.' });
        }
        return res.status(500).send('Server error.');
    }
});

router.post('/logout', (req, res) => {
    const cookieName = ADMIN_COOKIE_NAME;
    req.session.destroy(() => {
        res.clearCookie('sid');
        res.clearCookie(cookieName);
        res.send('로그아웃 완료');
    });
});

router.get('/secure', requireAdmin, (req, res) => {
    res.send(`보안 페이지 접근 성공. 사용자 ${req.session.user.username}`);
});

router.get('/admin/profile', requireAdmin, async (req, res) => {
    try {
        const userId = req.session.user?.id;
        if (!userId) {
            return res.status(401).json({ message: '로그인이 필요합니다.' });
        }
        const [rows] = await pool.query(
            'SELECT id, name, phone FROM tm WHERE id = ? AND isAdmin = 1 LIMIT 1',
            [userId]
        );
        if (rows.length === 0) {
            return res.status(404).json({ message: '관리자 정보를 찾을 수 없습니다.' });
        }
        return res.json({
            id: rows[0].id,
            username: rows[0].name,
            phone: rows[0].phone || '',
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ message: 'Server error.' });
    }
});

router.post('/admin/profile', requireAdmin, async (req, res) => {
    const { name, phone, password } = req.body || {};

    if (!name || !phone) {
        return res.status(400).json({ message: 'name and phone are required.' });
    }

    try {
        const userId = req.session.user?.id;
        if (!userId) {
            return res.status(401).json({ message: '로그인이 필요합니다.' });
        }

        const updates = ['name = ?', 'phone = ?'];
        const params = [name, phone];

        if (password && String(password).trim().length > 0) {
            const hashed = await bcrypt.hash(String(password), 10);
            updates.push('password = ?');
            params.push(hashed);
        }

        params.push(userId);
        const [result] = await pool.query(
            `UPDATE tm SET ${updates.join(', ')} WHERE id = ? AND isAdmin = 1`,
            params
        );

        if (result.affectedRows === 0) {
            return res.status(404).json({ message: '관리자 정보를 찾을 수 없습니다.' });
        }

        req.session.user = { ...req.session.user, username: name };

        return res.json({
            ok: true,
            id: userId,
            username: name,
            phone,
            isAdmin: true,
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ message: 'Server error.' });
    }
});

module.exports = router;


