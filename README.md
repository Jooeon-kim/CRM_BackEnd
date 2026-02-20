# CRM_BackEnd

샤인유의원 CRM 백엔드 서버입니다.  
TM/관리자 기능 API, 로그인/세션, 마감보고, 캘린더/배정 기능을 제공합니다.

## 기술 스택
- Node.js
- Express
- MySQL (`mysql2`)
- express-session

## 실행 방법
```bash
npm install
npm run dev
```

프로덕션 실행:
```bash
npm start
```

## 환경변수
`.env` 예시

```env
PORT=5000

DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=your_database
DB_PORT=3306

SESSION_SECRET=change-me

# CORS
# 예: https://shineyou-client.vercel.app
CORS_ORIGIN=
COOKIE_SAMESITE=lax

# 관리자 보조 쿠키 이름(선택)
ADMIN_COOKIE_NAME=admin_token
```

참고:
- Railway 사용 시 `MYSQLHOST`, `MYSQLUSER`, `MYSQLPASSWORD`, `MYSQLDATABASE`, `MYSQLPORT`도 자동 인식합니다.

## 인증/세션
- 로그인: `POST /auth/login`
- 로그아웃: `POST /auth/logout`
- 관리자 프로필 조회/수정:
  - `GET /auth/admin/profile`
  - `POST /auth/admin/profile`

## TM 주요 API 메뉴얼

### DB/배정
- `GET /tm/leads` : TM 목록 조회(필터 포함)
- `POST /tm/assign` : TM 배정
- `GET /tm/agents` : TM 계정 목록
- `POST /tm/agents` : TM 계정 생성

### 메모/상담 업데이트
- `GET /tm/memos` : 전화번호/리드 기준 메모 조회
- `POST /tm/leads/:id/update` : 상태, 메모, 예약일시, 리콜일시 등 업데이트

### 리콜대기
- `GET /tm/recalls` : 리콜대기 목록 조회
  - `mode=all|due|upcoming`

### 마감보고
- `POST /tm/reports/draft` : 임시저장
- `POST /tm/reports/submit` : 최종제출
- `GET /tm/reports/mine` : 내 마감보고 목록
- `GET /tm/reports/draft` : 특정 날짜 draft 조회
- `GET /tm/reports/:reportId/full` : 마감보고 상세(리드 목록 포함)

## 관리자 주요 API 메뉴얼

### DB 조회/수정
- `GET /dbdata` : 전체 DB 조회(검색/필터)
- `POST /admin/leads/:id/update` : 관리자 수정
- `POST /admin/leads/reassign-bulk` : 일괄 TM 변경

### 마감보고
- `GET /admin/reports/daily` : 날짜별 TM 마감보고 목록
- `GET /admin/reports/:reportId/full` : 마감보고 상세(부재중/리콜대기/실패/예약/내원 등)
- `GET /admin/reports/:reportId/leads` : metric 단건 상세 조회

### 메타 동기화/규칙
- `POST /admin/sync-meta-leads` : meta_leads → tm_leads 동기화
- `GET /admin/event-rules` : 이벤트 규칙 조회
- `POST /admin/event-rules` : 이벤트 규칙 추가
- `DELETE /admin/event-rules/:id` : 이벤트 규칙 삭제

## Export API
- `GET /tm/leads/export` : TM 기준 엑셀 다운로드
- `GET /dbdata/export` : 관리자 기준 엑셀 다운로드

## 운영 메모
- 시간 저장은 DB/서버 설정에 따라 UTC 기준으로 저장될 수 있습니다.
- 프론트 표시 시간은 화면 로직에서 KST 기준 보정해 표시합니다.
- CORS 문제 발생 시 `CORS_ORIGIN`, `COOKIE_SAMESITE`, 프론트 도메인을 우선 점검하세요.
