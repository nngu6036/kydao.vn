# Chess ELO Monorepo Scaffold

This scaffold contains:

- `apps/api` - FastAPI backend
- `apps/admin-web` - Angular Material admin app
- `apps/content-web` - Angular Material public content app
- `packages/shared-ui` - reusable UI and content sections library scaffold
- `packages/shared-types` - reusable TypeScript types library scaffold

## Intended architecture

```text
chess-elo/
  apps/
    api/
    admin-web/
    content-web/
  packages/
    shared-ui/
    shared-types/
```

## Notes

This is a scaffold intended to show the monorepo structure and how shared libraries fit in.
The `shared-ui` and `shared-types` packages are included as reusable libraries for both apps.

## Backend

Start the API and MongoDB with Docker Compose:

```bash
cp .env.example .env
docker compose -f deploy/docker-compose.yml up --build api
```

Or start the API locally with Uvicorn:

```bash
cd apps/api
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

The API runs at `http://localhost:8000`. Check it with `http://localhost:8000/health`.

### Local Celery

Celery uses Redis as the local broker and result backend. Start Redis first:

```bash
docker run --rm --name chess-elo-redis -p 6379:6379 redis:7-alpine
```

In a second terminal, start the Celery worker:

```bash
cd apps/api
python -m pip install -r requirements.txt
python -m celery -A app.celery_app:celery_app worker --loglevel=info
```

In a third terminal, start Celery Beat so the midnight schedule is active:

```bash
cd apps/api
python -m celery -A app.celery_app:celery_app beat --loglevel=info
```

The default local Celery settings are:

```bash
CHESS_ELO_CELERY_BROKER_URL=redis://localhost:6379/0
CHESS_ELO_CELERY_RESULT_BACKEND=redis://localhost:6379/1
CHESS_ELO_CELERY_TIMEZONE=Australia/Sydney
```

## Frontends
```bash
cd apps/admin-web
npm install
npm start
```

```bash
cd apps/content-web
npm install
npm start
```
