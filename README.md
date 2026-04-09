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
```bash
cp .env.example .env
docker compose up --build
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
