This package adapts the uploaded React mockup into the Angular `content-web` project.

What changed:
- Replaced the previous portal-style homepage with a focused mockup-style landing page
- Added Angular standalone components matching the mockup sections
- Removed unresolved `@chess-elo/shared-types` references so the project is self-contained

Notes:
- The header banner uses the remote image URL found in the mockup CSS
- Search/detail pages are simplified to match the new visual system
- Existing theme assets remain in `src/assets/theme` if needed for later pages
