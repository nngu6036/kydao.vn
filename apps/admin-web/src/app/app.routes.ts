import { Routes } from '@angular/router';
import { authGuard } from './core/auth.guard';
import { DashboardPage } from './pages/dashboard.page';
import { GameEditPage } from './pages/game-edit.page';
import { GameListPage } from './pages/game-list.page';
import { LoginPage } from './pages/login.page';
import { PlayerEditPage } from './pages/player-edit.page';
import { PlayerListPage } from './pages/player-list.page';
import { TournamentEditPage } from './pages/tournament-edit.page';
import { TournamentListPage } from './pages/tournament-list.page';

export const routes: Routes = [
  { path: 'login', component: LoginPage },
  { path: 'dashboard', component: DashboardPage, canActivate: [authGuard] },
  { path: 'tournaments', component: TournamentListPage, canActivate: [authGuard] },
  { path: 'tournaments/new', component: TournamentEditPage, canActivate: [authGuard] },
  { path: 'tournaments/:id/edit', component: TournamentEditPage, canActivate: [authGuard] },
  { path: 'games', component: GameListPage, canActivate: [authGuard] },
  { path: 'games/new', component: GameEditPage, canActivate: [authGuard] },
  { path: 'games/:id/edit', component: GameEditPage, canActivate: [authGuard] },
  { path: 'players', component: PlayerListPage, canActivate: [authGuard] },
  { path: 'players/new', component: PlayerEditPage, canActivate: [authGuard] },
  { path: 'players/:id/edit', component: PlayerEditPage, canActivate: [authGuard] },
  { path: '', pathMatch: 'full', redirectTo: 'dashboard' },
  { path: '**', redirectTo: 'dashboard' },
];
