import { Routes } from '@angular/router';
import { HomePage } from './pages/home.page';
import { SearchPage } from './pages/search.page';
import { ListPage } from './pages/list.page';
import { PlayerDetailPage } from './pages/player-detail.page';
import { GameDetailPage } from './pages/game-detail.page';
import { TournamentDetailPage } from './pages/tournament-detail.page';
import { OpeningDetailPage } from './pages/opening-detail.page';

export const routes: Routes = [
  { path: '', component: HomePage },
  { path: 'search', component: SearchPage },
  { path: 'tournaments', component: ListPage, data: { kind: 'tournaments', title: 'Danh Sách Giải Đấu' } },
  { path: 'players', component: ListPage, data: { kind: 'players', title: 'Danh Sách Kỳ Thủ' } },
  { path: 'games', component: ListPage, data: { kind: 'games', title: 'Danh Sách Ván Đấu' } },
  { path: 'openings', component: ListPage, data: { kind: 'openings', title: 'Thư Viện Khai Cuộc' } },
  { path: 'rankings', component: ListPage, data: { kind: 'rankings', title: 'Bảng Xếp Hạng' } },
  { path: 'players/:id', component: PlayerDetailPage },
  { path: 'games/:id', component: GameDetailPage },
  { path: 'tournaments/:id', component: TournamentDetailPage },
  { path: 'openings/:id', component: OpeningDetailPage },
  { path: '**', redirectTo: '' },
];
