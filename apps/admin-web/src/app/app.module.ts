import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { NgbPaginationModule } from '@ng-bootstrap/ng-bootstrap';
import { XiangqiBoardModule } from '@chess-elo/shared-ui/xiangqi-board';
import { AppComponent } from './app.component';
import { routes } from './app.routes';
import { authInterceptor } from './core/auth.interceptor';
import { DashboardPage } from './pages/dashboard.page';
import { GameEditPage } from './pages/game-edit.page';
import { GameListPage } from './pages/game-list.page';
import { LoginPage } from './pages/login.page';
import { OpeningEditPage } from './pages/opening-edit.page';
import { OpeningListPage } from './pages/opening-list.page';
import { PlayerEditPage } from './pages/player-edit.page';
import { PlayerListPage } from './pages/player-list.page';
import { TournamentEditPage } from './pages/tournament-edit.page';
import { TournamentListPage } from './pages/tournament-list.page';

@NgModule({
  declarations: [
    AppComponent,
    DashboardPage,
    GameEditPage,
    GameListPage,
    LoginPage,
    OpeningEditPage,
    OpeningListPage,
    PlayerEditPage,
    PlayerListPage,
    TournamentEditPage,
    TournamentListPage,
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    ReactiveFormsModule,
    NgbPaginationModule,
    RouterModule.forRoot(routes),
    XiangqiBoardModule,
  ],
  providers: [provideHttpClient(withInterceptors([authInterceptor]))],
  bootstrap: [AppComponent],
})
export class AppModule {}
