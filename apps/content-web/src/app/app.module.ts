import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { XiangqiBoardModule } from '@chess-elo/shared-ui/xiangqi-board';
import { AppComponent } from './app.component';
import { routes } from './app.routes';
import { FooterComponent } from './components/footer.component';
import { GameBlockComponent } from './components/game-block.component';
import { HeaderComponent } from './components/header.component';
import { OpeningBlockComponent } from './components/opening-block.component';
import { PlayerBlockComponent } from './components/player-block.component';
import { RankingBlockComponent } from './components/ranking-block.component';
import { SearchCenterComponent } from './components/search-center.component';
import { TournamentBlockComponent } from './components/tournament-block.component';
import { authInterceptor } from './core/auth.interceptor';
import { DetailPage } from './pages/detail.page';
import { GameDetailPage } from './pages/game-detail.page';
import { HomePage } from './pages/home.page';
import { ListPage } from './pages/list.page';
import { OpeningDetailPage } from './pages/opening-detail.page';
import { PlayerDetailPage } from './pages/player-detail.page';
import { SearchPage } from './pages/search.page';
import { TournamentDetailPage } from './pages/tournament-detail.page';

@NgModule({
  declarations: [
    AppComponent,
    DetailPage,
    FooterComponent,
    GameBlockComponent,
    GameDetailPage,
    HeaderComponent,
    HomePage,
    ListPage,
    OpeningBlockComponent,
    OpeningDetailPage,
    PlayerBlockComponent,
    PlayerDetailPage,
    RankingBlockComponent,
    SearchCenterComponent,
    SearchPage,
    TournamentBlockComponent,
    TournamentDetailPage,
  ],
  imports: [BrowserModule, BrowserAnimationsModule, FormsModule, RouterModule.forRoot(routes), XiangqiBoardModule],
  providers: [provideHttpClient(withInterceptors([authInterceptor]))],
  bootstrap: [AppComponent],
})
export class AppModule {}
