import { Component } from '@angular/core';
import { HeaderComponent } from '../components/header.component';
import { SearchCenterComponent } from '../components/search-center.component';
import { TournamentBlockComponent } from '../components/tournament-block.component';
import { PlayerBlockComponent } from '../components/player-block.component';
import { GameBlockComponent } from '../components/game-block.component';
import { OpeningBlockComponent } from '../components/opening-block.component';
import { RankingBlockComponent } from '../components/ranking-block.component';
import { FooterComponent } from '../components/footer.component';

@Component({
  template: `
    <div class="homepage">
      <app-header></app-header>
      <app-search-center></app-search-center>
      <div class="content-row content-row-featured">
        <app-tournament-block></app-tournament-block>
         <app-game-block></app-game-block>
      </div>

    
      <div class="content-row">
        <app-opening-block></app-opening-block>
        <app-ranking-block></app-ranking-block>
      </div>
      <app-footer></app-footer>
    </div>
  `
})
export class HomePage {}
