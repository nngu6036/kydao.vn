import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { SwiperDirective } from '../../directives/swiper.directive';
import { SwiperNavDirective } from '../../directives/swiper-nav.directive';
import { LightboxDirective } from '../../directives/lightbox.directive';
import { TeamSectionComponent } from '../../components/team-section/team-section.component';
import { MarqueeStyleTwoComponent } from '../../components/marquee-style-two/marquee-style-two.component';

@Component({
  selector: 'app-home3',
  standalone: true,
  imports: [CommonModule, RouterModule, SwiperDirective, SwiperNavDirective, LightboxDirective, TeamSectionComponent, MarqueeStyleTwoComponent],
  templateUrl: './home3.component.html',
  styleUrls: ['./home3.component.css']
})
export class Home3Component {
  pageTitle = 'Home 3';
}



