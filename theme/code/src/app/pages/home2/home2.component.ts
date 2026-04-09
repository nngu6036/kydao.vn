import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { SwiperDirective } from '../../directives/swiper.directive';
import { SwiperNavDirective } from '../../directives/swiper-nav.directive';
import { LightboxDirective } from '../../directives/lightbox.directive';
import { ChooseUsComponent } from '../../components/choose-us/choose-us.component';
import { CounterComponent } from '../../components/counter/counter.component';
import { ProjectStyleOneComponent } from '../../components/project-style-one/project-style-one.component';
import { MarqueeStyleTwoComponent } from '../../components/marquee-style-two/marquee-style-two.component';

@Component({
  selector: 'app-home2',
  standalone: true,
  imports: [CommonModule, RouterModule, SwiperDirective, SwiperNavDirective, LightboxDirective, ChooseUsComponent, CounterComponent, ProjectStyleOneComponent, MarqueeStyleTwoComponent],
  templateUrl: './home2.component.html',
  styleUrls: ['./home2.component.css']
})
export class Home2Component {
  pageTitle = 'Home 2';
}



