import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { SwiperDirective } from '../../directives/swiper.directive';
import { SwiperNavDirective } from '../../directives/swiper-nav.directive';
import { LightboxDirective } from '../../directives/lightbox.directive';
import { TeamSectionComponent } from '../../components/team-section/team-section.component';
import { MarqueeStyleTwoComponent } from '../../components/marquee-style-two/marquee-style-two.component';

@Component({
  selector: 'app-home',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    SwiperDirective,
    SwiperNavDirective,
    LightboxDirective,
    TeamSectionComponent,
    MarqueeStyleTwoComponent
  ],
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css']
})
export class HomeComponent {
  pageTitle = 'Home';

  // Services
  services = [
    {
      img: 'assets/images/service/serviceThumb1_1.png',
      icon: 'assets/images/icon/serviceIcon1_1.svg',
      title: 'Web Design &<br> Development',
      link: '/service-details',
      contentClass: 'service-content'
    },
    {
      img: 'assets/images/service/serviceThumb1_1.png',
      icon: 'assets/images/icon/serviceIcon1_2.svg',
      title: 'Digital Agency',
      link: '/service-details',
      contentClass: 'service-content style1'
    },
    {
      img: 'assets/images/service/serviceThumb1_1.png',
      icon: 'assets/images/icon/serviceIcon1_3.svg',
      title: 'Digital Marketing',
      link: '/service-details',
      contentClass: 'service-content style2'
    },
    {
      img: 'assets/images/service/serviceThumb1_1.png',
      icon: 'assets/images/icon/serviceIcon1_4.svg',
      title: 'Branding & Identity',
      link: '/service-details',
      contentClass: 'service-content style2'
    },
    {
      img: 'assets/images/service/serviceThumb1_1.png',
      icon: 'assets/images/icon/serviceIcon1_1.svg',
      title: 'SEO Optimization',
      link: '/service-details',
      contentClass: 'service-content style2'
    },
    {
      img: 'assets/images/service/serviceThumb1_1.png',
      icon: 'assets/images/icon/serviceIcon1_4.svg',
      title: 'Content Creation',
      link: '/service-details',
      contentClass: 'service-content style2'
    }
  ];

  // Projects (Features)
  features = [
    { number: '01', title: 'Empower Your Digital Presence', desc: 'Digital marketing involves promoting the a products or digital technolog Digital of am marketing Digital marketing involves', link: '#!' },
    { number: '02', title: 'Transform Your Online Strategy', desc: 'Digital marketing involves promoting the a products or digital technolog Digital of am marketing Digital marketing involves', link: '#!' },
    { number: '03', title: 'Shape Your Digital Future', desc: 'Digital marketing involves promoting the a products or digital technolog Digital of am marketing Digital marketing involves', link: '#!' }
  ];



  // Blog
  posts = [
    {
      img: 'assets/images/blog/blogThumb1_8.png',
      date: 'October 19, 2023', user: 'admin',
      title: 'Pioneering Tomorrow\'s Technology Today Your IT Partner for Progress',
      desc: 'Taxi service refers to the transportation of serv passengers from one location to another using a hired vehicle',
      link: '/blog-details',
      type: 'big'
    },
    {
      img: 'assets/images/blog/blogThumb1_9.png',
      date: 'October 19, 2023', user: 'admin',
      title: 'Streamlining Success in Bits and the more Bytes',
      desc: 'There are many variations of the a passages of Lorem',
      link: '/blog-details',
      type: 'small'
    },
    {
      img: 'assets/images/blog/blogThumb1_10.png',
      date: 'October 19, 2023', user: 'admin',
      title: 'Connect Engage Thrive Driven Excellence',
      desc: 'There are many variations of the a passages of Lorem',
      link: '/blog-details',
      type: 'small'
    }
  ];

  // Testimonials
  testimonials = [
    {
      text: 'A digital agency is a company that helps the businesses achieve their online through an services such web design social media of a management, SEO, and advertisin',
      img: 'assets/images/testimonial/testiThumb3_1.png',
      name: 'Ronald Richards',
      role: 'Web Designer'
    },
    {
      text: 'A digital agency is a company that helps the businesses achieve their online through an services such web design social media of a management, SEO, and advertisin',
      img: 'assets/images/testimonial/testiThumb3_2.png',
      name: 'Esther Howard',
      role: 'Web Developer'
    },
    {
      text: 'A digital agency is a company that helps the businesses achieve their online through an services such web design social media of a management, SEO, and advertisin',
      img: 'assets/images/testimonial/testiThumb3_3.png',
      name: 'Courtney Henry',
      role: 'Software Engineer'
    }
  ];


}
