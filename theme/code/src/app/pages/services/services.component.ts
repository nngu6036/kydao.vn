import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { BreadcrumbComponent } from '../../components/breadcrumb/breadcrumb.component';
import { FaqComponent } from '../../components/faq/faq.component';


@Component({
  selector: 'app-services',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    BreadcrumbComponent,
    FaqComponent
  ],
  templateUrl: './services.component.html',
  styleUrls: ['./services.component.css']
})
export class ServicesComponent {
  pageTitle = 'Services';

  services = [
    { icon: 'assets/images/icon/serviceIcon2_1.svg', title: 'Navigate the Digital Lands cape', desc: 'Digital marketing involves promoting products services using digital technologies' },
    { icon: 'assets/images/icon/serviceIcon2_2.svg', title: 'Dominate the Online Digital Strategy', desc: 'Digital marketing involves promoting products services using digital technologies' },
    { icon: 'assets/images/icon/serviceIcon2_3.svg', title: 'Elevate Your Brand Big Online Presence', desc: 'Digital marketing involves promoting products services using digital technologies' },
    { icon: 'assets/images/icon/serviceIcon2_4.svg', title: 'Elevate Your Online Presence Defined', desc: 'Digital marketing involves promoting products services using digital technologies' },
    { icon: 'assets/images/icon/serviceIcon2_5.svg', title: 'Transforming Brands Digitally Digital', desc: 'Digital marketing involves promoting products services using digital technologies' },
    { icon: 'assets/images/icon/serviceIcon2_6.svg', title: 'Powering Your Digital Journey Solutions', desc: 'Digital marketing involves promoting products services using digital technologies' }
  ];
}
