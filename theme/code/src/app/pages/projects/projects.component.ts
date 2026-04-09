import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { BreadcrumbComponent } from '../../components/breadcrumb/breadcrumb.component';
import { FaqComponent } from '../../components/faq/faq.component';


@Component({
  selector: 'app-projects',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    BreadcrumbComponent,
    FaqComponent
  ],
  templateUrl: './projects.component.html',
  styleUrls: ['./projects.component.css']
})
export class ProjectsComponent {
  pageTitle = 'Projects';

  projects = [
    { img: 'assets/images/project/projectThumb2_1.png', link: '/project-details', colClass: 'col-lg-6 col-md-6' },
    { img: 'assets/images/project/projectThumb2_2.png', link: '/project-details', colClass: 'col-lg-6 col-md-6' },
    { img: 'assets/images/project/projectThumb2_3.png', link: '/project-details', colClass: 'col-lg-7 col-md-7' },
    { img: 'assets/images/project/projectThumb2_4.png', link: '/project-details', colClass: 'col-lg-5 col-md-5' },
    { img: 'assets/images/project/projectThumb2_5.png', link: '/project-details', colClass: 'col-lg-6 col-md-6' },
    { img: 'assets/images/project/projectThumb2_6.png', link: '/project-details', colClass: 'col-lg-6 col-md-6' }
  ];
}
