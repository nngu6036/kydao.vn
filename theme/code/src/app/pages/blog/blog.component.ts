import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { BreadcrumbComponent } from '../../components/breadcrumb/breadcrumb.component';
import { BlogSidebarComponent } from '../../components/blog-sidebar/blog-sidebar.component';

@Component({
  selector: 'app-blog',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    BreadcrumbComponent,
    BlogSidebarComponent,
  ],
  templateUrl: './blog.component.html',
  styleUrls: ['./blog.component.css']
})
export class BlogComponent {
  pageTitle = 'Blog';

  posts = [
    {
      img: 'assets/images/blog/blogThumb1_1.png',
      date: '23 Dec 2023',
      user: 'admin',
      category: 'Category',
      title: 'Connect Digitally Grow Exponentially Businesses Digitally Crafted for Digital Transforming',
      link: '/blog-details'
    },
    {
      img: 'assets/images/blog/blogThumb1_2.png',
      date: '23 Dec 2023',
      user: 'admin',
      category: 'Category',
      title: 'Make Your Mark Online Where Ideas Go Digital Empowering Your Online Success',
      link: '/blog-details'
    },
    {
      img: 'assets/images/blog/blogThumb1_3.png',
      date: '23 Dec 2023',
      user: 'admin',
      category: 'Category',
      title: 'Your Digital Partner in Growth Unleash Your Digital Potential Digitally Crafted for Digital',
      link: '/blog-details'
    }
  ];
}
