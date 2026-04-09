import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { BreadcrumbComponent } from '../../components/breadcrumb/breadcrumb.component';
import { BlogSidebarComponent } from '../../components/blog-sidebar/blog-sidebar.component';

@Component({
  selector: 'app-blog-details',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    BreadcrumbComponent,
    BlogSidebarComponent,
  ],
  templateUrl: './blog-details.component.html',
  styleUrls: ['./blog-details.component.css']
})
export class BlogDetailsComponent {
  pageTitle = 'Blog Details';
}
