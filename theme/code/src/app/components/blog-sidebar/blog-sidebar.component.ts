import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';

@Component({
    selector: 'app-blog-sidebar',
    standalone: true,
    imports: [CommonModule, RouterModule],
    templateUrl: './blog-sidebar.component.html'
})
export class BlogSidebarComponent {
    @Input() sidebarClass: string = 'blog-main-sidebar';
    @Input() categoryClass: string = 'blog-widget-categories';

    recentPosts = [
        { title: 'Transforming Businesses The Digitally Crafted', category: 'Category', link: '/blog-details' },
        { title: 'Transforming Businesses The Digitally Crafted', category: 'Category', link: '/blog-details' },
        { title: 'Transforming Businesses The Digitally Crafted', category: 'Category', link: '/blog-details' }
    ];

    categories = [
        { title: 'Identity Design', count: '01', link: '/blog-details' },
        { title: 'Platform Setup', count: '02', link: '/blog-details' },
        { title: 'Marketing Automation', count: '03', link: '/blog-details', active: true },
        { title: 'Marketing Campaigns', count: '04', link: '/blog-details' }
    ];

    tags = [
        { title: 'Platform Setup', link: '/blog-details' },
        { title: 'Identity', link: '/blog-details' },
        { title: 'SEO', link: '/blog-details' },
        { title: 'Consulting', link: '/blog-details' },
        { title: 'Media', link: '/blog-details' },
        { title: 'Optimization', link: '/blog-details' }
    ];
}
