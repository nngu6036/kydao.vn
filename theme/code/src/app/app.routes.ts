import { Routes } from '@angular/router';
import { MainLayoutComponent } from './layouts/main-layout/main-layout.component';

export const routes: Routes = [
    {
        path: '',
        component: MainLayoutComponent,
        children: [
            {
                path: '',
                loadComponent: () => import('./pages/home/home.component').then(m => m.HomeComponent),
                data: { footerStyle: 'style1' }
            },
            {
                path: 'home-2',
                loadComponent: () => import('./pages/home2/home2.component').then(m => m.Home2Component)
            },
            {
                path: 'home-3',
                loadComponent: () => import('./pages/home3/home3.component').then(m => m.Home3Component),
                data: { footerStyle: 'style1' }
            },
            {
                path: 'about',
                loadComponent: () => import('./pages/about/about.component').then(m => m.AboutComponent)
            },
            {
                path: 'services',
                loadComponent: () => import('./pages/services/services.component').then(m => m.ServicesComponent)
            },
            {
                path: 'service-details',
                loadComponent: () => import('./pages/service-details/service-details.component').then(m => m.ServiceDetailsComponent)
            },
            {
                path: 'projects',
                loadComponent: () => import('./pages/projects/projects.component').then(m => m.ProjectsComponent)
            },
            {
                path: 'project-details',
                loadComponent: () => import('./pages/project-details/project-details.component').then(m => m.ProjectDetailsComponent)
            },
            {
                path: 'blog',
                loadComponent: () => import('./pages/blog/blog.component').then(m => m.BlogComponent)
            },
            {
                path: 'blog-details',
                loadComponent: () => import('./pages/blog-details/blog-details.component').then(m => m.BlogDetailsComponent)
            },
            {
                path: 'team',
                loadComponent: () => import('./pages/team/team.component').then(m => m.TeamComponent)
            },
            {
                path: 'team-details',
                loadComponent: () => import('./pages/team-details/team-details.component').then(m => m.TeamDetailsComponent)
            },
            {
                path: 'contact',
                loadComponent: () => import('./pages/contact/contact.component').then(m => m.ContactComponent)
            }
        ]
    },
    {
        path: '**',
        redirectTo: ''
    }
];
