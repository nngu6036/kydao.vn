import { Component, OnInit, Inject, PLATFORM_ID } from '@angular/core';
import { CommonModule, isPlatformBrowser } from '@angular/common';
import { RouterModule } from '@angular/router';

@Component({
    selector: 'app-offcanvas',
    standalone: true,
    imports: [CommonModule, RouterModule],
    templateUrl: './offcanvas.component.html',
    styleUrls: ['./offcanvas.component.css']
})
export class OffcanvasComponent implements OnInit {
    private isBrowser: boolean;

    menuItems = [
        {
            title: 'Home',
            open: false,
            subItems: [
                { title: 'Home 01', link: '/' },
                { title: 'Home 02', link: '/home-2' },
                { title: 'Home 03', link: '/home-3' }
            ]
        },
        { title: 'About Us', link: '/about' },
        {
            title: 'Services',
            open: false,
            subItems: [
                { title: 'Services', link: '/services' },
                { title: 'Service Details', link: '/service-details' }
            ]
        },
        {
            title: 'Projects',
            open: false,
            subItems: [
                { title: 'Projects', link: '/projects' },
                { title: 'Project Details', link: '/project-details' }
            ]
        },
        {
            title: 'Blog',
            open: false,
            subItems: [
                { title: 'Blog', link: '/blog' },
                { title: 'Blog Details', link: '/blog-details' }
            ]
        },
        {
            title: 'Pages',
            open: false,
            subItems: [
                { title: 'Team', link: '/team' },
                { title: 'Team Details', link: '/team-details' }
            ]
        },
        { title: 'Contact', link: '/contact' }
    ];

    constructor(@Inject(PLATFORM_ID) platformId: object) {
        this.isBrowser = isPlatformBrowser(platformId);
    }

    ngOnInit(): void {
        if (this.isBrowser) {
            this.initOffcanvas();
        }
    }

    toggleSubMenu(item: any): void {
        item.open = !item.open;
    }

    private initOffcanvas(): void {
        const closeBtn = document.querySelector('.offcanvas__close button');
        const overlay = document.querySelector('.offcanvas__overlay');

        const closeOffcanvas = () => {
            this.closeOffcanvas();
        };

        closeBtn?.addEventListener('click', closeOffcanvas);
        overlay?.addEventListener('click', closeOffcanvas);
    }

    closeOffcanvas(): void {
        const offcanvasInfo = document.querySelector('.offcanvas__info');
        const overlay = document.querySelector('.offcanvas__overlay');

        if (offcanvasInfo && overlay) {
            offcanvasInfo.classList.remove('info-open');
            overlay.classList.remove('overlay-open');
        }
    }
}
