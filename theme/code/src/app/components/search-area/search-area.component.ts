import { Component, OnInit, Inject, PLATFORM_ID } from '@angular/core';
import { CommonModule, isPlatformBrowser } from '@angular/common';

@Component({
    selector: 'app-search-area',
    standalone: true,
    imports: [CommonModule],
    templateUrl: './search-area.component.html',
    styleUrls: ['./search-area.component.css']
})
export class SearchAreaComponent implements OnInit {
    private isBrowser: boolean;

    constructor(@Inject(PLATFORM_ID) platformId: object) {
        this.isBrowser = isPlatformBrowser(platformId);
    }

    ngOnInit(): void {
        if (this.isBrowser) {
            const searchClose = document.getElementById('search-close');
            searchClose?.addEventListener('click', () => this.closeSearch());
        }
    }

    closeSearch(): void {
        const searchWrap = document.querySelector('.search-wrap');
        searchWrap?.classList.remove('open');
    }

    onSearchSubmit(event: Event): void {
        event.preventDefault();
        const form = event.target as HTMLFormElement;
        const input = form.querySelector('input[type="search"]') as HTMLInputElement;
        if (input && input.value) {
            // Add your search logic here
        }
    }
}
