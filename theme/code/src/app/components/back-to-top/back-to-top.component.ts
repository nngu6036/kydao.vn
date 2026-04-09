import { Component, OnInit, OnDestroy, Inject, PLATFORM_ID } from '@angular/core';
import { CommonModule, isPlatformBrowser } from '@angular/common';

@Component({
    selector: 'app-back-to-top',
    standalone: true,
    imports: [CommonModule],
    template: `
    <button id="back-top" class="back-to-top" (click)="scrollToTop()" [class.show]="isVisible">
        <i class="fa-solid fa-chevron-up"></i>
    </button>
  `,
    styles: []
})
export class BackToTopComponent implements OnInit, OnDestroy {
    isVisible = false;
    private scrollListener: (() => void) | null = null;
    private isBrowser: boolean;

    constructor(@Inject(PLATFORM_ID) platformId: object) {
        this.isBrowser = isPlatformBrowser(platformId);
    }

    ngOnInit(): void {
        if (this.isBrowser) {
            this.scrollListener = () => {
                this.isVisible = window.scrollY > 300;
            };
            window.addEventListener('scroll', this.scrollListener);
        }
    }

    ngOnDestroy(): void {
        if (this.scrollListener) {
            window.removeEventListener('scroll', this.scrollListener);
        }
    }

    scrollToTop(): void {
        if (this.isBrowser) {
            window.scrollTo({
                top: 0,
                behavior: 'smooth'
            });
        }
    }
}
