import { Directive, ElementRef, OnInit, Inject, PLATFORM_ID, Renderer2 } from '@angular/core';
import { isPlatformBrowser } from '@angular/common';

@Directive({
    selector: '[appProgressBar]',
    standalone: true
})
export class ProgressBarDirective implements OnInit {
    private isBrowser: boolean;

    constructor(
        private el: ElementRef,
        private renderer: Renderer2,
        @Inject(PLATFORM_ID) platformId: object
    ) {
        this.isBrowser = isPlatformBrowser(platformId);
    }

    ngOnInit() {
        if (this.isBrowser) {
            this.initObserver();
        }
    }

    private initObserver() {
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    this.animate();
                    observer.unobserve(this.el.nativeElement);
                }
            });
        }, { threshold: 0.75 });

        observer.observe(this.el.nativeElement);
    }

    private animate() {
        const style = getComputedStyle(this.el.nativeElement);
        const width = style.width;

        // Check if width is defined in inline style as percentage
        const inlineStyle = this.el.nativeElement.getAttribute('style');
        const match = inlineStyle ? inlineStyle.match(/width:\s*(\d+)%/) : null;

        if (match) {
            const progressWidth = match[1] + '%';
            this.renderer.setStyle(this.el.nativeElement, '--progress-width', progressWidth);
            this.renderer.setStyle(this.el.nativeElement, 'animation', 'animate-positive 1.8s forwards');
            this.renderer.setStyle(this.el.nativeElement, 'opacity', '1');
        }
    }
}
