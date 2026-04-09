import { Directive, ElementRef, HostListener } from '@angular/core';
import { LightboxService } from '../services/lightbox.service';

@Directive({
    selector: '.popup-video, .img-popup',
    standalone: true
})
export class LightboxDirective {
    constructor(
        private el: ElementRef,
        private lightboxService: LightboxService
    ) { }

    @HostListener('click', ['$event'])
    onClick(event: Event) {
        event.preventDefault();
        const el = this.el.nativeElement as HTMLAnchorElement;
        const src = el.getAttribute('href');

        if (src) {
            const isVideo = el.classList.contains('popup-video');
            this.lightboxService.open({
                type: isVideo ? 'iframe' : 'image',
                src: src
            });
        }
    }
}
