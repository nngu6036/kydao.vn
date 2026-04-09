import { Directive, ElementRef, HostListener, Input, OnInit } from '@angular/core';

@Directive({
    selector: '[data-slider-prev], [data-slider-next]',
    standalone: true
})
export class SwiperNavDirective {

    constructor(private el: ElementRef) { }

    @HostListener('click')
    onClick() {
        const prevSelector = this.el.nativeElement.getAttribute('data-slider-prev');
        const nextSelector = this.el.nativeElement.getAttribute('data-slider-next');
        const targetSelector = prevSelector || nextSelector;

        if (targetSelector) {
            const targetElement = document.querySelector(targetSelector);
            if (targetElement && (targetElement as any).swiper) {
                const swiper = (targetElement as any).swiper;
                if (prevSelector) {
                    swiper.slidePrev();
                } else {
                    swiper.slideNext();
                }
            }
        }
    }
}
