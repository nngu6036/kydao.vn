import { Directive, ElementRef, AfterViewInit, Inject, PLATFORM_ID, OnDestroy } from '@angular/core';
import { isPlatformBrowser } from '@angular/common';
import Swiper from 'swiper';
import { Navigation, Pagination, Autoplay, EffectFade } from 'swiper/modules';

@Directive({
    selector: '.ct-slider, .et-slider',
    standalone: true
})
export class SwiperDirective implements AfterViewInit, OnDestroy {
    private swiperInstance: Swiper | null = null;
    private isBrowser: boolean;

    constructor(
        private el: ElementRef,
        @Inject(PLATFORM_ID) platformId: object
    ) {
        this.isBrowser = isPlatformBrowser(platformId);
    }

    ngAfterViewInit() {
        if (this.isBrowser) {
            this.initSwiper();
        }
    }

    ngOnDestroy() {
        if (this.swiperInstance) {
            this.swiperInstance.destroy();
        }
    }

    private initSwiper() {
        const sliderContainer = this.el.nativeElement;
        const sliderOptionsData = sliderContainer.getAttribute('data-slider-options');

        let sliderOptions: any = {};
        if (sliderOptionsData) {
            try {
                // Try to parse the JSON, handling potential format issues
                sliderOptions = JSON.parse(sliderOptionsData);
            } catch (e) {
                console.error('Invalid JSON in data-slider-options', e);
                return;
            }
        }

        const previousArrow = sliderContainer.querySelector('.slider-prev');
        const nextArrow = sliderContainer.querySelector('.slider-next');
        const paginationElement = sliderContainer.querySelector('.slider-pagination');
        const numberedPagination = sliderContainer.querySelector('.slider-pagination.pagi-number');

        const paginationStyle = sliderOptions['paginationType'] || 'bullets';
        const autoplaySettings = sliderOptions['autoplay'] || {
            delay: 6000,
            disableOnInteraction: false
        };

        const defaultSwiperConfig: any = {
            modules: [Navigation, Pagination, Autoplay, EffectFade],
            slidesPerView: 1,
            spaceBetween: sliderOptions['spaceBetween'] || 24,
            loop: sliderOptions['loop'] !== false,
            speed: sliderOptions['speed'] || 1000,
            initialSlide: sliderOptions['initialSlide'] || 0,
            centeredSlides: !!sliderOptions['centeredSlides'],
            effect: sliderOptions['effect'] || 'slide',
            fadeEffect: {
                crossFade: true
            },
            autoplay: autoplaySettings,
            navigation: {
                nextEl: nextArrow,
                prevEl: previousArrow,
            },
            pagination: {
                el: paginationElement,
                type: paginationStyle,
                clickable: true,
                renderBullet: function (index: number, className: string) {
                    const bulletNumber = index + 1;
                    const formattedNumber = bulletNumber < 10 ? '0' + bulletNumber : bulletNumber;
                    if (numberedPagination) {
                        return '<span class="' + className + ' number">' + formattedNumber + '</span>';
                    } else {
                        return '<span class="' + className + '" aria-label="Go to Slide ' + formattedNumber + '"></span>';
                    }
                },
            },
            on: {
                slideChange: () => {
                    setTimeout(() => {
                        if (this.swiperInstance && this.swiperInstance.params.mousewheel) {
                            // this.swiperInstance.params.mousewheel.releaseOnEdges = false;
                        }
                    }, 500);
                },
                reachEnd: () => {
                    setTimeout(() => {
                        if (this.swiperInstance && this.swiperInstance.params.mousewheel) {
                            // this.swiperInstance.params.mousewheel.releaseOnEdges = true;
                        }
                    }, 750);
                }
            }
        };

        const finalConfig = { ...defaultSwiperConfig, ...sliderOptions };

        this.swiperInstance = new Swiper(sliderContainer, finalConfig);

        // Store instance on element for external access if needed (like in original JS)
        (sliderContainer as any).swiper = this.swiperInstance;
    }
}
