import { Directive, ElementRef, Input, OnInit, Renderer2 } from '@angular/core';

@Directive({
    selector: '[appBgImage]',
    standalone: true
})
export class BgImageDirective implements OnInit {
    @Input('appBgImage') bgImage: string = '';
    @Input() maskImage: string = '';

    constructor(private el: ElementRef, private renderer: Renderer2) { }

    ngOnInit() {
        // Handle Background Image
        if (this.bgImage) {
            this.renderer.setStyle(this.el.nativeElement, 'background-image', `url(${this.bgImage})`);
            this.renderer.addClass(this.el.nativeElement, 'background-image');
        } else {
            // Check for data-bg-src attribute if input not provided
            const dataBgSrc = this.el.nativeElement.getAttribute('data-bg-src');
            if (dataBgSrc) {
                this.renderer.setStyle(this.el.nativeElement, 'background-image', `url(${dataBgSrc})`);
                this.renderer.addClass(this.el.nativeElement, 'background-image');
            }
        }

        // Handle Mask Image
        if (this.maskImage) {
            this.applyMask(this.maskImage);
        } else {
            const dataMaskSrc = this.el.nativeElement.getAttribute('data-mask-src');
            if (dataMaskSrc) {
                this.applyMask(dataMaskSrc);
            }
        }
    }

    private applyMask(maskUrl: string) {
        this.renderer.setStyle(this.el.nativeElement, 'mask-image', `url(${maskUrl})`);
        this.renderer.setStyle(this.el.nativeElement, '-webkit-mask-image', `url(${maskUrl})`);
        this.renderer.addClass(this.el.nativeElement, 'bg-mask');
    }
}
