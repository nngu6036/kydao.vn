import { Component, AfterViewInit, OnDestroy, Inject, PLATFORM_ID, ElementRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { CommonModule, isPlatformBrowser } from '@angular/common';

@Component({
    selector: 'app-mouse-cursor',
    standalone: true,
    imports: [CommonModule],
    template: `
    <div #cursorOuter class="mouse-cursor cursor-outer"></div>
    <div #cursorInner class="mouse-cursor cursor-inner"></div>
  `,
    encapsulation: ViewEncapsulation.None,
    styles: [`
    .mouse-cursor { visibility: hidden; } 
  `]
})
export class MouseCursorComponent implements AfterViewInit, OnDestroy {
    @ViewChild('cursorInner') cursorInnerRef!: ElementRef;
    @ViewChild('cursorOuter') cursorOuterRef!: ElementRef;

    private isBrowser: boolean;
    private cleanups: (() => void)[] = [];

    constructor(@Inject(PLATFORM_ID) platformId: Object) {
        this.isBrowser = isPlatformBrowser(platformId);
    }

    ngAfterViewInit() {
        if (!this.isBrowser) return;

        const inner = this.cursorInnerRef.nativeElement;
        const outer = this.cursorOuterRef.nativeElement;

        // Visibility defaults
        inner.style.visibility = 'visible';
        outer.style.visibility = 'visible';

        // Mouse Move
        const onMouseMove = (e: MouseEvent) => {
            const x = e.clientX;
            const y = e.clientY;
            // Direct transform for GPU performance
            outer.style.transform = `translate(${x}px, ${y}px)`;
            inner.style.transform = `translate(${x}px, ${y}px)`;
        };

        window.addEventListener('mousemove', onMouseMove);
        this.cleanups.push(() => window.removeEventListener('mousemove', onMouseMove));

        const onMouseOver = (e: Event) => {
            const target = e.target as HTMLElement;
            if (target.closest('a') || target.closest('.cursor-pointer') || target.closest('button')) {
                inner.classList.add('cursor-hover');
                outer.classList.add('cursor-hover');
            }
        };
        const onMouseOut = (e: Event) => {
            const target = e.target as HTMLElement;
            if (target.closest('a') || target.closest('.cursor-pointer') || target.closest('button')) {
                inner.classList.remove('cursor-hover');
                outer.classList.remove('cursor-hover');
            }
        };

        document.body.addEventListener('mouseover', onMouseOver);
        document.body.addEventListener('mouseout', onMouseOut);

        this.cleanups.push(() => {
            document.body.removeEventListener('mouseover', onMouseOver);
            document.body.removeEventListener('mouseout', onMouseOut);
        });
    }

    ngOnDestroy() {
        this.cleanups.forEach(fn => fn());
    }
}
