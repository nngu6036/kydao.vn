import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
    selector: 'app-marquee-style-two',
    standalone: true,
    imports: [CommonModule],
    templateUrl: './marquee-style-two.component.html'
})
export class MarqueeStyleTwoComponent {
    @Input() items: string[] = [
        'Presence Solutions',
        'Brilliance Studio',
        'Digital Success',
        'Presence Solutions',
        'Brilliance Studio',
        'Digital Success',
        'Presence Solutions',
        'Brilliance Studio',
        'Digital Success'
    ];
    @Input() extraClass: string = '';
}
