import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
    selector: 'app-faq',
    standalone: true,
    imports: [CommonModule],
    templateUrl: './faq.component.html',
    styles: [`
    .collapse { display: none; }
    .collapse.show { display: block; }
  `]
})
export class FaqComponent {
    faqs = [
        {
            question: 'What is the importance of SEO in digital marketing?',
            answer: 'It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout',
            isOpen: true
        },
        {
            question: 'How can I start digital marketing for my business?',
            answer: 'It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout',
            isOpen: false
        },
        {
            question: 'Is social media marketing part of digital marketinct',
            answer: 'It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout',
            isOpen: false
        }
    ];

    toggle(index: number) {
        this.faqs.forEach((item, i) => {
            if (i === index) {
                item.isOpen = !item.isOpen;
            } else {
                item.isOpen = false;
            }
        });
    }
}
