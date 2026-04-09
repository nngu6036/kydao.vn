import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { RouterLink } from '@angular/router';

@Component({
    selector: 'app-footer',
    standalone: true,
    imports: [CommonModule, RouterLink, ReactiveFormsModule],
    templateUrl: './footer.component.html',
    styleUrls: ['./footer.component.css']
})
export class FooterComponent {
    @Input() layout: 'style1' | 'style2' = 'style2';
    currentYear: number = new Date().getFullYear();
    emailForm: FormGroup;

    constructor() {
        this.emailForm = new FormGroup({
            email: new FormControl('', [Validators.required, Validators.email])
        });
    }

    onSubmit() {
        if (this.emailForm.valid) {
            console.log('Email submitted:', this.emailForm.value.email);
            this.emailForm.reset();
        }
    }
}
