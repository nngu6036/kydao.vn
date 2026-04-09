import { Injectable, signal } from '@angular/core';

export interface LightboxItem {
    type: 'image' | 'iframe';
    src: string;
}

@Injectable({
    providedIn: 'root'
})
export class LightboxService {
    isOpen = signal(false);
    currentItem = signal<LightboxItem | null>(null);

    open(item: LightboxItem) {
        this.currentItem.set(item);
        this.isOpen.set(true);
        document.body.style.overflow = 'hidden';
    }

    close() {
        this.isOpen.set(false);
        this.currentItem.set(null);
        document.body.style.overflow = '';
    }
}
