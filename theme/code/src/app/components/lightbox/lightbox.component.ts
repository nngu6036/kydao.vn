import { Component, computed, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { DomSanitizer } from '@angular/platform-browser';
import { LightboxService } from '../../services/lightbox.service';

@Component({
    selector: 'app-lightbox',
    standalone: true,
    imports: [CommonModule],
    template: `
    @if (lightboxService.isOpen()) {
      <div class="lightbox-overlay" (click)="close()">
        <div class="lightbox-content" (click)="$event.stopPropagation()">
          <button class="close-btn" (click)="close()">
            <i class="fas fa-times"></i>
          </button>
          
          @if (item()?.type === 'image') {
            <img [src]="item()?.src" alt="Lightbox image">
          }
          
          @if (item()?.type === 'iframe') {
            <div class="iframe-container">
              <iframe 
                [src]="safeSrc()" 
                frameborder="0" 
                allow="autoplay; fullscreen" 
                allowfullscreen>
              </iframe>
            </div>
          }
        </div>
      </div>
    }
  `,
    styles: [`
    .lightbox-overlay {
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0, 0, 0, 0.9);
      z-index: 9999;
      display: flex;
      justify-content: center;
      align-items: center;
      animation: fadeIn 0.3s ease;
    }
    
    .lightbox-content {
      position: relative;
      max-width: 90%;
      max-height: 90%;
    }
    
    .lightbox-content img {
      max-width: 100%;
      max-height: 90vh;
      object-fit: contain;
      border-radius: 4px;
    }
    
    .iframe-container {
      width: 80vw;
      height: 80vh;
      max-width: 1200px;
    }
    
    .iframe-container iframe {
      width: 100%;
      height: 100%;
    }
    
    .close-btn {
      position: absolute;
      top: -40px;
      right: 0;
      background: none;
      border: none;
      color: white;
      font-size: 30px;
      cursor: pointer;
      padding: 5px;
      transition: transform 0.3s;
    }
    
    .close-btn:hover {
      transform: scale(1.1);
      color: #ff3c00;
    }
    
    @keyframes fadeIn {
      from { opacity: 0; }
      to { opacity: 1; }
    }
  `]
})
export class LightboxComponent {
    lightboxService = inject(LightboxService);
    sanitizer = inject(DomSanitizer);

    item = this.lightboxService.currentItem;

    safeSrc = computed(() => {
        const src = this.item()?.src;
        return src ? this.sanitizer.bypassSecurityTrustResourceUrl(src) : '';
    });

    close() {
        this.lightboxService.close();
    }
}
