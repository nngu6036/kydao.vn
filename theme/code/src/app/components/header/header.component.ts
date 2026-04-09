import { Component, OnInit, OnDestroy, Inject, PLATFORM_ID } from '@angular/core';
import { CommonModule, isPlatformBrowser } from '@angular/common';
import { RouterModule } from '@angular/router';

@Component({
  selector: 'app-header',
  standalone: true,
  imports: [CommonModule, RouterModule],
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.css']
})
export class HeaderComponent implements OnInit, OnDestroy {
  private scrollListener: (() => void) | null = null;
  private isBrowser: boolean;

  constructor(@Inject(PLATFORM_ID) platformId: object) {
    this.isBrowser = isPlatformBrowser(platformId);
  }

  ngOnInit(): void {
    if (this.isBrowser) {
      this.initStickyHeader();
    }
  }

  ngOnDestroy(): void {
    if (this.scrollListener) {
      window.removeEventListener('scroll', this.scrollListener);
    }
  }

  private initStickyHeader(): void {
    const header = document.getElementById('ct-header-sticky');
    if (!header) return;

    this.scrollListener = () => {
      if (window.scrollY > 100) {
        header.classList.add('header-sticky');
      } else {
        header.classList.remove('header-sticky');
      }
    };

    window.addEventListener('scroll', this.scrollListener);
  }

  toggleOffcanvas(): void {
    const offcanvasInfo = document.querySelector('.offcanvas__info');
    const overlay = document.querySelector('.offcanvas__overlay');

    if (offcanvasInfo && overlay) {
      offcanvasInfo.classList.add('info-open');
      overlay.classList.add('overlay-open');
    }
  }
}
