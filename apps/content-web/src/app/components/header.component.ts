import { Component } from '@angular/core';
import { NgFor } from '@angular/common';
import { RouterLink } from '@angular/router';

@Component({
  selector: 'app-header',
  template: `
    <header class="site-header">
      <div class="header-with-banner" aria-hidden="true"></div>
      <div class="header-nav-bar">
        <div class="header-nav-container">
          <nav class="header-nav">
            <a *ngFor="let item of navItems" [routerLink]="item.href">{{ item.label }}</a>
          </nav>
        </div>
      </div>
    </header>
  `
})
export class HeaderComponent {
  navItems = [
     { label: 'Trang chủ', href: '/' },
    { label: 'Giải đấu', href: '/tournaments' },
    { label: 'Kỳ thủ', href: '/players' },
    { label: 'Ván cờ', href: '/games' },
    { label: 'Khai cuộc', href: '/openings' },
    { label: 'Bảng xếp hạng', href: '/rankings' },
  ];
}
