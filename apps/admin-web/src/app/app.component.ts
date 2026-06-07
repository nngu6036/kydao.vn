import { Component } from '@angular/core';
import { NavigationEnd, Router } from '@angular/router';
import { filter } from 'rxjs';
import { AuthService } from './core/auth.service';

@Component({
  selector: 'app-root',
  template: `
    <div class="admin-shell" [class.login-shell]="isLoginRoute">
      <aside class="admin-sidebar" *ngIf="!isLoginRoute">
        <a class="brand" routerLink="/">
          <span class="brand-mark">TL</span>
          <span>
            <strong>Quản trị Kỳ Đạo</strong>
            <small>Vận hành nội dung</small>
          </span>
        </a>

        <nav class="admin-nav" aria-label="Khu vực quản trị">
          <a routerLink="/dashboard" routerLinkActive="active">Tổng quan</a>
          <a routerLink="/tournaments" routerLinkActive="active">Giải đấu</a>
          <a routerLink="/games" routerLinkActive="active">Ván đấu</a>
          <a routerLink="/players" routerLinkActive="active">Kỳ thủ</a>
        </nav>

        <button class="logout-action" type="button" (click)="auth.logout()">Đăng xuất</button>
      </aside>

      <main class="admin-main">
        <router-outlet></router-outlet>
      </main>
    </div>
  `,
})
export class AppComponent {
  isLoginRoute = false;

  constructor(readonly auth: AuthService, router: Router) {
    this.isLoginRoute = router.url.startsWith('/login');
    router.events.pipe(filter((event): event is NavigationEnd => event instanceof NavigationEnd)).subscribe((event) => {
      this.isLoginRoute = event.urlAfterRedirects.startsWith('/login');
    });
  }
}
