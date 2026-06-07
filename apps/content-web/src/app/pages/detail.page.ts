import { Component, inject } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { HeaderComponent } from '../components/header.component';
import { FooterComponent } from '../components/footer.component';

@Component({
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <div class="search-container">
          <h1 class="search-title">{{ kind }}</h1>
          <div class="content-block">
            <p>Đây là trang chi tiết mẫu cho {{ kind.toLowerCase() }}.</p>
            <p><strong>ID:</strong> {{ id }}</p>
          </div>
          <p><a routerLink="/">Quay lại trang chủ</a></p>
        </div>
      </section>
      <app-footer></app-footer>
    </div>
  `
})
export class DetailPage {
  private readonly route = inject(ActivatedRoute);
  readonly id = this.route.snapshot.paramMap.get('id');
  readonly kind = this.route.snapshot.data['kind'] || 'Chi tiết';
}
