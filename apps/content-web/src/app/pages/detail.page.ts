import { Component, inject } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { HeaderComponent } from '../components/header.component';
import { FooterComponent } from '../components/footer.component';

@Component({
  standalone: true,
  imports: [RouterLink, HeaderComponent, FooterComponent],
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <div class="search-container">
          <h1 class="search-title">{{ kind }}</h1>
          <div class="content-block">
            <p>Day la trang chi tiet mau cho {{ kind.toLowerCase() }}.</p>
            <p><strong>ID:</strong> {{ id }}</p>
          </div>
          <p><a routerLink="/">Quay lai trang chu</a></p>
        </div>
      </section>
      <app-footer></app-footer>
    </div>
  `
})
export class DetailPage {
  private readonly route = inject(ActivatedRoute);
  readonly id = this.route.snapshot.paramMap.get('id');
  readonly kind = this.route.snapshot.data['kind'] || 'Chi tiet';
}
