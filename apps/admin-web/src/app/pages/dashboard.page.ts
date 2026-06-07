import { AsyncPipe, DecimalPipe, NgFor, NgIf } from '@angular/common';
import { Component, inject } from '@angular/core';
import { RouterLink } from '@angular/router';
import { catchError, forkJoin, map, of } from 'rxjs';
import { AdminContentService, ENTITY_CONFIGS, EntityKind } from '../core/admin-content.service';

@Component({
  template: `
    <section class="page-head">
      <div>
        <p class="eyebrow">Trung tâm điều khiển</p>
        <h1>Tổng quan</h1>
      </div>
    </section>

    <section class="metric-grid" *ngIf="metrics$ | async as metrics">
      <a class="metric-card" *ngFor="let metric of metrics" [routerLink]="['/', metric.kind]">
        <span>{{ metric.label }}</span>
        <strong>{{ metric.total | number }}</strong>
        <small>{{ metric.description }}</small>
      </a>
    </section>

    <section class="work-panel">
      <div class="panel-head">
        <div>
          <p class="eyebrow">Quy trình</p>
          <h2>Bảo trì nội dung</h2>
        </div>
      </div>
      <div class="quick-grid">
        <a *ngFor="let item of quickLinks" [routerLink]="['/', item.kind, 'new']">
          <span>{{ item.singular }}</span>
          <strong>Tạo bản ghi</strong>
        </a>
      </div>
    </section>
  `,
})
export class DashboardPage {
  private readonly api = inject(AdminContentService);
  readonly quickLinks = Object.values(ENTITY_CONFIGS);

  readonly metrics$ = forkJoin(
    (Object.keys(ENTITY_CONFIGS) as EntityKind[]).map((kind) =>
      this.api.list(kind, '', 1, 1).pipe(
        map((page) => ({
          kind,
          label: ENTITY_CONFIGS[kind].label,
          description: ENTITY_CONFIGS[kind].description,
          total: page.total,
        })),
        catchError(() =>
          of({
            kind,
            label: ENTITY_CONFIGS[kind].label,
            description: ENTITY_CONFIGS[kind].description,
            total: 0,
          })
        )
      )
    )
  );
}
