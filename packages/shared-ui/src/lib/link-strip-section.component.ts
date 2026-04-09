import { Component, Input } from '@angular/core';
import { NgFor } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import type { ContentLink } from '@chess-elo/shared-types';

@Component({
  selector: 'shared-link-strip-section',
  standalone: true,
  imports: [NgFor, MatCardModule],
  template: `
    <mat-card style="margin:12px 0;">
      <div><strong>{{ title }}</strong></div>
      <div style="margin-top:8px; display:flex; flex-wrap:wrap; gap:10px;">
        <a *ngFor="let item of items" [href]="item.href">{{ item.title }}</a>
      </div>
    </mat-card>
  `
})
export class LinkStripSectionComponent {
  @Input() title = '';
  @Input() items: ContentLink[] = [];
}
