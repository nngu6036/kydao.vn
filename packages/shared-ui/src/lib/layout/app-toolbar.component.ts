import { Component, Input } from '@angular/core';
import { RouterLink } from '@angular/router';
import { NgFor } from '@angular/common';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatButtonModule } from '@angular/material/button';

@Component({
  selector: 'shared-app-toolbar',
  standalone: true,
  imports: [RouterLink, NgFor, MatToolbarModule, MatButtonModule],
  template: `
    <mat-toolbar color="primary">
      <span>{{ title }}</span>
      <span style="flex:1 1 auto"></span>
      <a mat-button *ngFor="let item of links" [routerLink]="item.href">{{ item.label }}</a>
    </mat-toolbar>
  `
})
export class AppToolbarComponent {
  @Input() title = 'Chess ELO';
  @Input() links: Array<{label: string; href: string}> = [];
}
