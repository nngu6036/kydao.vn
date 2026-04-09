import { Component, Input } from '@angular/core';
import { MatCardModule } from '@angular/material/card';

@Component({
  selector: 'shared-empty-state',
  standalone: true,
  imports: [MatCardModule],
  template: `<mat-card><h3>{{ title }}</h3><p>{{ message }}</p></mat-card>`
})
export class EmptyStateComponent {
  @Input() title = 'No data';
  @Input() message = 'Nothing to show.';
}
