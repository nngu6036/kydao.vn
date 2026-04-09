import { Component, Input } from '@angular/core';
import { MatCardModule } from '@angular/material/card';

@Component({
  selector: 'shared-player-card',
  standalone: true,
  imports: [MatCardModule],
  template: `<mat-card><h3>{{ name }}</h3><div>{{ subtitle }}</div></mat-card>`
})
export class PlayerCardComponent {
  @Input() name = '';
  @Input() subtitle = '';
}
