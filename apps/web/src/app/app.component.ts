import { Component } from '@angular/core';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatButtonModule } from '@angular/material/button';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [MatToolbarModule, MatButtonModule],
  template: `
    <mat-toolbar color="primary">
      <span>♟ Chess ELO</span>
      <span style="flex:1"></span>
      <button mat-button>Search</button>
      <button mat-button>Admin</button>
    </mat-toolbar>
  `
})
export class AppComponent {}
