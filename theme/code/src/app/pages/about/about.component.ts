import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { BreadcrumbComponent } from '../../components/breadcrumb/breadcrumb.component';
import { TeamSectionComponent } from '../../components/team-section/team-section.component';
import { ChooseUsComponent } from '../../components/choose-us/choose-us.component';
import { CounterComponent } from '../../components/counter/counter.component';
import { ProjectStyleOneComponent } from '../../components/project-style-one/project-style-one.component';

@Component({
  selector: 'app-about',
  standalone: true,
  imports: [CommonModule, RouterModule, BreadcrumbComponent, TeamSectionComponent, ChooseUsComponent, CounterComponent, ProjectStyleOneComponent],
  templateUrl: './about.component.html',
  styleUrls: ['./about.component.css']
})
export class AboutComponent {
  pageTitle = 'About Us';
}



