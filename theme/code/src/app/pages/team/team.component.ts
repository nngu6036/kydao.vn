import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { BreadcrumbComponent } from '../../components/breadcrumb/breadcrumb.component';
import { TeamSectionComponent } from '../../components/team-section/team-section.component';


@Component({
  selector: 'app-team',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    BreadcrumbComponent,
    TeamSectionComponent
  ],
  templateUrl: './team.component.html',
  styleUrls: ['./team.component.css']
})
export class TeamComponent {
  pageTitle = 'Team';

  teamMembers = [
    { img: 'assets/images/team/teamThumb1_1.png', name: 'Cameron Williamson', role: 'Senior Manager', link: '/team-details' },
    { img: 'assets/images/team/teamThumb1_2.png', name: 'Brooklyn Simmons', role: 'Senior Manager', link: '/team-details' },
    { img: 'assets/images/team/teamThumb1_3.png', name: 'Guy Hawkins', role: 'Senior Manager', link: '/team-details' },
    { img: 'assets/images/team/teamThumb1_4.png', name: 'Leslie Alexander', role: 'Senior Manager', link: '/team-details' },
    { img: 'assets/images/team/teamThumb1_5.png', name: 'Robert Fox', role: 'Senior Manager', link: '/team-details' },
    { img: 'assets/images/team/teamThumb1_6.png', name: 'Arlene McCoy', role: 'Senior Manager', link: '/team-details' }
  ];
}
