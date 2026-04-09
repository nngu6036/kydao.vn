import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';

@Component({
    selector: 'app-team-section',
    standalone: true,
    imports: [CommonModule, RouterModule],
    templateUrl: './team-section.component.html'
})
export class TeamSectionComponent {
    @Input() extraClass: string = '';
    @Input() members: any[] = [
        { img: 'assets/images/team/teamThumb1_1.png', name: 'James Mary', role: 'Ceo' },
        { img: 'assets/images/team/teamThumb1_2.png', name: 'James Mary', role: 'Ceo' },
        { img: 'assets/images/team/teamThumb1_3.png', name: 'James Mary', role: 'Ceo' }
    ];
    @Input() showTitle: boolean = true;
}
