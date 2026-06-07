import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { Component } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { AdminContentService } from '../core/admin-content.service';
import { ENTITY_EDIT_TEMPLATE, EntityEditBasePage } from './entity-edit.base';

@Component({
  template: ENTITY_EDIT_TEMPLATE,
})
export class TournamentEditPage extends EntityEditBasePage {
  constructor(route: ActivatedRoute, router: Router, api: AdminContentService) {
    super(route, router, api, 'tournaments');
  }
}
