import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { Component, inject } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { AdminContentService } from '../core/admin-content.service';
import { ENTITY_LIST_TEMPLATE, EntityListBasePage } from './entity-list.base';

@Component({
  template: ENTITY_LIST_TEMPLATE,
})
export class TournamentListPage extends EntityListBasePage {
  constructor() {
    super(inject(AdminContentService), 'tournaments', [
      { key: 'name', label: 'Tên giải đấu', widthPercent: 82 },
      { key: 'participants', label: 'Số kỳ thủ', widthPercent: 18 },
    ]);
  }
}
