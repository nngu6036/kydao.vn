import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { Component, inject } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { AdminContentService } from '../core/admin-content.service';
import { ENTITY_LIST_TEMPLATE, EntityListBasePage } from './entity-list.base';

@Component({
  template: ENTITY_LIST_TEMPLATE,
})
export class PlayerListPage extends EntityListBasePage {
  constructor() {
    super(inject(AdminContentService), 'players', [
      { key: 'name', label: 'Tên kỳ thủ', widthPercent: 42 },
      { key: 'title', label: 'Danh hiệu', widthPercent: 20 },
      { key: 'nationality', label: 'Quốc tịch', widthPercent: 18 },
      { key: 'elo', label: 'ELO', widthPercent: 10 },
    ], { column: 'name', direction: 'asc' });
  }
}
