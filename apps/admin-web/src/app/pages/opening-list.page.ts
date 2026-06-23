import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { Component, inject } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { AdminContentService } from '../core/admin-content.service';
import { ENTITY_LIST_TEMPLATE, EntityListBasePage } from './entity-list.base';

@Component({
  template: ENTITY_LIST_TEMPLATE,
})
export class OpeningListPage extends EntityListBasePage {
  constructor() {
    super(inject(AdminContentService), 'openings', [
      { key: 'name', label: 'Tên khai cuộc', widthPercent: 55 },
      { key: 'code', label: 'Mã', widthPercent: 20 },
      { key: 'games', label: 'Số ván', widthPercent: 15 },
    ], { column: 'name', direction: 'asc' });
  }
}
