import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { Component, inject } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { AdminContentService } from '../core/admin-content.service';
import { ENTITY_LIST_TEMPLATE, EntityListBasePage } from './entity-list.base';

@Component({
  template: ENTITY_LIST_TEMPLATE,
})
export class GameListPage extends EntityListBasePage {
  constructor() {
    super(inject(AdminContentService), 'games', [
      { key: 'red_name', label: 'Kỳ thủ đỏ', widthPercent: 24 },
      { key: 'result', label: 'Kết quả', widthPercent: 12 },
      { key: 'black_name', label: 'Kỳ thủ đen', widthPercent: 24 },
      { key: 'tournament_name', label: 'Giải đấu', widthPercent: 40 },
    ]);
  }
}
