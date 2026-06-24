import { Component, inject } from '@angular/core';
import { catchError, debounceTime, distinctUntilChanged, map, of, startWith, switchMap } from 'rxjs';
import { AdminContentService } from '../core/admin-content.service';
import { EntityListBasePage } from './entity-list.base';

interface OpeningTreeNode {
  item: Record<string, unknown>;
  depth: number;
}

@Component({
  template: `
    <section class="page-head">
      <div>
        <p class="eyebrow">Quản lý</p>
        <h1>{{ config.label }}</h1>
        <p class="page-copy">{{ config.description }}</p>
      </div>
      <a class="primary-action" [routerLink]="['/', config.kind, 'new']">Tạo {{ config.singular }}</a>
    </section>

    <section class="work-panel entity-list-panel" *ngIf="treeVm$ | async as vm">
      <div class="table-toolbar">
        <input type="search" [formControl]="search" placeholder="Tìm kiếm khai cuộc" />
        <span>{{ vm.total }} bản ghi</span>
      </div>

      <div class="opening-tree-admin" *ngIf="vm.nodes.length; else emptyState">
        <div class="opening-tree-head">
          <span>Khai cuộc</span>
          <span>Mã</span>
          <span>Số ván</span>
          <span></span>
        </div>

        <div
          class="opening-tree-row-admin"
          *ngFor="let node of vm.nodes"
          [style.--opening-depth]="node.depth"
          [routerLink]="['/', config.kind, node.item['id'], 'edit']"
        >
          <span class="opening-tree-name-admin">
            <span class="opening-tree-guide" aria-hidden="true"></span>
            <span class="opening-tree-title">{{ display(node.item['name']) }}</span>
          </span>
          <span>{{ display(node.item['code']) }}</span>
          <span>{{ display(node.item['games']) }}</span>
          <span class="row-actions">
            <a
              class="row-action"
              [routerLink]="['/', config.kind, node.item['id'], 'edit']"
              (click)="$event.stopPropagation()"
            >
              Sửa
            </a>
            <button class="row-action row-action-danger" type="button" (click)="deleteItem(node.item); $event.stopPropagation()">
              Xóa
            </button>
          </span>
        </div>
      </div>

      <div class="pagination-bar pagination-bar-bottom" *ngIf="vm.total > treePageSize">
        <span>Đang hiển thị {{ vm.nodes.length }} / {{ vm.total }} mục đầu tiên</span>
      </div>

      <ng-template #emptyState>
        <div class="empty-state">
          <strong>Không tìm thấy khai cuộc</strong>
          <span>Hãy thử từ khóa khác hoặc tạo mục mới.</span>
        </div>
      </ng-template>
    </section>
  `,
})
export class OpeningListPage extends EntityListBasePage {
  readonly treePageSize = 200;

  readonly treeVm$ = this.search.valueChanges.pipe(
    startWith(''),
    debounceTime(200),
    distinctUntilChanged(),
    switchMap((query) =>
      this.api.list(this.config.kind, query, 1, this.treePageSize, 'name', 'asc').pipe(
        map((page) => ({
          total: page.total,
          nodes: this.toOpeningTree(page.items),
        })),
        catchError(() => of({ total: 0, nodes: [] as OpeningTreeNode[] }))
      )
    )
  );

  constructor() {
    super(inject(AdminContentService), 'openings', [
      { key: 'name', label: 'Tên khai cuộc', widthPercent: 55 },
      { key: 'code', label: 'Mã', widthPercent: 20 },
      { key: 'games', label: 'Số ván', widthPercent: 15 },
    ], { column: 'name', direction: 'asc' });
  }

  private toOpeningTree(items: Record<string, unknown>[]): OpeningTreeNode[] {
    const byId = new Map<string, Record<string, unknown>>();
    for (const item of items) {
      const id = this.itemId(item);
      if (id) {
        byId.set(id, item);
      }
    }

    const childrenByParent = new Map<string, Record<string, unknown>[]>();
    for (const item of items) {
      const parentId = this.parentId(item);
      const resolvedParentId = parentId && byId.has(parentId) ? parentId : '';
      const children = childrenByParent.get(resolvedParentId) ?? [];
      children.push(item);
      childrenByParent.set(resolvedParentId, children);
    }

    for (const children of childrenByParent.values()) {
      children.sort((a, b) => this.itemName(a).localeCompare(this.itemName(b), 'vi', { sensitivity: 'base' }));
    }

    const nodes: OpeningTreeNode[] = [];
    const visit = (item: Record<string, unknown>, depth: number, ancestors: Set<string>) => {
      const id = this.itemId(item);
      if (!id || ancestors.has(id)) {
        return;
      }
      nodes.push({ item, depth });
      const nextAncestors = new Set(ancestors);
      nextAncestors.add(id);
      for (const child of childrenByParent.get(id) ?? []) {
        visit(child, depth + 1, nextAncestors);
      }
    };

    for (const root of childrenByParent.get('') ?? []) {
      visit(root, 0, new Set<string>());
    }

    return nodes;
  }

  private itemId(item: Record<string, unknown>): string {
    return typeof item['id'] === 'string' ? item['id'] : '';
  }

  private parentId(item: Record<string, unknown>): string {
    return typeof item['parent_id'] === 'string' ? item['parent_id'] : '';
  }

  private itemName(item: Record<string, unknown>): string {
    return typeof item['name'] === 'string' ? item['name'] : '';
  }
}
