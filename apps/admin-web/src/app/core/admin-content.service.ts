import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from './environment';

export type EntityKind = 'tournaments' | 'games' | 'players';

export interface PageResponse<T = Record<string, unknown>> {
  items: T[];
  total: number;
  skip: number;
  limit: number;
  page: number;
  page_size: number;
  pages: number;
}

export interface EntitySearchOption {
  id: string;
  name?: string;
  [key: string]: unknown;
}

export interface EntityField {
  key: string;
  label: string;
  type?: 'text' | 'number' | 'date' | 'textarea' | 'checkbox' | 'select' | 'entity-search';
  searchKind?: Extract<EntityKind, 'players' | 'tournaments'>;
  payloadKey?: string;
  options?: Array<{ value: string; label: string }>;
}

export interface EntityConfig {
  kind: EntityKind;
  label: string;
  singular: string;
  description: string;
  columns: string[];
  fields: EntityField[];
}

export const ENTITY_CONFIGS: Record<EntityKind, EntityConfig> = {
  tournaments: {
    kind: 'tournaments',
    label: 'Giải đấu',
    singular: 'Giải đấu',
    description: 'Hồ sơ các giải đấu được nhập từ sự kiện Kỳ Đạo.',
    columns: ['name', 'status', 'date', 'country', 'location', 'participants'],
    fields: [
      { key: 'name', label: 'Tên' },
      { key: 'date', label: 'Ngày', type: 'date' },
      {
        key: 'country',
        label: 'Quốc gia',
        type: 'select',
        options: [
          { value: 'vn', label: 'Việt Nam' },
          { value: 'non-vn', label: 'ngoài Việt Nam' },
        ],
      },
      { key: 'location', label: 'Địa điểm' },
      { key: 'participants', label: 'Số kỳ thủ', type: 'number' },
    ],
  },
  games: {
    kind: 'games',
    label: 'Ván đấu',
    singular: 'Ván đấu',
    description: 'Hồ sơ ván đấu gồm kỳ thủ, giải đấu, kết quả, FEN và danh sách nước đi.',
    columns: ['red_name', 'result', 'black_name', 'tournament_name', 'moves'],
    fields: [
      {
        key: 'red_id',
        label: 'Kỳ thủ đỏ',
        type: 'entity-search',
        searchKind: 'players',
      },
      {
        key: 'black_id',
        label: 'Kỳ thủ đen',
        type: 'entity-search',
        searchKind: 'players',
      },
      {
        key: 'result',
        label: 'Kết quả',
        type: 'select',
        options: [
          { value: 'win', label: 'Thắng' },
          { value: 'lose', label: 'Thua' },
          { value: 'draw', label: 'Hòa' },
        ],
      },
      {
        key: 'tournament_id',
        label: 'Giải đấu',
        type: 'entity-search',
        searchKind: 'tournaments',
      },
      { key: 'move_list', label: 'Danh sách nước đi', type: 'textarea' },
    ],
  },
  players: {
    kind: 'players',
    label: 'Kỳ thủ',
    singular: 'Kỳ thủ',
    description: 'Hồ sơ kỳ thủ và danh tính nguồn từ Kỳ Đạo.',
    columns: ['name', 'kydao_id', 'title', 'location', 'initial_level', 'rating'],
    fields: [
      { key: 'name', label: 'Tên' },
      { key: 'title', label: 'Danh hiệu' },
      {
        key: 'nationality',
        label: 'Quốc tịch',
        type: 'select',
        options: [
          { value: 'vn', label: 'Việt Nam' },
          { value: 'non-vn', label: 'ngoài Việt Nam' },
        ],
      },
      { key: 'location', label: 'Địa phương' },
      {
        key: 'initial_level',
        label: 'Cấp độ ban đầu',
        type: 'select',
        options: [
          { value: 'a2_level', label: 'Kỳ thủ A2' },
          { value: 'a1_level', label: 'Kỳ thủ A1' },
          { value: 'national_master', label: 'Kiện tướng quốc gia' },
          { value: 'international_master', label: 'Quốc tế đại sư' },
          { value: 'international_grand_master', label: 'Đặc cấp Quốc tế đại sư' },
        ],
      },
    ],
  },
};

@Injectable({ providedIn: 'root' })
export class AdminContentService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  list(
    kind: EntityKind,
    query = '',
    page = 1,
    pageSize = 25,
    sortBy?: string,
    sortDir: 'asc' | 'desc' = 'asc',
  ): Observable<PageResponse> {
    const params: Record<string, string | number> = { query, page, page_size: pageSize };
    if (sortBy) {
      params['sort_by'] = sortBy;
      params['sort_dir'] = sortDir;
    }
    return this.http.get<PageResponse>(`${this.baseUrl}/admin/${kind}`, {
      params,
    });
  }

  searchByName(kind: Extract<EntityKind, 'players' | 'tournaments'>, name: string): Observable<EntitySearchOption[]> {
    return this.http.get<EntitySearchOption[]>(`${this.baseUrl}/admin/${kind}/search`, {
      params: { name, limit: 10 },
    });
  }

  get(kind: EntityKind, id: string): Observable<Record<string, unknown>> {
    return this.http.get<Record<string, unknown>>(`${this.baseUrl}/admin/${kind}/${id}`);
  }

  create(kind: EntityKind, payload: Record<string, unknown>): Observable<Record<string, unknown>> {
    return this.http.post<Record<string, unknown>>(`${this.baseUrl}/admin/${kind}`, payload);
  }

  update(kind: EntityKind, id: string, payload: Record<string, unknown>): Observable<Record<string, unknown>> {
    return this.http.put<Record<string, unknown>>(`${this.baseUrl}/admin/${kind}/${id}`, payload);
  }

  delete(kind: EntityKind, id: string): Observable<void> {
    return this.http.delete<void>(`${this.baseUrl}/admin/${kind}/${id}`);
  }
}
