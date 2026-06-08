import { adminEnvironment } from '../../environments/environment';

export interface AdminEnvironment {
  apiBaseUrl: string;
}

export const environment: AdminEnvironment = adminEnvironment;
