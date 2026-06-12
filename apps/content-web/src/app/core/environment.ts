import { contentEnvironment } from '../../environments/environment';

export interface ContentEnvironment {
  apiBaseUrl: string;
}

export const environment: ContentEnvironment = contentEnvironment;
