import environmentSettings from '../../environments/environment.json';

export interface AdminEnvironment {
  apiBaseUrl: string;
}

export const environment: AdminEnvironment = environmentSettings;
