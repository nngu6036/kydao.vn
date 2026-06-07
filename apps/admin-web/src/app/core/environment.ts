import environmentSettings from '../../environments/.env.json';

export interface AdminEnvironment {
  apiBaseUrl: string;
}

export const environment: AdminEnvironment = environmentSettings;
