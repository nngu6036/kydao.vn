import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { environment } from './environment';

const TOKEN_KEY = 'chess_elo_token';
const ID_TOKEN_KEY = 'chess_elo_id_token';
const REFRESH_TOKEN_KEY = 'chess_elo_refresh_token';

interface LoginResponse {
  access_token: string;
  token_type: string;
  expires_in?: number;
  id_token?: string;
  refresh_token?: string;
}

@Injectable({providedIn: 'root'})
export class AuthService {
  private readonly http = inject(HttpClient);

  getToken(): string | null { return localStorage.getItem(TOKEN_KEY); }
  isLoggedIn(): boolean { return !!this.getToken(); }
  clearSession() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(ID_TOKEN_KEY);
    localStorage.removeItem(REFRESH_TOKEN_KEY);
  }
  setSession(response: LoginResponse) {
    localStorage.setItem(TOKEN_KEY, response.access_token);
    if (response.id_token) {
      localStorage.setItem(ID_TOKEN_KEY, response.id_token);
    }
    if (response.refresh_token) {
      localStorage.setItem(REFRESH_TOKEN_KEY, response.refresh_token);
    }
  }
  login(username: string, password: string): Observable<LoginResponse> {
    return this.http.post<LoginResponse>(`${environment.apiBaseUrl}/auth/login`, { username, password }).pipe(
      tap((response) => this.setSession(response))
    );
  }
  logout() {
    this.clearSession();
    window.location.href = '/login';
  }
}
