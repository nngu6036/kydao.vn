import { Injectable } from '@angular/core';
const TOKEN_KEY = 'chess_elo_token';
@Injectable({providedIn: 'root'})
export class AuthService {
  getToken(): string | null { return localStorage.getItem(TOKEN_KEY); }
  isLoggedIn(): boolean { return !!this.getToken(); }
  setToken(token: string) { localStorage.setItem(TOKEN_KEY, token); }
  logout() { localStorage.removeItem(TOKEN_KEY); window.location.href = '/'; }
}
