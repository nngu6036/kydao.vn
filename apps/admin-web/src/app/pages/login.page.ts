import { NgIf } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { Component, inject } from '@angular/core';
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { AuthService } from '../core/auth.service';

const LAST_LOGIN_EMAIL_KEY = 'chess_elo_last_login_email';

@Component({
  template: `
    <section class="login-page">
      <div class="login-panel">
        <p class="eyebrow">Truy cập quản trị Kydao.vn</p>
        <div class="login-title-wrap">
          <h1>Đăng nhập</h1>
        </div>

        <form class="login-form" [formGroup]="form" (ngSubmit)="login()">
          <label>
            <span>Tên đăng nhập</span>
            <input type="text" formControlName="username" autocomplete="username" />
          </label>

          <label>
            <span>Mật khẩu</span>
            <input type="password" formControlName="password" autocomplete="current-password" />
          </label>

          <button class="primary-action" type="submit" [disabled]="form.invalid || loading">
            {{ loading ? 'Đang đăng nhập...' : 'Đăng nhập' }}
          </button>

          <span class="form-error" *ngIf="message">{{ message }}</span>
        </form>
      </div>
    </section>
  `,
})
export class LoginPage {
  private readonly auth = inject(AuthService);
  private readonly router = inject(Router);

  private readonly rememberedEmail = localStorage.getItem(LAST_LOGIN_EMAIL_KEY) ?? '';

  readonly form = new FormGroup({
    username: new FormControl(this.rememberedEmail, { nonNullable: true, validators: [Validators.required] }),
    password: new FormControl('', { nonNullable: true, validators: [Validators.required] }),
  });

  loading = false;
  message = '';

  constructor() {
    if (this.auth.isLoggedIn()) {
      this.router.navigate(['/dashboard']);
    }
  }

  login(): void {
    if (this.form.invalid || this.loading) {
      return;
    }

    this.loading = true;
    this.message = '';
    const { username, password } = this.form.getRawValue();

    this.auth.login(username, password).subscribe({
      next: () => {
        localStorage.setItem(LAST_LOGIN_EMAIL_KEY, username);
        this.loading = false;
        this.router.navigate(['/dashboard']);
      },
      error: (error: HttpErrorResponse) => {
        this.loading = false;
        this.message = this.errorMessage(error);
      },
    });
  }

  private errorMessage(error: HttpErrorResponse): string {
    if (error.status === 403) {
      return 'Tài khoản cần hoàn tất bước xác thực bổ sung trên Cognito.';
    }
    if (error.status === 0 || error.status >= 500) {
      return 'Không thể kết nối hệ thống đăng nhập. Vui lòng thử lại sau.';
    }
    return 'Tên đăng nhập hoặc mật khẩu không đúng.';
  }
}
