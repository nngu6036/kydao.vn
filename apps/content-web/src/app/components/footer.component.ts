import { Component } from '@angular/core';
import { NgFor } from '@angular/common';

@Component({
  selector: 'app-footer',
  standalone: true,
  imports: [NgFor],
  template: `
    <footer class="footer">
      <div class="footer-container">
        <div class="footer-section">
          <h3 class="footer-title">Về kydao.vn</h3>
          <p class="footer-description">Cơ sở dữ liệu cờ tướng toàn diện của Việt Nam. Lưu trữ và phân tích hàng nghìn ván cờ, giải đấu, và kỳ thủ.</p>
        </div>
        <div class="footer-section">
          <h3 class="footer-title">Liên kết</h3>
          <div class="footer-links"><a *ngFor="let item of footerLinks1" [href]="item.href">{{ item.label }}</a></div>
        </div>
        <div class="footer-section">
          <h3 class="footer-title">Tài nguyên</h3>
          <div class="footer-links"><a *ngFor="let item of footerLinks2" [href]="item.href">{{ item.label }}</a></div>
        </div>
        <div class="footer-section">
          <h3 class="footer-title">Kết nối</h3>
          <div class="footer-social"><a href="#">Facebook</a><a href="#">GitHub</a><a href="mailto:contact@kydao.vn">Email</a></div>
        </div>
      </div>
      <div class="footer-bottom"><p>© 2026 kydao.vn. Tất cả các quyền được bảo lưu.</p></div>
    </footer>
  `
})
export class FooterComponent {
  footerLinks1 = [
    { label: 'Giới thiệu', href: '#' }, { label: 'Liên hệ', href: '#' }, { label: 'Chính sách bảo mật', href: '#' }, { label: 'Điều khoản sử dụng', href: '#' }
  ];
  footerLinks2 = [
    { label: 'API', href: '#' }, { label: 'Tài liệu', href: '#' }, { label: 'Đóng góp', href: '#' }, { label: 'Đối tác', href: '#' }
  ];
}
