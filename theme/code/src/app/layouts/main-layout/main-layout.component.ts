import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router, NavigationEnd, ActivatedRoute, RouterModule } from '@angular/router';
import { filter } from 'rxjs/operators';
import { HeaderComponent } from '../../components/header/header.component';
import { FooterComponent } from '../../components/footer/footer.component';
import { OffcanvasComponent } from '../../components/offcanvas/offcanvas.component';
import { BackToTopComponent } from '../../components/back-to-top/back-to-top.component';
import { MouseCursorComponent } from '../../components/mouse-cursor/mouse-cursor.component';
import { SearchAreaComponent } from '../../components/search-area/search-area.component';
import { LightboxComponent } from '../../components/lightbox/lightbox.component';

@Component({
    selector: 'app-main-layout',
    standalone: true,
    imports: [
        CommonModule,
        RouterModule,
        HeaderComponent,
        FooterComponent,
        OffcanvasComponent,
        BackToTopComponent,
        MouseCursorComponent,
        SearchAreaComponent,
        LightboxComponent
    ],
    templateUrl: './main-layout.component.html',
    styleUrls: ['./main-layout.component.css']
})
export class MainLayoutComponent {
    footerStyle: 'style1' | 'style2' = 'style2';

    constructor(private router: Router, private route: ActivatedRoute) {
        this.router.events.pipe(
            filter(event => event instanceof NavigationEnd)
        ).subscribe(() => {
            let currentRoute = this.route;
            while (currentRoute.firstChild) {
                currentRoute = currentRoute.firstChild;
            }
            this.footerStyle = currentRoute.snapshot.data['footerStyle'] || 'style2';
        });
    }
}
