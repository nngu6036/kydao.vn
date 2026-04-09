(function ($) {
    "use strict";

    $(document).ready(function () {


        /*-----------------------------------
          Mobile Menu  
        -----------------------------------*/
        $('#mobile-menu').meanmenu({
            meanMenuContainer: '.mobile-menu',
            meanScreenWidth: "1199",
            meanExpand: ['<i class="fas fa-angle-right"></i>'],
        });



        /*-----------------------------------
         Sidebar Toggle  
        -----------------------------------*/
        $(".offcanvas__close,.offcanvas__overlay").on("click", function () {
            $(".offcanvas__info").removeClass("info-open");
            $(".offcanvas__overlay").removeClass("overlay-open");
        });
        $(".sidebar__toggle").on("click", function () {
            $(".offcanvas__info").addClass("info-open");
            $(".offcanvas__overlay").addClass("overlay-open");
        });



        /*-----------------------------------
         Body Overlay 
        -----------------------------------*/
        $(".body-overlay").on("click", function () {
            $(".offcanvas__area").removeClass("offcanvas-opened");
            $(".df-search-area").removeClass("opened");;
            $(".body-overlay").removeClass("opened");
        });



        /*-----------------------------------
          Sticky Header 
        -----------------------------------*/
        $(window).scroll(function () {
            if ($(this).scrollTop() > 250) {
                $("#header-sticky").addClass("sticky");
            } else {
                $("#header-sticky").removeClass("sticky");
            }

        });



        /*-----------------------------------
         Sticky Header Two
        -----------------------------------*/

        var windowOn = $(window);

        windowOn.on('scroll', function () {
            var scroll = windowOn.scrollTop();
            if (scroll > 100) {
                $('#ct-header-sticky').addClass("header-sticky");
            } else {
                $('#ct-header-sticky').removeClass("header-sticky");
            }
        });




        /*-----------------------------------
           Set Background Image & Mask   
        -----------------------------------*/
        if ($("[data-bg-src]").length > 0) {
            $("[data-bg-src]").each(function () {
                var src = $(this).attr("data-bg-src");
                $(this).css("background-image", "url(" + src + ")");
                $(this).removeAttr("data-bg-src").addClass("background-image");
            });
        }


        if ($('[data-mask-src]').length > 0) {
            $('[data-mask-src]').each(function () {
                var mask = $(this).attr('data-mask-src');
                $(this).css({
                    'mask-image': 'url(' + mask + ')',
                    '-webkit-mask-image': 'url(' + mask + ')'
                });
                $(this).addClass('bg-mask');
                $(this).removeAttr('data-mask-src');
            });
        };



        /*-----------------------------------
        Global Slider
        -----------------------------------*/
        function applyAnimationProperties() {
            $('[data-ani]').each(function () {
                var animationClass = $(this).data('ani');
                $(this).addClass(animationClass);
            });

            $('[data-ani-delay]').each(function () {
                var delay = $(this).data('ani-delay');
                $(this).css('animation-delay', delay);
            });
        }

        // Call the animation properties function
        applyAnimationProperties();

        // Function to initialize Swiper
        function initializeSwiper(sliderContainer) {
            var sliderOptions = sliderContainer.data('slider-options');

            console.log("Slider options: ", sliderOptions);

            var previousArrow = sliderContainer.find('.slider-prev');
            var nextArrow = sliderContainer.find('.slider-next');
            var paginationElement = sliderContainer.find('.slider-pagination');
            var numberedPagination = sliderContainer.find('.slider-pagination.pagi-number');

            var paginationStyle = sliderOptions['paginationType'] || 'bullets';
            var autoplaySettings = sliderOptions['autoplay'] || {
                delay: 6000,
                disableOnInteraction: false
            };

            var defaultSwiperConfig = {
                slidesPerView: 1,
                spaceBetween: sliderOptions['spaceBetween'] || 24,
                loop: sliderOptions['loop'] !== false,
                speed: sliderOptions['speed'] || 1000,
                initialSlide: sliderOptions['initialSlide'] || 0,
                centeredSlides: !!sliderOptions['centeredSlides'],
                effect: sliderOptions['effect'] || 'slide',
                fadeEffect: {
                    crossFade: true
                },
                autoplay: autoplaySettings,
                navigation: {
                    nextEl: nextArrow.length ? nextArrow.get(0) : null,
                    prevEl: previousArrow.length ? previousArrow.get(0) : null,
                },
                pagination: {
                    el: paginationElement.length ? paginationElement.get(0) : null,
                    type: paginationStyle,
                    clickable: true,
                    renderBullet: function (index, className) {
                        var bulletNumber = index + 1;
                        var formattedNumber = bulletNumber < 10 ? '0' + bulletNumber : bulletNumber;
                        if (numberedPagination.length) {
                            return '<span class="' + className + ' number">' + formattedNumber + '</span>';
                        } else {
                            return '<span class="' + className + '" aria-label="Go to Slide ' + formattedNumber + '"></span>';
                        }
                    },
                },
                on: {
                    slideChange: function () {
                        setTimeout(function () {
                            this.params.mousewheel.releaseOnEdges = false;
                        }.bind(this), 500);
                    },
                    reachEnd: function () {
                        setTimeout(function () {
                            this.params.mousewheel.releaseOnEdges = true;
                        }.bind(this), 750);
                    }
                }
            };

            var finalConfig = $.extend({}, defaultSwiperConfig, sliderOptions);
            console.log("Complete Swiper options: ", finalConfig);

            // Initialize the Swiper instance
            return new Swiper(sliderContainer.get(0), finalConfig);
        }

        // Initialize Swipers on page load
        var swiperInstances = [];
        $('.ct-slider').each(function () {
            var sliderContainer = $(this);
            var swiperInstance = initializeSwiper(sliderContainer);
            swiperInstances.push(swiperInstance);
        });


        // Bootstrap tab show event
        $('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
            var targetTab = $(e.target).attr('href');
            $(targetTab).find('.et-slider').each(function () {
                var sliderContainer = $(this);
                if (!sliderContainer[0].swiper) {
                    initializeSwiper(sliderContainer);
                } else {
                    sliderContainer[0].swiper.update();
                }
            });
        });

        // Add click event handlers for external slider arrows based on data attributes
        $('[data-slider-prev], [data-slider-next]').on('click', function () {
            var targetSliderSelector = $(this).data('slider-prev') || $(this).data('slider-next');
            var targetSlider = $(targetSliderSelector);

            if (targetSlider.length) {
                var swiper = targetSlider[0].swiper;

                if (swiper) {
                    if ($(this).data('slider-prev')) {
                        swiper.slidePrev();
                    } else {
                        swiper.slideNext();
                    }
                }
            }
        });



        /*-----------------------------------
            Back to top    
        -----------------------------------*/
        $(window).on("scroll", function () {
            if ($(this).scrollTop() > 20) {
                $("#back-top").addClass("show");
            } else {
                $("#back-top").removeClass("show");
            }
        });

        $(document).on("click", "#back-top", function () {
            $("html, body").animate({ scrollTop: 0 }, 800);
            return false;
        })



        /*-----------------------------------
             MagnificPopup  view    
        -----------------------------------*/
        $(".popup-video").magnificPopup({
            type: "iframe",
            removalDelay: 260,
            mainClass: 'mfp-zoom-in',
        });

        $(".img-popup").magnificPopup({
            type: "image",
            gallery: {
                enabled: true,
            },
        });




        /*-----------------------------------
             NiceSelect     
        -----------------------------------*/
        if ($('.single-select').length) {
            $('.single-select').niceSelect();
        }



        /*-----------------------------------
           Mouse Cursor    
        -----------------------------------*/
        function mousecursor() {
            if ($("body")) {
                const e = document.querySelector(".cursor-inner"),
                    t = document.querySelector(".cursor-outer");
                let n,
                    i = 0,
                    o = !1;
                (window.onmousemove = function (s) {
                    o ||
                        (t.style.transform =
                            "translate(" + s.clientX + "px, " + s.clientY + "px)"),
                        (e.style.transform =
                            "translate(" + s.clientX + "px, " + s.clientY + "px)"),
                        (n = s.clientY),
                        (i = s.clientX);
                }),
                    $("body").on("mouseenter", "a, .cursor-pointer", function () {
                        e.classList.add("cursor-hover");
                        t.classList.add("cursor-hover");
                    }),
                    $("body").on("mouseleave", "a, .cursor-pointer", function () {
                        ($(this).is("a") && $(this).closest(".cursor-pointer").length) ||
                            (e.classList.remove("cursor-hover"),
                                t.classList.remove("cursor-hover"));
                    }),
                    (e.style.visibility = "visible"),
                    (t.style.visibility = "visible");
            }
        }
        $(function () {
            mousecursor();
        });



        /*-----------------------------------
            Progress Bar   
        -----------------------------------*/
        $('.progress-bar').each(function () {
            var $this = $(this);
            var progressWidth = $this.attr('style').match(/width:\s*(\d+)%/)[1] + '%';

            $this.waypoint(function () {
                $this.css({
                    '--progress-width': progressWidth,
                    'animation': 'animate-positive 1.8s forwards',
                    'opacity': '1'
                });
            }, { offset: '75%' });
        });



        /*--------------------------------------------------
          Search Popup
      ---------------------------------------------------*/
        const $searchWrap = $(".search-wrap");
        const $navSearch = $(".nav-search");
        const $searchClose = $("#search-close");

        $(".search-trigger").on("click", function (e) {
            e.preventDefault();
            $searchWrap.animate({ opacity: "toggle" }, 500);
            $navSearch.add($searchClose).addClass("open");
        });

        $(".search-close").on("click", function (e) {
            e.preventDefault();
            $searchWrap.animate({ opacity: "toggle" }, 500);
            $navSearch.add($searchClose).removeClass("open");
        });

        function closeSearch() {
            $searchWrap.fadeOut(200);
            $navSearch.add($searchClose).removeClass("open");
        }
        $(document.body).on("click", function (e) {
            closeSearch();
        });

        $(".search-trigger, .main-search-input").on("click", function (e) {
            e.stopPropagation();
        });



        // 1) Disable right-click context menu
        $(document).on("contextmenu", function (e) {
            e.preventDefault();
            return false;
        });

        // 2) Disable drag/drop (reduces easy image dragging)
        $(document).on("dragstart drop", function (e) {
            e.preventDefault();
            return false;
        });

        // 3) Block common key combos: Ctrl+U, Ctrl+S, Ctrl+P, Ctrl+Shift+I/J/C, F12
        $(document).on("keydown", function (e) {
            const key = (e.key || "").toLowerCase();

            // F12
            if (e.keyCode === 123) {
                e.preventDefault();
                return false;
            }

            // Ctrl/Meta combos
            const ctrlOrCmd = e.ctrlKey || e.metaKey;

            if (ctrlOrCmd) {
                // Ctrl+U (view source), Ctrl+S (save), Ctrl+P (print)
                if (key === "u" || key === "s" || key === "p") {
                    e.preventDefault();
                    return false;
                }

                // Ctrl+Shift+I/J/C (DevTools open shortcuts)
                if (e.shiftKey && (key === "i" || key === "j" || key === "c")) {
                    e.preventDefault();
                    return false;
                }
            }

            return true;
        });

        // 4) Basic DevTools "detection" (not reliable, but discourages)
        // If devtools opens, trigger an overlay or redirect.
        let devtoolsOpen = false;

        function devtoolsCheck() {
            const threshold = 160; // common heuristic
            const widthDiff = window.outerWidth - window.innerWidth;
            const heightDiff = window.outerHeight - window.innerHeight;

            const opened = (widthDiff > threshold) || (heightDiff > threshold);

            if (opened && !devtoolsOpen) {
                devtoolsOpen = true;

                // Choose ONE action:
                // A) show overlay
                showBlockOverlay();

                // B) or redirect (uncomment)
                // window.location.href = "/";
            }

            if (!opened && devtoolsOpen) {
                devtoolsOpen = false;
                removeBlockOverlay();
            }
        }

        function showBlockOverlay() {
            if ($("#antiInspectOverlay").length) return;

            const overlay = `
      <div id="antiInspectOverlay"
           style="position:fixed;inset:0;z-index:2147483647;
                  background:rgba(0,0,0,.85);color:#fff;
                  display:flex;align-items:center;justify-content:center;
                  font:16px/1.4 Arial;padding:24px;text-align:center;">
        <div>
          <div style="font-size:20px;margin-bottom:10px;">Access Restricted</div>
          <div>Developer tools are disabled on this page.</div>
        </div>
      </div>
    `;
            $("body").append(overlay);
        }

        function removeBlockOverlay() {
            $("#antiInspectOverlay").remove();
        }

        // run check periodically
        setInterval(devtoolsCheck, 800);


    }); // End Document Ready Function   



})(jQuery); // End jQuery

