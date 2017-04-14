/* Similarity Search
“© Copyright 2017  Hewlett Packard Enterprise Development LP

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.”
*/
'use strict';

var router,changepage,pushstate=false,mobile=false,retina=false,mp4=false,ipad=false,iphone=false,iphone34=false,ie=false,ie8=false,android=false,firstpage = true,didresize = false;

require.config({
    baseUrl: "",
    paths: {
        jquery:         'js/vendor/jquery.min',
        backbone:       'js/vendor/backbone.min',
        underscore:     'js/vendor/underscore.min',
        text:           'js/vendor/text.min',
        router:         'js/router',
        collections:    'js/collections',
        pages:          'js/views/pages',
        modules:        'js/modules',
        models:         'js/models'
    }
});

require([
    'jquery',
    'underscore',
    'router'
], function( $, _, Router ) {
    $(document).ready(function(){
        /*----- user agent ------*/
        var uagent = navigator.userAgent.toLowerCase(),body = document.body,
        mobile_search = [ "iphone","ipod","series60","symbian","android","windows ce","windows7phone","w7p","blackberry","palm" ];

        /*--------mobile---------*/
        for(var i in mobile_search){
            if( uagent.search( mobile_search[i] ) > -1 ){
                mobile = true;
                console.log("is mobile");
                
				window.location.replace("https://www.similaritysearch.labs.hpe.com/ExtremeSearchGreenMobile");
                break;
            }
        }

        /*--------retina---------*/
        retina = mobile && window.devicePixelRatio > 1;

        /*--------pushstate---------*/
        pushstate = !!(window.history && window.history.pushState);
        pushstate = false;

        /*--------mp4---------*/
        mp4 = ( Modernizr.video && document.createElement('video').canPlayType('video/mp4; codecs=avc1.42E01E,mp4a.40.2') );

        /*--------ie,ie8---------*/
        if( uagent.search( "msie" ) > -1 ) ie = true;
        ie8 = $("body").hasClass("ie8");

        /*--------ipad,iphone---------*/
        if( uagent.search( "ipad" ) > -1 ) ipad = true;
        if( uagent.search( "iphone" ) > -1 ) iphone = true;
        if( uagent.search( "android" ) > -1 ) android = true;

        if( iphone && ( (window.screen.height == 480 && window.screen.width == 320) || (window.screen.width == 480 && window.screen.height == 320) ) )
            iphone34 = true;

        /*-------- set body tags ---------*/
        if(mobile) body.className += " mobile";
        if(pushstate) body.className += " pushstate";
        if(retina) body.className += " retina";
        if(mp4) body.className += " mp4";
        if(ipad) body.className += " ipad";
        if(iphone) body.className += " iphone";
        if(iphone34) body.className += " iphone34";
        if(android) body.className += " android";
        if(ie) body.className += " ie";
        if(debug) body.className += " debug";

        //jquery plugins
        $.fn.jQuerySimpleCounter = function( options ) {
            var settings = $.extend({
                start:  0,
                end:    216000,
                easing: 'swing',
                duration: 2500,
                complete: ''
            }, options );

            var thisElement = $(this);

            $({count: settings.start}).animate({count: settings.end}, {
                duration: settings.duration,
                easing: settings.easing,
                step: function() {
                    var mathCount = Math.ceil(this.count);
                    thisElement.text(mathCount);
                },
                complete: function() {
                    settings.complete;
                    thisElement.parent().addClass('time-done');
                }
            });
        };

        /*----- init router ------*/
        router = new Router();

        changepage = function(_pageid){
            router.onchangepage(_pageid);
        }
    });
});
