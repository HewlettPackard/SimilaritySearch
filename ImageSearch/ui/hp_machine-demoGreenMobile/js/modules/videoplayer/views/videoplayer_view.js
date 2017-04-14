define([
        'backbone'
], function( Backbone ){
    var VideoPlayerView = Backbone.View.extend({
        initialize:function(){
            var _t = this;

            _t.poster         = _t.$el.find( "div.cfm-videoplayer-poster" )[0];
            _t.video          = _t.$el.find( "video.cfm-videoplayer-desktop" )[0];
            _t.mobile_video   = _t.$el.find( "video.cfm-videoplayer-mobile" )[0];

            _t.model          = new Backbone.Model( { ready:false } );

            _t.model.on( "change:ready", function( _model ){
                if( _model.get( "ready" ) == true ){
                    if( !_t.$el.hasClass( "ready" ) )
                        _t.$el.addClass( "ready" );
                } else {
                    _t.$el.removeClass( "ready" );
                }
            });

            _t.video_url        = _t.$el.attr("data-video");
            _t.poster_url       = _t.$el.attr("data-poster");
            _t.video_type       = mp4 ? "mp4" : "webm";
            _t.video_width      = _t.$el.attr("data-width");
            _t.video_height     = _t.$el.attr("data-height");
            _t.$el.css({
                "width":_t.video_width + "px",
                "height":_t.video_height + "px",
                "margin-top":-_t.video_height*.5 + "px",
                "margin-left":-_t.video_width*.5 + "px"
            });

            _t.load( _t.video_url, _t.video_type, _t.poster_url );
        },
        play:function(){
            if( mobile == false ) this.video.play();
                else this.mobile_video.play();
        },
        pause:function(){
            if( mobile == true ) this.mobile_video.pause();
            else this.video.pause();
        },
        load:function( _url, _type, _poster ){

            var _t = this;

            _url += ( "." + _type );

            if( mobile == true ){
                $( _t.video ).remove();

                if( _type ) $( _t.mobile_video ).attr( "type", "video/" + _type );
                if( _url ) $( _t.mobile_video ).attr( "src", _url );

                $( _t.mobile_video ).on( "play", function(){
                    $( _t.mobile_video ).css( "opacity", 1 );

                });

                if( iphone ){
                    $( _t.mobile_video ).on( "pause", function(){
                        $( _t.mobile_video ).css( "opacity",0 );
                    });
                }
                _t.play();
            } else {
                $( _t.mobile_video ).remove();

                if( _type ) $( _t.video ).attr( "type", "video/" + _type );
                if( _url ) $( _t.video ).attr( "src", _url );

                $( _t.video ).on( "play", function(){
                    $( _t.poster ).fadeOut( 200 );
                });

                _t.play();
            }

            if( _poster ) _t.loadposter( _poster );
        },
        onended:function(_callback){
            this.video.onended = _callback;
        },
        loadposter:function( _url ){
            var _t = this, img = new Image();

            img.onload = function(){
                $( _t.poster ).attr( "style", "background-image:url(" + _url + ")" );

                _t.model.set( "ready", true );

                $(_t.poster).click( function(){
                    _t.play();
                });
            }

            img.src = _url;

            _t.resize( $(window).width(), $(window).height() );
        },
        resize:function(_width, _height){
            var _t = this;

            var scale = _width/_t.video_width;
            if( (_t.video_height*scale) < _height ) scale = _height/_t.video_height;

            _t.$el.css({
                "transform":"scale("+scale+")"
            });
        }
    });

    return VideoPlayerView;
});
