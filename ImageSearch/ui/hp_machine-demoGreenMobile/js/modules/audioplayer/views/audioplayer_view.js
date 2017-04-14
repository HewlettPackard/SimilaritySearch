define([
	'backbone'
], function( Backbone ){
    var AudioPlayerView = Backbone.View.extend({
        initialize:function(){
            var _t              = this;

            _t.audio            = _t.$el.find( "audio" )[0];
            _t.audio_type       = mp4 ? "mp3" : "ogg";
            _t.audio_url        = _t.$el.attr( "data-audio" ) + ( "." + _t.audio_type );

            $( _t.audio ).attr( "type", "audio/" + _t.audio_type );
            $( _t.audio ).attr( "src", _t.audio_url );
        },
        onended:function(_callback){
            this.audio.onended = _callback;
        },
        play:function(){
            this.audio.play();
        },
        pause:function(){
            this.audio.pause();
        }
    });

    return AudioPlayerView;
});