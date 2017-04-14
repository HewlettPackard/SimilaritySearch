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
define([
  'pages/page_view',
  'text!templates/pages/search.html'
], function(PageView, Template){
        var SearchView = PageView.extend({
                template: _.template( Template ),
                id:"search",
                onready:function(){
                        var _t = this;

                        _t.lshcomplete                  = false;
                        _t.ajax_queue                   = [];
                        _t.audiotrack_keys              = { "hadoop":0, "naive":1, "lsh":2 }
                        _t.selected_photo_el    = _t.$el.find( "div.selected-photo" )[0];
                        _t.hadoop_time_el               = _t.$el.find( "#hadoop .time-cost" ).eq(0);
                        _t.uuid                                 = randomString( 36, '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' );
                        _t.timers               = [];

                        if( _t.session_model.get( "selected_photo_url" ) ){
                                $( _t.selected_photo_el).css( "background-image", "url(" + _t.session_model.get( "selected_photo_url" ) + ")" );

                                _t.selected_photo_id = _t.session_model.get( "selected_file_id" );

                                if( _t.selected_photo_id ){
                                        _t.startHadoopTimer();

                                        _t.doSearch( "hadoop" );
                                        _t.timers.push( setTimeout( function(){ _t.doSearch( "naive" ) }, 500/*_t.presenter_mode() ? 500 : 10500*/ ));
                                        _t.timers.push( setTimeout( function(){ _t.doSearch( "lsh" ) }, 1000/*_t.presenter_mode() ? 1000 : 20500*/ ));
                                }
                        }
                },
                doSearch:function( _method ){
                        var _t                  = this,
                        search_el               = _t.$el.find("#" + _method).eq(0);

                        search_el.addClass( "visible" );

                        console.log("do search : ", app.routes.search[ _method ] + _t.selected_photo_id + "/" + ( _method == "hadoop" ? _t.uuid : "" ) );

                        _t.ajax_queue.push( $.ajax({
                        url: app.routes.search[ _method ] + _t.selected_photo_id + "/" + ( _method == "hadoop" ? _t.uuid : "" ) ,
                        method:"get",
                            cache: false,
                            contentType: false,
                            processData: false,
                        success:function( _data ){
                                console.log( "search success : ", _method, "," , _data );

                                _t.displayImageList( _data, _method, search_el );

                                if(_method == "hadoop"){
                                        _t.stopHadoopTimer();

                                        //_t.timers.push( setTimeout( function(){ changepage("wrapup") }, 10000 ));
                                }
                        },
                        error:function( _e )
                        {
                                console.log( "pull list Error: " );
                                console.log( _e );
                        }
                    }));

                        if( _t.presenter_mode() ){
                                if( _method == "lsh" ){
                                        _t.lshcomplete = true;
                                        _t.enableallnavigation();
                                }
                        } else {
                                //_t.timers.push( setTimeout( function(){ _t.audioplayers[ _t.audiotrack_keys[ _method ] ].play(); }, 10000 ));
                               /* _t.audioplayers[ _t.audiotrack_keys[ _method ] ].onended(function(){
                                        console.log("audio eneded: ", _method);

                                        if( _method == "lsh" ){
                                                _t.lshcomplete = true;
                                                _t.enableallnavigation();
                                        }
                                });*/

                             	//_t.audioplayers[ _t.audiotrack_keys[ _method ] ].play();
                             	
                        	
                                /*if( _method == "hadoop"){
                                        _t.audioplayers[ _t.audiotrack_keys[ _method ] ].play();
                                }else if ( _method == "naive"){
                                        _t.timers.push( setTimeout( function(){ _t.audioplayers[ _t.audiotrack_keys[ _method ] ].play(); }, 10000 ));
                                }else if( _method == "lsh" ){
                                        _t.timers.push( setTimeout( function(){ _t.audioplayers[ _t.audiotrack_keys[ _method ] ].play(); }, 20000 ));
                                }*/
                        	
                        		_t.audioplayers[0].play();
                        }
                },
                displayImageList:function( _data, _method, _search_el ) {
                        var _t                  = this,
                        search_ul               = _search_el.find("ul.image-list").eq(0),
                        time_el                 = _search_el.find(".time-cost").eq(0),
                        gear_el                 = _search_el.find(".gear").eq(0),
                        time                    = _data['results'][ _data['results'].length - 1 ]['time'];

                gear_el.fadeOut();
                search_ul.addClass("visible");
                search_ul.empty();

                //animate time
                if(_method == "hadoop"){
                        time_el.parent().addClass("time-done");
                } else {
                        time_el.jQuerySimpleCounter( { start:0, end:time, duration: 800 } );
                }

                //build images
                var imageList = _t.session_model.get( "image_list" );
                var iInit,iEnd = null;

                //If the first result img is the same as selected img then show results from 2-5
                if( _data['results'][0]['img'].split("/")[1] == _t.selected_photo_id){ iInit=1; iEnd = 5; }

                //If selected img is from upload (not from gallery) then show results from 1-4
                if(_t.session_model.get( "selected_photo_url" ).indexOf("data:image") > -1){ iInit=0; iEnd = 4;   }
                console.log("showing: " + iInit + "-" + iEnd);

                        for(var i = iInit; i < iEnd; i++) {
                                var filename = _data['results'][i]['img'].split("/")[1];

                                //create image li
                        var li                          = $('<li class="visible"></li>'),
                        inner                           = $('<div class="project-inner"></div>'),
                                a                                       = $('<a></a>');

                                li.append( inner );
                                inner.append( a );

                                //append li to image list ul
                                search_ul.append( li );


                                function getimage( _a, _filename ){
                                        _t.ajax_queue.push( $.ajax({
                                        url: app.routes.image_path + _filename,
                                        method:"get",
                                        success: function( _file_path ){
                                                console.log( "displayImageList getimage success: ", _file_path );

                                                _a.css( "background-image","url('" + _file_path + "')" );
                                        },
                                        error: function( _e )
                                        {
                                                console.log( "displayImageList getimage error: ", e );
                                        }
                                    }) );
                                }
                                getimage( a, filename );

                        }
                },
                startHadoopTimer:function(){
                        this.hadoop_time                = new Date().getTime();
                        this.hadoop_running     = true;
                },
                stopHadoopTimer:function(){
                        this.hadoop_running = false;
                },
                cancelHadoop:function(){
                        var _t = this;

                        //cancel ajax calls
                        $.each( _t.ajax_queue, function(){ this.abort(); });

                        _t.ajax_queue = [];

                        //don't queue this one. It must finish!
                        $.ajax({
                        url: app.routes.cancel_hadoop + _t.uuid,
                        method:"get",
                            cache: false,
                            contentType: false,
                            processData: false,
                        success:function( _data ){
                                console.log("cancel hadoop success: ", _data );
                        },
                        error: function( _e )
                        {
                                console.log( "cancel hadoop error: " );
                                console.log( _e );
                        }
                    });

                    _t.stopHadoopTimer();
                },
                onstep:function(){
                        if(this.hadoop_running){
                                this.hadoop_time_el.html( new Date().getTime()-this.hadoop_time );
                        }
                },
                onnavbuttonclicked:function( _pageid ){
                if( _pageid == "wrapup" && !this.lshcomplete )
                                return;

                        changepage( _pageid );
                },
                onclose:function(){
                        var _t = this;

                        //cancel timers
                        $.each( _t.timers, function(){ clearTimeout(this); });

                        //cancel hadoop;
                        this.cancelHadoop();
                },
        });
        return SearchView;
});
