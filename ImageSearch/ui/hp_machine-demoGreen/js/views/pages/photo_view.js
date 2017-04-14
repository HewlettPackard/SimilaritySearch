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
*/define([
  'pages/page_view',
  'text!templates/pages/photo.html'
], function(PageView, Template){
        var PhotoView = PageView.extend({
                template: _.template( Template ),
                id:"photo",
                canvas_data:null,
                onready:function(){
                        var _t = this;

                        _t.imageselected                                        = false;

                        //html elements
                        _t.file_input                                   = _t.$el.find("input[type=file]")[0];
                        _t.selected_photo_container     = _t.$el.find(".selected-photo-container")[0];
                        _t.selected_photo                               = _t.$el.find("div.selected-photo")[0];
                        _t.canvas                                               = _t.$el.find("canvas")[0];


                        //jquery objects
                        _t.library_list                                 = _t.$el.find("#library-list").eq(0);
                        _t.more_button                                  = _t.$el.find("#more-image-btn").eq(0);
						_t.submit_btn                                 = _t.$el.find("#submit").eq(0);
						_t.cancel_btn                                 = _t.$el.find("#cancel").eq(0);
						_t.choose_file_btn                                 = _t.$el.find("#choose-file-btn")[0];

                       
                        //Check cookie, if false then disable Choose File btn
						if(!this.session_model.getPrivacyAgreement()){
							//Disable upload button
							_t.file_input.disabled = true;
							_t.choose_file_btn.className += " disabled";
						}

                        //file input change hadler handler
                        _t.file_input.addEventListener('change', function(e){
                                                        _t.resizeimage( e.target.files[0] );
                        });

                        //to keep last uploaded image filename
                        _t.filename = "";

                        //uploaded image click to reselect
                        _t.canvas.addEventListener('click', function(e) {
                                if(_t.filename !== ""){
                                        //add outline to uploaded photo
                                        $("#upload-photo-container").addClass('selected');

                                        //remove outline from gallery img if there is one selected
                                        if(_t.library_list.find("li").hasClass( "selected" )){
                                                _t.library_list.find("li").removeClass("selected");
                                        }
                                        console.log("selected uploaded image: " + _t.filename);
                                        _t.setselectedimage( _t.canvas_data, _t.filename );
                                }
                        });

                        //more button click handler
                        _t.more_button.click(function(){
                                                        _t.imagelist_page = _t.imagelist_page + 4; if(_t.imagelist_page > _t.library_list.find("li").length - 4) _t.imagelist_page = 0;

                                                        _t.library_list.find("li").removeClass('visible');
                                                        _t.library_list.find("li").slice(_t.imagelist_page, _t.imagelist_page + 4).addClass('visible');
                                                });

                _t.getImageList();

                _t.audioplayers[0].play();
                },
                resizeimage:function(file){
                        var _t = this;

                        var img = new Image(), reader = new FileReader();

                        img.onload = function(e){
                                var ctx = _t.canvas.getContext("2d");
                        var sourceSize = img.width > img.height ? img.height : img.width;
                        var destX = (img.width-sourceSize)*.5;
                        var destY = (img.height-sourceSize)*.5;

                                ctx.drawImage(img, destX, destY, sourceSize, sourceSize, 0, 0, 500, 500);

                                _t.canvas_data = _t.canvas.toDataURL( "image/jpeg" );
                                _t.uploadImage();



                        }

                        reader.onload = function(e){
                                img.src = e.target.result;
                        }

                        reader.readAsDataURL(file);
                },
                uploadImage:function() {
                        var _t = this;

                        var data = {
                                "name": "myImage.jpg",
                                "src" : _t.canvas_data
                        };

                        $.ajax({
                        url: app.routes.image_upload,
                        method:"post",
                            cache: false,
                            contentType: 'application/json',
                            processData: false,
                        data:JSON.stringify( data ),
                        success:function( _response ){
                                _t.filename = _response['filename'].slice(0, -4);
                                //_t.filename = filename;
                                console.log( "upload image success: ", _response, _t.filename );

                                //add outline to uploaded photo
                                $("#upload-photo-container").addClass('selected')

                                //remove outline from gallery img if there is one selected
                                if(_t.library_list.find("li").hasClass( "selected" )){
                                        _t.library_list.find("li").removeClass("selected");
                                }

                                _t.setselectedimage( _t.canvas_data, _t.filename );
                        },
                        error: function( _e )
                        {
                                console.log( "upload image error: " );
                                console.log( _e );
                        }
                    });
                },
                setselectedimage:function( _url, _id ){
                        this.session_model.set( "selected_photo_url", _url );
                        this.session_model.set( "selected_file_id", _id );

                        this.imageselected = true;

                        this.enableallnavigation();
                },
                getImageList: function() {
                        var _t = this;

                        $.ajax({
                        url: app.routes.image_list,
                        method:"get",
                        success: function( _response ){
                                _t.imagelist_page = 0;

                                //todo:fix this
                                $('.gear').fadeOut();

                                _t.buildImageList( _response );

                                _t.library_list = _t.$el.find("#library-list").eq(0);

                                //todo:fix this
                                _t.more_button.css("opacity",1);
                        },
                        error:function( _e )
                        {
                                console.log( "getImageList error " );
                                console.log( _e );
                        }
                    });
                },
                buildImageList:function( _data ){
                        var _t = this;

                        for (i = 0; i < _data.length; i++) {


                                function getfilepath(_i, _filename){
                                        $.ajax({
                                        url: app.routes.image_path + _filename,
                                        method:"get",
                                        success: function( _response ){
                                                var li = $('<li data-filepath="' + _response + '" data-filename="' + _filename + '"></li>'),
                                                inner = $('<div class="project-inner"></div>'),
                                                        a = $('<a style="background-image: url(' + _response + ')"></a>');

                                                        inner.append(a);
                                                        li.append(inner);
                                                        _t.library_list.append(li);

                                                        li.click( function doimagelistimageselected(){ _t.imageListImageSelected( $(this) ); } );

                                                        if(_i < 4) li.addClass("visible");
                                        },
                                        error:function( _e )
                                        {
                                                console.log( "buildImageList getfilepath error: " );
                                                console.log( _e );
                                        }
                                    });
                                }
                                getfilepath( i, _data[i].split("/")[1].slice(0,-4) );

                        }
                },
                imageListImageSelected:function( _li ){
                        var _t = this;

                _t.library_list.find("li").removeClass('selected');
                _li.addClass('selected');

                //remove outline from uploaded img if there is one selected
                if( $("#upload-photo-container").hasClass('selected')){
                        $("#upload-photo-container").removeClass("selected");
                }


                filepath = _li.data('filepath');
                filename = _li.data('filename');

                console.log("image list selected", filename, filepath);

                _t.setselectedimage( filepath, filename );
            },
            onnavbuttonclicked:function( _pageid ){
                if( _pageid == "search" && !this.imageselected )
                                return;

                        changepage( _pageid );
                },
                onclose:function(){
                },
        });
        return PhotoView;
});
