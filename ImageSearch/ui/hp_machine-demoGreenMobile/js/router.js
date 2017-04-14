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
  'backbone',
  'collections/page_collection',
  //  'pages/attract_view',
  'pages/intro_view',
  'pages/photo_view',
  'pages/search_view',
  /*'pages/wrapup_view',*/
  'pages/statistics_view',
  'pages/thanks_view',
  'models/session_model'
        ], function (Backbone, PageCollection, /*AttractView,*/ IntroView, PhotoView, SearchView,/* WrapupView,*/ StatisticsView, ThanksView, SessionModel){
  var Router   = Backbone.Router.extend({
    initialize:function(){
      var _t = this;

      _t.page_collection  = new PageCollection();
      _t.session_model    = new SessionModel();

      _t.session_model.set( "presenter_mode", false );

      _t.page_views = [
        //new AttractView({ collection:_t.page_collection, session_model:_t.session_model }),
        new IntroView({ collection:_t.page_collection, session_model:_t.session_model }),
        new PhotoView({ collection:_t.page_collection, session_model:_t.session_model }),
        new SearchView({ collection:_t.page_collection, session_model:_t.session_model }),
        //new WrapupView({ collection:_t.page_collection, session_model:_t.session_model }),
         new StatisticsView({ collection:_t.page_collection, session_model:_t.session_model }),
         new ThanksView({ collection:_t.page_collection, session_model:_t.session_model })
      ];

      _t.register_container_el  = $( "div#register-container" );
      _t.email_message_el       = _t.register_container_el.find( "span.message" ).eq(0);
      _t.email_form_el          = _t.register_container_el.find( "form#register-form" ).eq(0);
      _t.email_input_el         = _t.register_container_el.find( "input.email" ).eq(0);

      _t.email_form_el.submit( function(e){
        e.preventDefault();

        var _email_val = _t.email_input_el.val();

        //validate form
        _t.email_input_el.removeClass("error");

        if( _email_val && _email_val != "" && validateemail(_email_val) ){
          _t.registeruser( _email_val );
        } else {
          _t.email_input_el.addClass("error");
        }
      });

      $(window).resize(function doresize(e){
        didresize = true;
      });

	  if(_t.checkCookie())
			_t.onchangepage("photo");
	  else
		  _t.onchangepage("intro");
    },
    registeruser:function( _email ){
      var _t = this;

      _t.register_container_el.removeClass();
      _t.register_container_el.addClass("sending");

      var data = {
        "email" : _email
      };

      $.ajax({
          url: app.routes.register,
          method:"post",
          cache: false,
          contentType: 'application/json',
          processData: false,
          data:JSON.stringify( data ),
          success:function( _response ){
            console.log( "register user success: ", _response );

            _t.register_container_el.removeClass();
            _t.register_container_el.addClass("success");
            _t.email_message_el.html("Thank you for registering!");
          },
          error: function( _e )
          {
            console.log( "register error: " );
            console.log( _e );

            _t.register_container_el.addClass("error");
            _t.email_message_el.html("Sorry, please try again later");
          }
      });

      //reset form
      if(_t.resetform_timeout) clearTimeout(_t.resetform_timeout);

      _t.resetform_timeout = setTimeout(function resetregisterform(){
        _t.register_container_el.removeClass();
        _t.email_message_el.html("Register for updates");
        _t.email_input_el.val("");
      }, 4000);
    },
	checkCookie:function(){
		//If cookie doesn't exist, return false to show Privacy Agreement page
		if(this.session_model.getCookie("data") === "" || !this.session_model.getPrivacyAgreement())
			return false;
		else if(this.session_model.getPrivacyAgreement())
			return true;
	},
    onchangepage:function(_pageid){
      !_pageid ? _pageid = "photo" : null;

      $("html,body").scrollTop(0);

      this.page_collection.activatePageById( _pageid );

      if( firstpage ) firstpage = false;
    }
  });

  return Router;
});
