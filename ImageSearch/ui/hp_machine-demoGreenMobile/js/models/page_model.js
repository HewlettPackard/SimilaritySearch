define([
  'backbone',
], function (Backbone) {
  var PageModel = Backbone.Model.extend({
  	defaults: {
	    "active": false,
      "silent": false
	},
  initialize:function(){
  }
  });

  return PageModel;
});