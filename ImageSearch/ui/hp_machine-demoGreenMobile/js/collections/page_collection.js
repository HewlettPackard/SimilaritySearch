define([
  'backbone',
  'models/page_model'
], function (Backbone, PageModel) {
  var PageCollection = Backbone.Collection.extend({
    sidebar_is_open:false,
    subnav_is_open:false,
    initialize:function(){
      var _t = this;
    },
    activatePageById:function(_id){
      console.log("activatePageById: ", _id);
      
  		var _m = this.get(_id);
  		if(!_m) return;

      _active = this.where({active:true});

      /*--- silently deactivate curretly active model(s) -----
      -------------------------------------------------------*/
      _.each(_active, function(_model){
         _model.set({"active":false, "silent":true});
      });
      
      /*---- remove silent and activate selected model -------
      -------------------------------------------------------*/
      _m.set({"active":true, "silent":false});
    }
  });

  return PageCollection;
});