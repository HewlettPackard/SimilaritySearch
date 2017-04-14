define([
  'backbone', ], function (Backbone) {
  var SessionModel = Backbone.Model.extend({
    selected_photo_url:null,
    selected_file_id: null,
    uploaded_file_id: null,
    data: null,
    exdays: 3,//expiration time in days
    initialize:function(){
      if(this.getCookie("data"))
        this.data=JSON.parse(document.cookie.split('data=')[1].split(';')[0]);
      else
        this.data= {};
    },
    getCookie(cname) {
        var name = cname + "=";
        var ca = document.cookie.split(';');
        for(var i=0; i<ca.length; i++) {
                var c = ca[i];
                while (c.charAt(0)==' ') c = c.substring(1);
                if (c.indexOf(name) == 0) return c.substring(name.length,c.length);
        }
        return "";
    },
    getPrivacyAgreement: function() {console.log(this.data)
        return this.data.privacyAgreement;
    },
    setPrivacyAgreement: function(value) {
        this.data.privacyAgreement= value;
        var d = new Date();
        d.setTime(d.getTime() + (this.exdays*24*60*60*1000));
        var expires = "expires="+d.toUTCString();
        document.cookie= "data="+JSON.stringify(this.data)+";"+expires+";path=/";
    }
  });
  return SessionModel;
});
