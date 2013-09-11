var mongoose = require('mongoose') ;
var db = mongoose.connect( 'mongodb://localhost:27017/libTest');
var Persistence = require('./index') ;
var _ = require('underscore') ;
var async = require('async') ;
var client = require('redis').createClient();
var persistence = new Persistence({ cacheFields : ['user_number','_id'] , modelName : 'User' , client : client }) ;
var UserSchema = mongoose.Schema({
	email : { type : String , unique : true , sparse : true , required : false } ,
  user_number : { type : Number , unique : true , required : true }
});

UserSchema.plugin( persistence.plugin() );

var User = db.model('User',UserSchema) ;


//User.updateWithCacheById( '522f964d6ba5e2af7a000001' , {email : 'gabbo'} , function(){console.log(arguments)} )

//User.getFromCacheByIds(['522f964d6ba5e2af7a000001','522f92bef2a493b979000001'],['email'],{lean:true},function(){console.log(arguments)})

var a = new User({
  email : 'foo1@ba1sdr.com'+_.random(0,1000),
  user_number : 12341567+_.random(0,1000)
})

var ids = ["522fda6488f72a388e000001"];

a.saveWithCache(function(err,a){ 
  
  ids.push(a._id);
  
  User.getFromCacheByIds(ids,null,function(err,user){
  
    console.log(arguments);
  
  });
  
});


