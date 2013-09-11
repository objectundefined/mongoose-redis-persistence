var mongoose = require('mongoose') ;
var db = mongoose.connect( 'mongodb://localhost:27017/libTest');
var Persistence = require('./index') ;
var _ = require('underscore') ;
var async = require('async') ;
var client = require('redis').createClient();
var persistence = new Persistence({ cacheFields : ['user_number','_id','not_unique'] , modelName : 'User' , client : client }) ;
var UserSchema = mongoose.Schema({
	email : { type : String , unique : true , sparse : true , required : false } ,
  user_number : { type : Number , unique : true , required : true },
  not_unique : { type : String }
});

UserSchema.plugin( persistence.plugin() );

var User = db.model('User',UserSchema) ;


User.updateWithCacheById( '522f964d6ba5e2af7a000001' , {email : 'gabbbo'} , function(){console.log(arguments)} )

//User.getFromCacheByIds(['522f964d6ba5e2af7a000001','522f92bef2a493b979000001'],['email'],{lean:true},function(){console.log(arguments)})

/*

a = new User({'email':'foo1@ba1sdr.com'+_.random(0,1000),'not_unique':'bar','user_number':12341567+_.random(0,1000)})

a.save(function(){ 
  
  
  // returns many
  
//  User.findCachedByNotUnique('bar',console.log);
  
  
  // returns one
  
  User.findCachedById('522f913e0ecf7e7679000001',function(err,user){
    
    user.saveWithCache(function(){
      
      console.log(arguments);
      
    })
    
  });
  
});
*/
