var mongoose = require('mongoose') ;
var db = mongoose.connect( 'mongodb://localhost:27017/libTest');
var Persistence = require('./index') ;
var _ = require('underscore') ;
var persistence = new Persistence({ cacheFields : ['user_number','_id','not_unique'] , modelName : 'User' }) ;
var UserSchema = mongoose.Schema({
	email : { type : String , unique : true , sparse : true , required : false } ,
  user_number : { type : Number , unique : true , required : true },
  not_unique : { type : String }
});

UserSchema.plugin( persistence.plugin() );

// new persistence.Query( fieldName , findMany ) ; // find

UserSchema.statics.findCachedById = persistence.QueryField( '_id' ); 

UserSchema.statics.findCachedByUserNumber = persistence.QueryField( 'user_number' );

UserSchema.statics.findCachedByNotUnique = persistence.QueryField( 'not_unique' , true /* return multiple */ );

var User = db.model('User',UserSchema) ;


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


/*!
 * hydrates many documents
 *
 * @param {Model} model
 * @param {Array} docs
 * @param {Object} fields
 * @param {Query} self
 * @param {Array} [pop] array of paths used in population
 * @param {Promise} promise
 */

function completeMany (model, docs, fields, self, pop, promise) {
  var arr = [];
  var count = docs.length;
  var len = count;
  var i = 0;
  var opts = pop ?
    { populated: pop }
    : undefined;

  for (; i < len; ++i) {
    arr[i] = new model(undefined, fields, true);
    arr[i].init(docs[i], opts, function (err) {
      if (err) return promise.error(err);
      --count || promise.complete(arr);
    });
  }
}


/*
// game-schema.js
var lastMod = require('./lastMod');
var Game = new Schema({ ... });
Game.plugin(lastMod, { index: true });

// player-schema.js
var lastMod = require('./lastMod');
var Player = new Schema({ ... });
Player.plugin(lastMod);

*/
