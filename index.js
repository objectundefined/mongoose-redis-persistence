var mongoose = require('mongoose') ;
var _ = require('underscore') ;
var format = require('util').format ;
var msgpack = require('msgpack') ;
var redis = require('redis') ;
var client = redis.createClient() ;
var async = require('async') ;


var Persistence = module.exports = function (options) {
  
  var _this = this ;
  
  var cacheFields = options.cacheFields || ['_id'] ;
  
  _this.cacheFields = cacheFields ;
  
  _this.autocache = !! options.autocache ;
  
  _this.modelName = options.modelName ;
  
  return _this ;
  
};

Persistence.prototype.plugin = function(schema){
  
  var _this = this ;
  
  return function ( schema ) {
    
    
    if ( _this.autocache ) {
      
      schema.post('save', _this.saveCache.bind(_this) );
      
    }
    
    schema.methods.saveWithCache = function (cb) {
      
      var doc = this ;
      
      async.waterfall([
        
        function ( cb ) {
          
          doc.save(cb);
          
        },
        
        function ( doc , numAffected , cb ) {
          
          _this.saveCache(doc,function(err){
            if ( ! err ) {
              cb(null,doc,numAffected);
            } else {
              cb(err)
            }
          });
        }
        
      ],cb)
      
    }
    
  }
  
}

Persistence.prototype.saveCache = function(doc,cb){
  
  var _this = this ;
  var modelName = this.modelName ;
  var model = doc.model(doc.constructor.modelName);
  var cacheFields = this.cacheFields ;
  var json = doc.toJSON({ depopulate: true }) ;
  var serialized = {} ;
  
  _.each( json , function ( val , k ) {
    
    serialized[ k ] = JSON.stringify(val) ;
    
  });
  
  var cachableFields = cacheFields.filter(function(fieldName){
    
    if ( json.hasOwnProperty( fieldName ) ) return true ;
    
  }).map(function( fieldName ){
    
    var fieldIsUnique = _this.fieldIsUnique( fieldName , model ) ;
    
    var cacheKey = format('%s::%s::%s::%s' , modelName , fieldName , fieldIsUnique ? "UNIQUE" : doc._id , json[fieldName] ) ;
    
    return cacheKey ;
    
  });
  
  async.each( cachableFields , function ( k , cb ) {
    
    client.hmset( k , serialized , cb );
    
  }, cb || function(){} )

};

Persistence.prototype.QueryField = function ( fieldName , many ) {
  
  var _this = this ;
  
  return function ( keyEquals , cb ) {
    
    var Model = this ;
    var modelName = Model.modelName ;
    var fieldIsUnique = _this.fieldIsUnique( fieldName , Model ) ;
    
    async.waterfall([
      
      function ( cb ) {
        
        if ( fieldIsUnique ) {
          
          var uniqueKey = format('%s::%s::%s::%s' , modelName , fieldName , "UNIQUE" , keyEquals ) ;
          
          cb ( null , [ uniqueKey ] ) ;
          
        } else {
          
          var keysQuery = format('%s::%s::%s::%s' , modelName , fieldName , "*" , keyEquals ) ;
          
          client.keys( keysQuery , cb ) ;
          
        }
        
      },
      
      function ( keys , cb ) {
        
        if ( ! many ) {
          
          keys = keys.slice(0,1)
          
        }
        
        async.map( keys , function( key , cb ){
          
          _this.execQuery( Model , key , cb )
          
        } , cb ) ;
        
      },
      
      function ( results , cb ) {
        
        if ( ! many && results ) {
          
          cb( null , results[0] )
          
        } else {
          
          cb(null,results);
          
        }
        
      }
      
    ],cb)
    
  }
  
//  console.log(this,Model);
  
};

Persistence.prototype.execQuery = function ( Model , query , cb ) {
  
  var _this = this ;
  
  async.waterfall([
    
    function ( cb ) {
      
      client.hgetall( query , cb )
      
    },
    
    function ( redisHash , cb ) {
      
      if ( redisHash ) {
        
        cb( null , _this.unserializeHash( redisHash ) )
        
      } else {
        
        cb (  null , null ) ;
        
      }
      
    },
    
    function ( unserialized , cb ) {
      
      if ( unserialized ) {
        
        var m = new Model();
      
        m.init(unserialized);
        
        cb( null , m ) ;
        
      } else {
        
        cb( null , null ) ;
        
      }
      
      
    }
    
  ],cb)
  
}


Persistence.prototype.unserializeHash = function ( hash ) {
  
  var unserialized = {} ;
  
  _.each( hash , function ( packedVal , k ) {
    
    unserialized[ k ] = JSON.parse(packedVal);//msgpack.unpack( new Buffer(packedVal) ) ;
    
  })
  
  return unserialized ;
  
}

Persistence.prototype.fieldIsUnique = function ( field , model ) {
  
  if ( field == '_id' ) {
    
    return true ;
    
  } else {
    
    return !! ( model && model.schema && model.schema.tree && model.schema.tree[ field ] && model.schema.tree[ field ].unique ) ;
    
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

/*
      async.map( cachableFields , function ( k , cb ) {
        
        async.waterfall([
            
          function ( cb ) {
            
            client.hgetall( k , cb );
      
          },
          
          function ( d , cb ) {
            
            var unserialized = {} ;
            
            _.each( d , function ( packedVal , k ) {
              
              unserialized[ k ] = msgpack.unpack( new Buffer(packedVal) ) ;
              
            })
            
            cb( null , unserialized ) ;
            
          },
          
          function ( d , cb ) {
            
            var m = new Model();
            
            m.init(d);
            
//            var m = new schema() ;
            
//            m.init( d ) ;
            
            cb( null , m ) ;
            
          }
            
        ],cb)
        
      },function ( err , d ){
      
        console.log('retrieved',arguments);
      
      })
      
*/