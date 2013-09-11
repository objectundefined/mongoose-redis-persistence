var mongoose = require('mongoose') ;
var _ = require('underscore') ;
var format = require('util').format ;
var msgpack = require('msgpack') ;
var redis = require('redis') ;
var async = require('async') ;


var Persistence = module.exports = function (options) {
  
  var _this = this ;
  
  var cacheFields = options.cacheFields || ['_id'] ;
  
  _this.cacheFields = cacheFields ;
  
  _this.autocache = !! options.autocache ;
  
  _this.modelName = options.modelName ;
  
  _this.client = options.client ;
  
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
    
    schema.statics.updateWithCacheById = function( condition , update, options, cb){
  
      var id = condition._id ;
      var args = _.toArray(arguments);
      var conditions = args.shift() ;
      var update = args.shift() || {} ;
      var cb = args.pop() ;
      var opts = args.pop() || {} ;
    
      var Model = this  ;
      
      
      if ( _.isFunction( cb ) ) {
    
        Model.update( condition , update , opts , function ( err , numAffected , results ) {
    
          if ( ! err ) {
            
            var doc = {}; 
            
            _.extend(doc,update,{_id : id})
            
            _this.saveCache( doc , function(){
              
              console.log('updated cache',arguments);
              
            })
            
          }
          
          cb( err , numAffected , results ) ;
          
        })
        
      }
  
  
    }

    schema.statics.getFromCacheById = function(id, fields, options, cb){
  
      var args = _.toArray(arguments);
      var id = args.shift() ;
      var cb = args.pop() ;
      var opts = args.pop() || {} ;
      opts.fields = args.pop() || null;

      if ( id && _.isFunction(cb) ) {
        persistence.findOne('_id',id,opts,cb);
      }

    }

    schema.statics.getFromCacheByIds = function(ids, fields, options, cb){

      var args = _.toArray(arguments);
      var ids = args.shift() ;
      var cb = args.pop() ;
      var opts = args.pop() || {} ;
      opts.fields = args.pop() || null;

      if ( _.isArray( ids ) && _.isFunction(cb) ) {
    
        async.map( ids , function ( id , cb ) {
      
          persistence.findOne('_id',id,opts,cb);
      
        } , cb ) ;
    
      }

    }
    
  }
  
}

Persistence.prototype.saveCache = function(doc,cb){
  
  var _this = this ;
  var modelName = this.modelName ;
  var model = mongoose.models[modelName] ;
  var client = this.client ;
  var cacheFields = this.cacheFields ;
  var json = _.isFunction(doc && doc.toJSON) ? doc.toJSON({ depopulate: true }) : doc ;
  var serialized = {} ;
  
  _.each( json , function ( val , k ) {
    
    serialized[ k ] = JSON.stringify(val) ;
    
  });
  
  var cachableFields = cacheFields.filter(function(fieldName){
    
    if ( json.hasOwnProperty( fieldName ) ) return true ;
    
  }).map(function( fieldName ){
    
    var fieldIsUnique = checkFieldUnique( fieldName , model ) ;
    
    var cacheKey = format('%s::%s::%s::%s' , modelName , fieldName , fieldIsUnique ? "0" : doc._id , json[fieldName] ) ;
    
    return cacheKey ;
    
  });
  
  async.each( cachableFields , function ( k , cb ) {
    
    client.hmset( k , serialized , cb );
    
  }, cb || function(){} )

};

Persistence.prototype.findOne = function ( fieldName , fieldEquals , opts , cb ) {
  
  var model = mongoose.models[this.modelName] ;
  var client = this.client ;
  var cb = _.last(arguments) ;
  var cacheQuery = new CacheQuery( client , model , opts  ).where(fieldName,fieldEquals) ;
  
  cacheQuery.exec(cb);
  
};

Persistence.prototype.find = function ( fieldName , fieldEquals , opts , cb ) {
  
  var model = mongoose.models[this.modelName] ;
  var client = this.client ;
  var cb = _.last(arguments) ;
  var opts = opts || {} ;

  opts.many = true ;
  
  var cacheQuery = new CacheQuery( client , model , opts  ).where(fieldName,fieldEquals) ;
  
  cacheQuery.exec(cb);
  
};


function CacheQuery ( client , Model , opts ) {
  
  this.query = { fieldName : '' , fieldEquals : '' } ;
  this.opts = opts || {} ;
  this.Model = Model ;
  this.client = client ;

  return this ;
//  console.log(this,Model);
  
};


CacheQuery.prototype.where = function ( field , equals ) {
  
  this.query.fieldName = field ;
  this.query.fieldEquals = equals ;
  
  return this ;
  
};

CacheQuery.prototype.exec = function ( cb ) {
  
  var _this = this ;
  var fieldName = _this.query.fieldName ;
  var fieldEquals = _this.query.fieldEquals ;
  var client = _this.client ;
  var Model = _this.Model ;
  var modelName = Model.modelName ;
  var fieldIsUnique = checkFieldUnique( fieldName , Model ) ;
  var opts = _this.opts || {} ;
  var many = opts.many ;
  
  
  async.waterfall([
    
    function ( cb ) {
      
      if ( fieldIsUnique ) {
        
        var uniqueKey = format('%s::%s::%s::%s' , modelName , fieldName , "0" , fieldEquals ) ;
        
        cb ( null , [ uniqueKey ] ) ;
        
      } else {
        
        var keysQuery = format('%s::%s::%s::%s' , modelName , fieldName , "*" , fieldEquals ) ;
        
        client.keys( keysQuery , cb ) ;
        
      }
      
    },
    
    function ( keys , cb ) {
      
      if ( ! many ) {
        
        keys = keys.slice(0,1)
        
      }
      
      async.map( keys , function( key , cb ){

        execQuery( client , Model , key , opts || {} , cb )
        
      } , cb ) ;
      
    }
    
  ],function(err,results){
    
    if ( ! opts.many && results.length ) {
      
      cb( err , results[0] )
      
    } else if ( opts.many && results.length ) {
      
      cb( err , results ) ;
      
    } else {
      
      cb( err , results ) ;
      
    } 
    
  })
  
}

function execQuery ( client , Model , query , opts , cb ) {
  
  var _this = this ;
  
  async.waterfall([
    
    function ( cb ) {
      
      client.hgetall( query , cb )
      
    },
    
    function ( redisHash , cb ) {
      
      if ( redisHash ) {
        
        cb( null , unserializeHash( redisHash , opts && opts.fields ) )
        
      } else {
        
        cb (  null , null ) ;
        
      }
      
    },
    
    function ( unserialized , cb ) {
      
      if ( unserialized ) {
        
        if ( opts && opts.lean ) {
          
          cb( null , unserialized ) ;
          
        } else {
          
          var m = new Model();
      
          m.init(unserialized);
        
          cb( null , m ) ;
          
        }
        
      } else {
        
        cb( null , null ) ;
        
      }
      
    }
    
  ],cb)
  
}


function unserializeHash ( hash , fields ) {
  
  
  var unserialized = {} ;
  
  _.each( hash , function ( packedVal , field ) {
    
    var doUnserialize = 
      
      ( field == '_id' ) ||
      ( ! fields ) || 
      (_.isObject(fields) && fields[field] == 1 ) ||
      (_.isArray(fields) && fields.length && fields.indexOf(field) !=-1 ) ;
    
    if ( doUnserialize ) {
      
      unserialized[ field ] = JSON.parse(packedVal);//msgpack.unpack( new Buffer(packedVal) ) ;
      
    }
    
  })
  
  return unserialized ;
  
}

function checkFieldUnique ( field , model ) {
  
  if ( field == '_id' ) {
    
    return true ;
    
  } else {
    
    return !! ( model && model.schema && model.schema.tree && model.schema.tree[ field ] && model.schema.tree[ field ].unique ) ;
    
  }
  
}
