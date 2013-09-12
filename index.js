var mongoose = require('mongoose') ;
var _ = require('underscore') ;
var format = require('util').format ;
var redis = require('redis') ;
var async = require('async') ;


var Persistence = module.exports = function (options) {
  
  var _this = this ;
  
  var cacheFields = options.cacheFields || ['_id'] ;
  
  _this.cacheFields = cacheFields ;
  
  _this.autocache = !! options.autocache ;
  
  _this.client = options.client ;
  
  _this.expireSeconds = options.expireSeconds || null ;
  
  return _this ;
  
};

Persistence.prototype.plugin = function(schema){
  
  var _this = this ;
  
  return function ( schema ) {
    
    
    if ( _this.autocache ) {
      
      schema.post('save', function(){
        var doc = this.toJSON({ depopulate: true })
        var Model = this.model(this.constructor.modelName);
        _this.saveCache.apply(_this,[Model,doc])
        
      } );
      
    }
    
    
    schema.methods.saveWithCache = function (cb) {
      
      var doc = this ;
      var Model = this.model(this.constructor.modelName);
      async.waterfall([
        
        function ( cb ) {
          
          doc.save(cb);
          
        },
        
        function ( doc , numAffected , cb ) {
          
          _this.saveCache(Model,doc.toJSON({ depopulate: true }),function(err){
            if ( ! err ) {
              cb(null,doc,numAffected);
            } else {
              cb(err)
            }
          });
        }
        
      ],cb)
      
    }
    
    schema.methods.flushCache = function (cb) {
      
      var doc = this ;
      var Model = this.model(this.constructor.modelName);
      async.waterfall([
        
        function ( cb ) {
          
          doc.save(cb);
          
        },
        
        function ( doc , numAffected , cb ) {
          
          _this.saveCache(Model,doc.toJSON({ depopulate: true }),function(err){
            if ( ! err ) {
              cb(null,doc,numAffected);
            } else {
              cb(err)
            }
          });
        }
        
      ],cb)
      
    }
    
    schema.statics.updateWithCache = function( condition , update, options, cb){
  
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
            
            _this.cacheExists( Model , doc , function (err , exists ){
              
              if ( exists ) {
                
                _this.saveCache( Model , doc , function (err) {
                  
                  if ( ! err ) {
                    
                    console.log('updated cache for doc %s',doc._id);
                    
                  }
                  
                })
                
              }
              
            })
            
          }
          
          cb( err , numAffected , results ) ;
          
        })
        
      }
  
  
    }

    schema.statics.findOneCached = function( queryObj , fields, options, cb){
  
      var args = _.toArray(arguments);
      var queryObj = args.shift() ;
      var cb = args.pop() ;
      var fields = args.shift() || null ;
      var opts = args.shift() || {} ;
      var fieldName , fieldVal ;
      var Model = this ;
      opts.fields = fields;
      
      _.each( queryObj , function ( v , k ) {
        
        if ( _.contains( _this.cacheFields , k ) ) {
          
          fieldName = k ;
          
          fieldVal = _.isArray( v ) ? v[0] : _.isArray( v && v.$in ) ? v.$in[0] || null : v ;
          
        }

      });

      if ( fieldName && fieldVal && _.isFunction(cb) ) {
      
        _this._findOne( Model , fieldName , fieldVal ,opts,cb);
      
      } else if ( _.isFunction(cb) ) {
        
        cb( new Error('Improper fields provided in query.') ) ;
        
      }

    }


    schema.statics.findCached = function( queryObj , fields, options, cb){
  
      var args = _.toArray(arguments);
      var queryObj = args.shift() ;
      var cb = args.pop() ;
      var fields = args.shift() || null ;
      var opts = args.shift() || {} ;
      var fieldName , fieldVals ;
      var Model = this ;
      
      opts.fields = fields;
      
      _.each( queryObj , function ( v , k ) {
        
        if ( _.contains( _this.cacheFields , k ) ) {
          
          fieldName = k ;
          
          fieldVals = _.isArray( v && v.$in ) ? v.$in : [v] ;
          
        }
        
      });
      
      if ( fieldName && fieldVals && _.isFunction(cb) ) {
        
        async.map( fieldVals , function ( fieldVal , cb ){
          
          _this._findOne( Model , fieldName , fieldVal ,opts,cb);
          
        } , function ( err , docs ) {
          
          if ( err ) {
            
            cb( err )
            
          } else {
            
            var seen = {} ;
            
            var filteredDocs = docs.filter(function(doc){
              
              var idStr = doc && doc._id && doc._id.toString()
              
              if ( idStr && ! seen[ idStr ] ) {
                
                seen[ idStr ] = 1 ;
                
                return true ;
                
              }
              
              else {
                
                return false ;
                
              }
              
            })
            
            cb( null , filteredDocs );
            
          }
          
        });
        
      } else if ( _.isFunction(cb) ) {
        
        cb( new Error('Improper fields provided in query.') ) ;
        
      }

    }

    
  }
  
}

Persistence.prototype.saveCache = function( Model , json , cb ){
  
  var _this = this ;
  var modelName = Model.modelName ;
  var client = this.client ;
  var cacheFields = this.cacheFields ;
  var expireSeconds = this.expireSeconds ;
  var serialized = {} ;
  
  _.each( json , function ( val , k ) {
    
    serialized[ k ] = JSON.stringify(val) ;
    
  });
  
  var cachableFields = cacheFields.filter(function(fieldName){
    
    if ( json.hasOwnProperty( fieldName ) ) return true ;
    
  }).map(function( fieldName ){
    
    var cacheKey = format('%s::%s::%s' , modelName , fieldName , json[fieldName] ) ;
    
    return cacheKey ;
    
  });
  
  async.each( cachableFields , function ( k , cb ) {
    
    async.series([
      
      function ( cb ) {
        
        client.hmset( k , serialized , cb );
        
      },
      
      function ( cb ) {
        
        if ( expireSeconds ) {
          
          client.expire( k , expireSeconds , cb );          
        
        } else {
          
          cb(null)
          
        }
        
      }
        
    ] , cb );
    
    
  }, function(err){
    
    if ( ! err ) cb(null,json) ;
    
    else {
    
      console.error( "error writing to cache: %s" , err ) ;
      console.error("doc",json)
      
      cb( null , json )
      
    }
    
  })

};

Persistence.prototype.cacheExists = function(Model,json,cb){
  
  var _this = this ;
  var modelName = Model.modelName ;
  var client = this.client ;
  var cacheFields = this.cacheFields ;
  
  var cachableFields = cacheFields.map(function( fieldName ){
    
    var cacheKey = format('%s::%s::%s' , modelName , fieldName , json[fieldName] ) ;
    
    return cacheKey ;
    
  });
  
  async.map( cachableFields , function ( k , cb ) {

    client.type( k , cb );

  }, function ( err , results ) {

    if ( ! err ) {

      var cacheExists = _.every( results , function ( result ) { return result == "hash" }) ;

      cb( null , cacheExists ) ; 

    } else {

      console.error( "error checking to cache: %s" , err ) ;

      cb( null , false )

    }

  });

};

Persistence.prototype.flushCache = function(Model,json,cb){
  
  var _this = this ;
  var modelName = Model.modelName ;
  var client = this.client ;
  var cacheFields = this.cacheFields ;
  
  var cachableFields = cacheFields.map(function( fieldName ){
    
    var cacheKey = format('%s::%s::%s' , modelName , fieldName , json[fieldName] ) ;
    
    return cacheKey ;
    
  });
  
  async.each( cachableFields , function ( k , cb ) {

    client.del( k , cb );

  }, function ( err ) {

    if ( ! err ) {

      cb( null , true ) ; 

    } else {

      console.error( "error checking to cache: %s" , err ) ;

      cb( null , false )

    }

  });

};


Persistence.prototype._findOne = function ( Model , fieldName , fieldEquals , opts , cb ) {
  
  var client = this.client ;
  var cb = _.last(arguments) ;
  var _this = this ;
  var cacheQuery = new CacheQuery( client , Model , opts  ).where(fieldName,fieldEquals) ;
  
  cacheQuery.exec(function(err,doc){

    if ( doc  ) {
      
      cb( null , doc ) ;
      
    } else if ( ! doc ) {
      
      var q = {} ;
      
      q[ fieldName ] = fieldEquals ;
      
      async.waterfall([
        
        function ( cb ) {
          
          Model.findOne( q ).lean().exec( cb ) ;
          
        },
        
        function ( doc , cb ) {
          
          if ( doc ) {
            
            _this.saveCache( Model , doc , cb )
            
          } else {
            
            cb( null , null ) ;
            
          }
          
          
        },
        
        function ( doc , cb ) {
          
          if ( doc ) {
            
            cb( null , pruneFields( doc , opts.fields ) ) ;
            
          } else {
            
            cb( null , null ) ;
            
          }
          
        },
        
        function ( doc , cb ) {
          
          if ( doc && opts.lean ) {
            
            doc._id = doc._id.toString() ;
            
            cb( null , doc ) ;
            
          } else if ( doc && !opts.lean ) {
            
            var m = new Model(undefined,null,true);
      
            m.init(doc);
        
            cb( null , m ) ;
            
          } else {
            
            cb( null , null )
            
          }
          
        }
        
      ] , cb );
            
    } 

  });
  
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
  var opts = _this.opts || {} ;
  var key = format('%s::%s::%s' , modelName , fieldName , fieldEquals ) ;

  execQuery( client , Model , key , opts || {} , function(err,doc){
    
    if ( ! err ) {
      
      cb(null,doc);
      
    } else {
      
      console.error( "error reading from cache: %s" , err ) ;
      cb(null,null);
      
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
          
          var m = new Model(undefined,null,true);
      
          m.init(unserialized);
        
          cb( null , m ) ;
          
        }
        
      } else {
        
        cb( null , null ) ;
        
      }
      
    }
    
  ],cb)
  
}


function pruneFields( doc , fields ) {
  
  var newDoc = {} ;
  
  _.each( doc , function(val,field){
    
    var addField =   
      ( field == '_id' ) ||
      ( ! fields ) || 
      (_.isObject(fields) && fields[field] == 1 ) ||
      (_.isString(fields) && fields.length && fields.indexOf(field) !=-1 ) ||
      (_.isArray(fields) && fields.length && fields.indexOf(field) !=-1 ) ;
    
    if ( addField ) {
      
      newDoc[ field ] = val ;
      
    }
    
  });
  
  return newDoc ;
}

function unserializeHash ( hash , fields ) {
  
  
  var unserialized = {} ;
  
  _.each( hash , function ( packedVal , field ) {
    
    var doUnserialize = 
      
      ( field == '_id' ) ||
      ( ! fields ) || 
      (_.isObject(fields) && fields[field] == 1 ) ||
      (_.isString(fields) && fields.length && fields.indexOf(field) !=-1 ) ||
      (_.isArray(fields) && fields.length && fields.indexOf(field) !=-1 ) ;
    
    if ( doUnserialize ) {
      
      unserialized[ field ] = JSON.parse(packedVal);
      
    }
    
  })
  
  return unserialized ;
  
}