const mongoose = require("mongoose");
const fs = require('fs');
const jsonfile = require("jsonfile-promised");
const _ = require("underscore");
const asyncLoop = require('node-async-loop');
const moment = require('moment');
const zlib = require('zlib');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
var connection = mongoose.connection;
var allCollections = [];
var userCollections = [];
var userProjectIDs = [];

mongoose.connect(
    'mongodb://oncoscape-dev-db1.sttrcancer.io:27017,oncoscape-dev-db2.sttrcancer.io:27017,oncoscape-dev-db3.sttrcancer.io:27017/v2?authSource=admin', {
    db: {
        native_parser: true
    },
    server: {
        poolSize: 5,
        reconnectTries: Number.MAX_VALUE
    },
    replset: {
        rs_name: 'rs0'
    },
    user: process.env.MONGO_USERNAME,
    pass: process.env.MONGO_PASSWORD
});

var credentials = new AWS.SharedIniFileCredentials({profile: 'default'});
AWS.config.credentials = credentials;
s3.config.region = 'us-west-2';
var params = {Bucket:'canaantt-test'};

connection.once('open', function(){
    var db = connection.db; 
    db.listCollections().toArray(function(err, names){
        allCollections = names.map(n=>n.name);
        userCollections = allCollections.filter(collName => {
            return collName.split('_')[0].length === 24
        });
        userProjectIDs = _.uniq(userCollections.map(collName => collName.split('_')[0]));
        // console.log(userCollections.filter(collName => collName.indexOf(userProjectIDs[0]) > -1));
        asyncLoop(userCollections, function(collectionName, next){ 
            console.log('******', collectionName);
            var collectionName = '5a6a52d184674a0047fed264_phenotype';
            db.collection(collectionName).find().toArray(function(err, data){
                if(collectionName.indexOf("_phenotype") > -1){
                    var res1 = CompressionFactory.compress_clinical(data);
                    var clinicalFileName = collectionName.split('_')[0]+'_clinical.json.gz';
                    gzip_upload2S3_private(res1, clinicalFileName);

                    var res2 = CompressionFactory.compress_clinicalEvent(data);
                    var clinicalEventFileName = collectionName.split('_')[0]+'_events.json.gz';
                    gzip_upload2S3_private(res2, clinicalEventFileName);
                }
                if(collectionName.indexOf("_EXPR") > -1 || collectionName.indexOf("_CNV") > -1){
                    var res = CompressionFactory.compress_molecularMatrix(data);
                    var molecularMatrixFileName = collectionName + '.json.gz';
                    gzip_upload2S3_private(res, molecularMatrixFileName);
                }
                if(collectionName.indexOf("_MUT") > -1){
                    var res = CompressionFactory.compress_mutation(data);
                    var molecularMutationFileName = collectionName + '.json.gz';
                    gzip_upload2S3_private(res, molecularMutationFileName);
                }
                if(collectionName.indexOf("_samplemap") > -1){
                    var res = CompressionFactory.compress_sample(data);
                    var sampleMapFileName = collectionName.split('_')[0] + '_psmap.json.gz';
                    gzip_upload2S3_private(res, sampleMapFileName);
                } 
                // if(collectionName.indexOf("_samplemap") > -1){
                //     var res = CompressionFactory.compress_sample(data);
                //     var sampleMapFileName = collectionName.split('_')[0] + '_psmap.json.gz';
                //     gzip_upload2S3_private(res, sampleMapFileName);
                // } 
            });
          }, function (err)
          {
              if (err)
              {
                  console.error('Error: ' + err.message);
                  return;
              }
              console.log('Finished!');
              connection.close();
          });
    });
});

var CompressionFactory = {  
    compress_clinical: function(clinicalData) {
        var obj = {};
        var ids = clinicalData.map(c=>c.id);
        var fields = {};
        var keys = ['enum',
                    'num',
                    'date',
                    'boolean',
                    'other'];
        var flattened = []; 
        clinicalData.forEach(function(d){
            var o = {};
            o['id'] = d.id;
            keys.forEach(function(key){
                if(Object.keys(d[key]).length !== 0){
                    Object.keys(d[key]).forEach(function(k){
                        o[k+'--'+key] = d[key][k];
                    });
                }
            });
            flattened.push(o);
        });
        var compiledfields = flattened.map(f=>Object.keys(f)).reduce(function(a,b){return _.uniq(a.concat(b))});
        compiledfields.shift();
        compiledfields.forEach(function(f){
            if(f.indexOf('--num') > -1 || f.indexOf('--date') > -1) {
                if(f.indexOf('--date') > -1){
                    fields[f] = {
                        'min': _.min(flattened.map(d=>d[f])),
                        'max': _.max(flattened.map(d=>d[f]))
                    }
                }else{
                    fields[f] = {
                        'min': _.min(flattened.map(d=>parseFloat(d[f]))),
                        'max': _.max(flattened.map(d=>parseFloat(d[f])))
                    }
                }
                
            } else {
                fields[f] = _.uniq(flattened.map(d=>d[f]));
            }
        });
        var values = flattened.map(fd=>{
            return compiledfields.map(key => {
                if(key.indexOf('--num') > -1 || key.indexOf('--date') > -1) {
                    return fd[key];
                } else {
                   return fields[key].indexOf(fd[key]);
                }
            });
        });
        obj.ids = ids;
        obj.fields = fields;
        obj.values = values;
        return obj;
    },
    compress_clinicalEvent: function(clinicalEventData){
        var events = clinicalEventData.map(c=>c.events);
        if( _.uniq(events).length != 1){
            events = events.reduce(function(a,b){return a.concat(b)});
            var mapkeys = _.uniq(events.map(e=>e.subType));
            var map = {};
            mapkeys.forEach(k => {
                map[k] = events.find(e => e.subType==k).type;
            });
            var data = events.map(e=>{
                var arr = [];
                arr.push(e.PatientId);
                arr.push(mapkeys.indexOf(e.subType));
                arr.push(new Date(e.startDate).getTime()/1000);
                arr.push(new Date(e.endDate).getTime()/1000);
                arr.push(_.omit(e, "type", "PatientId", "startDate", "endDate"));
                return arr;
            });
            var obj = {};
            obj.map = map;
            obj.data = data;
            console.log(obj.data[0]);
            
            return obj;
        } else {
            return null;
        } 
    },
    compress_molecularMatrix: function(molecularData) {
        var obj = {};
        var ids = molecularData[0].s;
        var genes = molecularData.map(c=>c.m);
        var values = molecularData.map(c=>c.d);
        obj.ids = ids;
        obj.genes = genes;
        obj.values = values;
        console.log(obj.genes.length);
        console.log(obj.values.length);
        return obj;
    },
    compress_mutation: function(mutationData) {
        var obj = {};
        var ids = mutationData[0].s;
        var genes = mutationData.map(c=>c.m);
        var values = [];
        var i = 0;
        var j = 0;
        mutationData.forEach(function(byMarker){
            byMarker.d.forEach(function(bySample){
                if(bySample !== 'NA'){
                    values.push(i + '-' + j + '-' + 1);
                } else {
                    values.push(i + '-' + j + '-' + 0);
                } 
                j++;
            });
            i++;
        });
        obj.ids = ids;
        obj.genes = genes;
        obj.values = values;
        console.log(obj.values.length);
        return obj;
    },
    compress_sample: function(samplePatientData) {
        var sampleData = samplePatientData[0];
        var keys = _.uniq(_.values(sampleData));
        console.log(keys);
        keys.shift();
        var obj = {};
        keys.forEach(function(key){
            obj[key] = Object.keys(sampleData).filter(sk=> sampleData[sk] == key)
        });
        console.log(obj);
        return obj;
    }, 
    generate_meta_data: function(projectID) {
        return 'projectID';
    }
};

var gzip_upload2S3_private = function(JSONOBJ, FILENAME){
    zlib.gzip(JSON.stringify(JSONOBJ), level=9, function(err, result){
        s3.putObject({Bucket:'canaantt-test', 
                  Key: FILENAME, 
                  Body: result, 
                  ACL:'private'}, 
                  function(res, err){
                      console.log(res);
                      if(err){
                          console.log(err);
                      }
                      console.log('Success!');
                    });
    });
    
};



/*  
    AWS S3 related
    aws-sdk package needs to be installed gloablly to pick up ~/.aws/credentials.
    - to set ~/.aws/credentials, do `$ aws configure`
    - sudo npm install -g aws-sdk # definitely delete the local aws-sdk package. 
    - to be able to require('aws-sdk') from global packages dir, need to export this enviramental variable
       `export NODE_PATH=/usr/local/lib/node_modules`
    if there is aws credential issue, when the first direct way is to check process.env in nodeJS.
    It could be ~/.aws/credential is not set right. refer to the proper profile. 
    It could be ~/.bash-profile that the AWS_ACCESS_KEY_ID and/or AWS_SECRET_ACCESS_KEY were set earlier.
        unset AWS_ACCESS_KEY_ID
        unset AWS_SECRET_ACCESS_KEY
*/


// s3.createBucket(params, function(err, data){ if(err) console.log(err, err.stack); else console.log(data);});

// s3.deleteBucket(params, function(err, data) {
//     if (err) console.log(err, err.stack); // an error occurred
//     else     console.log(data);           // successful response
// });


