const mongoose = require("mongoose");
const jsonfile = require("jsonfile-promised");
const _ = require("underscore");
const asyncLoop = require('node-async-loop');
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
            var collectionName = process.env.GOOD_CLINICAL;
            console.log('******', collectionName);
            var mongoData;
            db.collection(collectionName).find().toArray(function(err, data){
                mongoData = data;
            });

            var sampleMongData = process.env.GOOD_SAMPLE;
            db.collection(sampleMongData).find().toArray(function(err, data){
                if(sampleMongData.indexOf("_samplemap") > -1) {
                    Compression.compress_sample(data);
                }
            });

            var matrixData = '5a5671a2672fe4005779e25a_EXPR-RNAseq_log2_fpkm';
            db.collection(matrixData).find().toArray(function(err, data){
                if(matrixData.indexOf("_CNV") > -1 || matrixData.indexOf("_EXPR") > -1) {
                    Compression.compress_molecular(data);
                }
            });

            var mutData = '5a46b8d9d2601e00405c650f_MUT-prot';
            var mut;
            db.collection(mutData).find().toArray(function(err, data){
                mut = data;
                if(matrixData.indexOf("_CNV") > -1 || matrixData.indexOf("_EXPR") > -1) {
                    Compression.compress_molecular(data);
                }
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

var Compression = {  
    compress_clinical: function(clinicalData) {
        return 'clinicalData';
    },
    compress_clinicalEvent: function(clinicalEventData){
        return 'clinicalEventData';
    },
    compress_molecular: function(molecularData) {
        obj = {};
        ids = molecularData[0].s;
        genes = molecularData.map(c=>c.m);
        values = molecularData.map(c=>c.d);
        obj.ids = ids;
        obj.genes = genes;
        obj.values = values;
        // console.log(Object.keys(obj));
        // console.log(obj.ids);
        console.log(obj.genes.length);
        console.log(obj.values.length);
        return obj;
    },
    compress_mutation: function(mutationData) {
        return 'mutationData';
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
