const mongoose = require("mongoose");
const jsonfile = require("jsonfile-promised");
const _ = require("underscore");
const asyncLoop = require('node-async-loop');
var connection = mongoose.connection;
var allCollections = [];
var userCollections = [];
var userProjectIDs = [];
var ACCOUNT_USERS = [];
var ACCOUNT_PERMISSIONS = [];
var ACCOUNT_PROJECTS = [];
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
        

    });
});

var Compression = {  
    compress_clinical: function() {
        return 'generic dough';
    },
    compress_molecular: function() {
        return 'generic sauce';
    },
    compress_sample: function() {

    }, 
    generate_meta_data: function() {
        return 'generate_meta_data';
    }
};
