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
        db.collection("Accounts_Users").find().toArray(function(err, data){
            ACCOUNT_USERS = data;
        });
        db.collection("Accounts_Permissions").find().toArray(function(err, data){
            ACCOUNT_PERMISSIONS =  data;
        });
        db.collection("Accounts_Projects").find().toArray(function(err, data){
            ACCOUNT_PROJECTS =  data;
        });
        /*
        During Data uploading tool development, projects, users and permissions were generated with less accuracy.
        Check points:
        - Step I: Get all the existing projects and check them against ACCOUNT_PROJECTS and clean up ACCOUNT_PROJECTS
        - Step II: Check ACCOUNT_PERMISSIONS against ACCOUNT_PROJECTS and ACCOUNT_USERS and clean up 
        - Step III: clean ACCOUNT_USERS
        */

        // Step I: 
        userProjectIDs.filter(up => ACCOUNT_PROJECTS.map(p => p._id.toString()).indexOf(up) === -1)
        //[ '5a566f6cd8fd21006a0753d1', '5a456cfd5b0d8d2316817aad' ] are not registered in ACCOUNT_PROJECTS
        ACCOUNT_PROJECTS.map(p => p._id.toString()).filter(str => userProjectIDs.indexOf(str) === -1)
        // [ '5a468751c4e6c13d87ad0bbd','5a4fc82c672fe4005779e257','5a6102e94593b7005ee06191','5a626bf04593b7005ee06192' ]
        
        // Step II: results are [], which means all the Permissions Users and Projects exist in the ACCOUNT_USERS and ACCOUNT_PROJECTS respectively. 
        ACCOUNT_PERMISSIONS.map(p => p.Project.toString()).filter(u => ACCOUNT_PROJECTS.map(u => u._id.toString()).indexOf(u) === -1)
        ACCOUNT_PERMISSIONS.map(p => p.User.toString()).filter(u => ACCOUNT_USERS.map(u => u._id.toString()).indexOf(u) === -1)
        

    });
});

