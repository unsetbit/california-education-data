var MongoClient = require('mongodb').MongoClient;
var csv = require('fast-csv');
var fs = require('fs');

// File is at ftp://ftp.cde.ca.gov/demo/schlname/pubschls.txt

var counter = 0;
var start = Date.now();
MongoClient.connect('mongodb://localhost/caleddata', function(err, db){
	if(err) throw err;
	console.log('Ready!');
	var apis = db.collection('api');
	var schools = db.collection('schools');

	schools.distinct('scode', function(err, ids){
		apis.distinct('CDS', {scode: {$nin: ids}}, function(err, docs){
			console.log(docs);
		});
	});
});
