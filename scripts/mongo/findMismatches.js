var MongoClient = require('mongodb').MongoClient;
var csv = require('fast-csv');
var fs = require('fs');

// File is at ftp://ftp.cde.ca.gov/demo/schlname/pubschls.txt

// Finds discrepencies in school ids between the file above and the apis which were reported

// cds 39686769003484 not in schools but it has data // Children's Home of Stockton, Stockton City Unified
MongoClient.connect('mongodb://localhost/caleddata', function(err, db){
	if(err) throw err;
	console.log('Ready!');
	var apis = db.collection('api');
	var schools = db.collection('schools');

	schools.distinct('_id', function(err, ids){
		apis.distinct('CDS', {CDS: {$nin: ids}}, function(err, cdses){
			console.log(cdses.length);
			var ccodes = {};
			var dcodes = {};
			var scodes = {};

			cdses.forEach(function(cds){
				ccodes[cds.substr(0,2)] = true;
				dcodes[cds.substr(2,5)] = true;
				scodes[cds.substr(7)] = true;
			});
			
			ccodes = Object.keys(ccodes);
			dcodes = Object.keys(dcodes);
			scodes = Object.keys(scodes);

			console.log('ccodes', ccodes.length);
			console.log('dcodes', dcodes.length);
			console.log('scodes', scodes.length);

			schools.distinct('ccode', {ccode : {$in: ccodes}}, function(err, fccodes){
				console.log('found ccodes', fccodes.length);
				console.log('missing', reduce(ccodes, fccodes));
			});

			schools.distinct('dcode', {dcode : {$in: dcodes}}, function(err, fdcodes){
				console.log('found dcodes', fdcodes.length);
				console.log('missing', reduce(dcodes, fdcodes));
			});

			schools.distinct('scode', {scode : {$in: scodes}}, function(err, fscodes){
				console.log('found scodes', fscodes.length);
				console.log('missing', reduce(scodes, fscodes));
			});
		});
	});
});

function reduce(big, little){
	return big.filter(function(item){ return little.indexOf(item) === -1; });
}


// school district 70862 -> api 40246 // Petaluma City Schools
// school district 70920 -> api 40253 // Santa Rosa City Schools
// school 08100820106625 -> api 08100821016625 // Elk Creek (Juvenile Hall)
// api drop where cds = 34739733403081 //  Non-public non-sectarian schools, Center Joint Unified, no data and not in schools list
// api drop where cds = 50757397024490 // Stanislaus Academy, Turlock Unified, no data and not in schools list
// api drop where cds = 19643297066079 // Joan Macy, Bonita Unified, no data and not in schools list
// api drop where cds = 19643297081268 // Canyon View, Bonita Unified, no data and not in schools list
// api drop where cds = 43694849003476 // Odd Fellow Rebekah, Gilroy Unified // no data and not in schools list
// cds 39686769003484 not in schools but it has data // Children's Home of Stockton, Stockton City Unified