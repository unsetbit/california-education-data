// Removes some discrepencies from  the API table to ensure it matches the schools list

var MongoClient = require('mongodb').MongoClient;
var csv = require('fast-csv');
var fs = require('fs');

MongoClient.connect('mongodb://localhost/caleddata', function(err, db){
	if(err) throw err;
	console.log('Ready!');
	var apis = db.collection('api');
	var schools = db.collection('schools');

 	// Petaluma City Schools
	changeDistrict(apis, '40246', '70862', function(err){ if(err) console.error(err); });
	
	// Santa Rosa City Schools
	changeDistrict(apis, '40253', '70920', function(err){ if(err) console.error(err); }); 
	
	// Elk Creek (Juvenile Hall)
	changeSchool(apis, '1016625', '0106625', function(err){ if(err) console.error(err); }); 
	
	// Non-public non-sectarian schools, Center Joint Unified, no data and not in schools list
	apis.remove({ CDS: '34739733403081'}, function(err){ if(err) console.error(err); }); 
	
	// Stanislaus Academy, Turlock Unified, no data and not in schools list
	apis.remove({ CDS: '50757397024490'}, function(err){ if(err) console.error(err); }); 
	
	// Joan Macy, Bonita Unified, no data and not in schools list
	apis.remove({ CDS: '19643297066079'}, function(err){ if(err) console.error(err); }); 
	
	// Canyon View, Bonita Unified, no data and not in schools list
	apis.remove({ CDS: '19643297081268'}, function(err){ if(err) console.error(err); }); 
	
	// Odd Fellow Rebekah, Gilroy Unified, no data and not in schools list
	apis.remove({ CDS: '43694849003476'}, function(err){ if(err) console.error(err); }); 

	// cds 39686769003484 not in schools but it has data // Children's Home of Stockton, Stockton City Unified
});


function changeDistrict(docs, oldDcode, newDcode){
	docs.find({ dcode: oldDcode }).each(function(err, doc){
		if(err) return console.log(err);
		if(doc === null) return;

		var ccode = doc.ccode;
		var dcode = newDcode;
		var scode = doc.scode;
		var cds = '' + ccode + dcode + scode;

		docs.update(
			{ _id : doc._id }, 
			{ 
				$set : {
					CDS: cds,
					ccode: ccode,
					dcode: dcode,
					scode: scode
				}
			},
			function(err){
				if(err) console.log(err);
			}
		);
	});
}

function changeSchool(docs, oldScode, newScode){
	docs.find({ scode: oldScode }).each(function(err, doc){
		if(err) return console.log(err);
		if(doc === null) return;

		var ccode = doc.ccode;
		var dcode = doc.dcode;
		var scode = newScode;
		var cds = '' + ccode + dcode + scode;

		docs.update(
			{ _id : doc._id }, 
			{ 
				$set : {
					CDS: cds,
					ccode: ccode,
					dcode: dcode,
					scode: scode
				}
			},
			function(err){
				if(err) console.log(err);
			}
		);
	});
}

/*

function findMismatches(apis, schools){
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
}

function reduce(big, little){
	return big.filter(function(item){ return little.indexOf(item) === -1; });
}


*/
