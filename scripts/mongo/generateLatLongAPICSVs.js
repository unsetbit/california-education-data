var MongoClient = require('mongodb').MongoClient;
var csv = require('fast-csv');
var fs = require('fs');
var q = require('q');
var path = require('path');
var mkdirp = require('mkdirp');

MongoClient.connect('mongodb://localhost/caleddata', function(err, db){
	if(err) throw err;
	console.log('Ready!');
	var apis = db.collection('api');
	var schools = db.collection('schools');

	schools.find({ccode: {$ne: '00'}, scode: {$ne:'0000000'}}, {NCESDist: 1, District: 1}).toArray(function(err, dlist){
		if(err) return console.error(err);
		
		var ids = dlist.map(function(d){return d._id});

		function generateAPILatLongCSV(filePath, year, type){
			var deferred = q.defer();

			apis.find(
				{startYear:year, dataType:type, API:{$ne:''}, CDS: {$in: ids}}, 
				{_id: 0, API: 1, CDS: 1}
			).toArray(function(err, apiList){
				if(err){
					console.error(err);
					deferred.reject(err);
					return;
				}
				
				// merge district list to apilist
				merged = apiList.map(function(api){
					var d = dlist.filter(function(d){
						return d._id === api.CDS; 
					})[0];

					d.API = api.API;
					return d;
				});

				csv.writeToPath(filePath, merged, { headers: true }).on('finish', function(){
					deferred.resolve();
				});
			});

			return deferred.promise;
		}


		var baseDir = path.join(__dirname, '../../data/csv/generated/APILatLong/');
		mkdirp.sync(baseDir);
		generateAPILatLongCSV(baseDir + '2012-base-api.csv', 2012, 'base')
		.then(function(){
			console.log('DONE!');
			db.close();
		});
	});
});

