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

	schools.find({ Latitude: {$ne: ''}, School: {$ne: ''}}, {Latitude: 1, Longitude: 1, School: 1}).toArray(function(err, schoolList){
		if(err) return console.error(err);
		
		var ids = schoolList.map(function(school){return school._id});


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
				
				// merge school list to apilist
				merged = apiList.map(function(api){
					var school = schoolList.filter(function(school){
						return school._id === api.CDS; 
					})[0];

					school.API = api.API;
					return school;
				});

				csv.writeToPath(filePath, merged, { headers: true }).on('finish', function(){
					deferred.resolve();
				});
			});

			return deferred.promise;
		}


		var baseDir = path.join(__dirname, '../../data/csv/generated/APILatLong/');
		mkdirp.sync(baseDir);
		generateAPILatLongCSV(baseDir + '2012base.csv', 2012, 'base').then(function(){
			return generateAPILatLongCSV(baseDir + '2011base.csv', 2011, 'base');
		}).then(function(){
			console.log('DONE!');
			db.close();
		});
	});
});

