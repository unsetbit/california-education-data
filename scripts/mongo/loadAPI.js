var MongoClient = require('mongodb').MongoClient;
var csv = require('fast-csv');
var fs = require('fs');

var years = [
	'2000-2001',
	'2001-2002',
	'2002-2003',
	'2003-2004',
	'2004-2005',
	'2005-2006',
	'2006-2007',
	'2007-2008',
	'2008-2009',
	'2009-2010',
	'2010-2011',
	'2011-2012',
	'2012-2013'
];

var counter = 0;
var yearCount = years.length;
var start = Date.now();
var theDn;
MongoClient.connect('mongodb://localhost/caleddata', function(err, db){
	if(err) throw err;
	console.log('Ready!');
	var apiCollection = db.collection('api');
	fill(apiCollection);
	theDb = db; // used to close in finalize #lazy
});

var results = [];
function fill(collection){
	var year = years.pop();
	console.log('Started loading', year);

	var startYear = parseInt(year.split('-')[0], 10);

	var growthApiPath = '../../data/csv/api/' + year + '/' + startYear + '-growth-api.csv';
	var baseApiPath = '../../data/csv/api/' + year + '/' + startYear + '-base-api.csv';

	var done = false;
	function next(){
		if(done){
			if(years.length){
				fill(collection);
			} else {
				finish(results, collection);
			}
		} else {
			done = true;
		}
	}

	fs.exists(growthApiPath, function(exists){
		if(!exists) return;

		csv.fromPath(growthApiPath, { headers:true })
			.on('record', function(row){
				row._id = getId(startYear, row) + 'G';
				row.startYear = startYear;
				row.dataType = "growth";
				collection.insert(row, function(err){
					if(err) console.error(err);
				});
				counter++;
			}).on('end', function(){
				var end = Date.now();
				console.log('Finished loading csv of ', year, counter);	
				console.log('elapsed', end - start);
				next();
			}
		);
	});

	fs.exists(baseApiPath, function(exists){
		if(!exists) return;
		
		csv.fromPath(baseApiPath, { headers:true })
			.on('record', function(row){
				row._id = getId(startYear, row) + 'B';
				row.startYear = startYear;
				row.dataType = "base";
				collection.insert(row, function(err){
					if(err) console.error(err);
				});
				
				counter++;
			}).on('end', function(){
				var end = Date.now();
				console.log('Finished loading csv of ', year, counter);	
				console.log('elapsed', end - start);
				next();
			}
		);
	});
};

function finish(results, collection){
	console.log("Total Record Count", results.length);
	collection.count(function(err, count) {
		theDb.close();
	});
}

function getId(year, row){
	return '' + year + row.CDS;
}