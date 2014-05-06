var MongoClient = require('mongodb').MongoClient;
var csv = require('fast-csv');
var fs = require('fs');

// File is at ftp://ftp.cde.ca.gov/demo/schlname/pubschls.txt

var counter = 0;
var start = Date.now();
MongoClient.connect('mongodb://localhost/caleddata', function(err, db){
	if(err) throw err;
	console.log('Ready!');
	var docs = db.collection('schools');
	docs.remove({}, function(err){
		fill(docs);
	});
	
	theDb = db; // used to close in finalize #lazy
});

function fill(docs){
	console.log('Started loading');

	var schoolsTsvPath = '../../data/tsv/schools.txt';
	fs.exists(schoolsTsvPath, function(exists){
		if(!exists) return console.error('File not found!', schoolsTsvPath);

		csv.fromPath(schoolsTsvPath, { headers:true, delimiter: '\t', ignoreEmpty: true, quote:"sfdsdf", escape:"sdfsdf", trim:true })
			.on('record', function(row){
				if(!row.CDSCode) return;
				row._id = row.CDSCode;
				delete row.CDSCode;

				docs.insert(row, function(err){
					if(err) console.error(err, row);
				});
				counter++;
			}).on('end', function(){
				var end = Date.now();
				console.log('Finished loading schools tsv ', counter);	
				console.log('elapsed', end - start);
			}
		);
	});
};
