var MongoClient = require('mongodb').MongoClient;

MongoClient.connect('mongodb://localhost/caleddata', function(err, db){
	if(err) throw err;
	console.log('Ready!');
	var docs = db.collection('api');

	normalizeApiFields(docs, function(){
		splitCDS(docs, function(){
			removeEmpties(docs, function(){
				db.close();	
			});
		});
	});
});

function removeEmpties(docs, cb){
	docs.delete({scode: {$wq: ''}}, function(err){
		if(err) console.log(err);
		cb();
	});
}

function splitCDS(docs, cb){
	docs.find().each(function(err, doc){
		if(err) return console.log(err);
		if(doc === null) return cb();

		var cds = doc.CDS;
		var ccode = cds.substr(0,2);
		var dcode = cds.substr(2,5);
		var scode = cds.substr(7);

		docs.update(
			{ _id : doc._id }, 
			{ 
				$set : {
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

function normalizeApiFields(docs, cb){
	var renameAPIField = { 
		'API00': 2000,
		'API01': 2001,
		'API02': 2002,
		'API03': 2003,
		'API04': 2004,
		'API05': 2005,
		'API05B': 2005,
		'API06': 2006,
		'API06B': 2006,
		'API07': 2007,
		'API07B': 2007,
		'API08': 2008,
		'API08B': 2008,
		'API09': 2009,
		'API09B': 2009,
		'API10': 2010,
		'API10B': 2010,
		'API11': 2011,
		'API11B': 2011,
		'API12': 2012,
		'API12B': 2012,
		'API13': 2013,
		'API13B': 2013,
	};
	var count = Object.keys(renameAPIField).length;
	
	Object.keys(renameAPIField).forEach(function(fieldName){
		var updateObj = {};
		updateObj[fieldName] = 'API';

		docs.update( 
			{ startYear: renameAPIField[fieldName] }, 
			{ $rename : updateObj },
			{ multi: true },
			function(err){
				count--;
				if(err) console.error(err);
				else console.log('updated', fieldName);
				if(!count){
					cb();
				}
			}
		);
	});
}