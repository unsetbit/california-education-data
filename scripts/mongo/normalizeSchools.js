var MongoClient = require('mongodb').MongoClient;

MongoClient.connect('mongodb://localhost/caleddata', function(err, db){
	if(err) throw err;
	console.log('Ready!');
	var docs = db.collection('schools');

	splitCDS(docs, function(){
		//db.close();	
	});
});


function splitCDS(docs, cb){
	docs.find().each(function(err, doc){
		if(err) return console.log(err);
		if(doc === null) return cb();

		var cds = doc._id;
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
