var sqlite = require('sqlite3');


var csv = require('fast-csv');

var years = [
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

var db = new sqlite.Database('../../data/sqlite/cal-ed-annual-financial-data.sqlite3', function(){
	db.serialize(function() {
		db.run("PRAGMA synchronous=OFF");
		db.run("PRAGMA count_changes=OFF");
		db.run("PRAGMA journal_mode=MEMORY");
		db.run("PRAGMA temp_store=MEMORY");
		db.run("DROP TABLE IF EXISTS UserGL");
		db.run("CREATE TABLE UserGL (Ccode TEXT, Dcode TEXT, SchoolCode TEXT, Fiscalyear TEXT, Period TEXT, Colcode TEXT, Account TEXT, Fund TEXT, Resource TEXT, Projectyear TEXT, Goal TEXT, Function TEXT, Object TEXT, Value NUMERIC)", fillit);
	});
});

function fillit(){
	var insert = db.prepare("INSERT INTO UserGL VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

	var year = years.pop();
	console.log('Started loading', year);	

	csv.fromPath('../../data/csv/sacs/' + year + '/UserGL.csv', {headers:true}).on('record', function(row){
		insert.run(row.Ccode, row.Dcode, row.SchoolCode, row.Fiscalyear, row.Period, row.Colcode, row.Account, row.Fund, row.Resource, row.Projectyear, row.Goal, row.Function, row.Object, row.Value);
		counter++;
	}).on('end', function(){
		var end = Date.now();
		console.log('Finished loading csv of ', year);	
		console.log('elapsed', end - start);

		insert.finalize(function(){
			console.log('Finished loading', year);
			if(years.length){
				fillit();
			}
		});
	});
};