// Inserts Function, Fund, Goal, Object, and Resource lookup tables to the DB
// Titles are normalized to use the latest definitions

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
].reverse();

var fileNames = [
	'Function',
	'Fund',
	'Goal',
	'Object',
	'Resource'
];
var db = new sqlite.Database('../../data/sqlite/cal-ed-annual-financial-data.sqlite3', function(){
	db.serialize(function() {
		db.run("PRAGMA synchronous=OFF");
		db.run("PRAGMA count_changes=OFF");
		db.run("PRAGMA journal_mode=MEMORY");
		db.run("PRAGMA temp_store=MEMORY", start);
	});
});

function start(){
	fileNames.forEach(function(fileName){
		console.log('Started loading', fileName);

		var counter = 0;
		var yearCount = years.length;
		var start = Date.now();
		var codeMap = {};
		years.forEach(function(year){
			csv.fromPath('../../data/csv/sacs/' + year + '/' + fileName + '.csv', {headers:true}).on('record', function(row){
				var code = row.code || row.Code;
				var title = row.title || row.Title;
				if(code in codeMap){
					if(title !== codeMap[code]){
						// console.log("Mismatch!", year, code, title, " !== " , codeMap[code]);
					}
				} else {
					codeMap[code] = title;
				}
				counter++;
			}).on('end', function(){
				yearCount--;
				if(!yearCount){
					writeIt();
				}
			});
		});


		function writeIt(){
			console.log('Inserting', Object.keys(codeMap).length, 'items to', fileName, 'table');

			db.serialize(function() {
				db.run("DROP TABLE IF EXISTS " + fileName);
				db.run("CREATE TABLE " + fileName + " (Code TEXT PRIMARY KEY, Title TEXT)", fillit);
			});

			function fillit(){
				var insert = db.prepare("INSERT INTO " + fileName + " VALUES (?,?)");
				Object.keys(codeMap).forEach(function(code){
					insert.run(code, codeMap[code]);
				});
			
				insert.finalize(function(){
					console.log('Finished loading', fileName, 'in', Date.now() - start, "ms");	
				});
			};
		}
	});
}
