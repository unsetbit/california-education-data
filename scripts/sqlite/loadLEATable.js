// Inserts LEA lookup tables to the DB
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

var db = new sqlite.Database('../../data/cal-ed-annual-financial-data.sqlite3', function(){
	db.serialize(function() {
		db.run("PRAGMA synchronous=OFF");
		db.run("PRAGMA count_changes=OFF");
		db.run("PRAGMA journal_mode=MEMORY");
		db.run("PRAGMA temp_store=MEMORY", start);
	});
});

function start(){
	console.log('Started loading');
	var fileName = "LEAs";

	var counter = 0;
	var yearCount = years.length;
	var start = Date.now();
	var codeMap = {};
	years.forEach(function(year){
		csv.fromPath('../../data/csv/sacs/' + year + '/' + fileName  + '.csv', {headers:true}).on('record', function(row){
			var ccode = row.ccode || row.Ccode;
			var dcode = row.dcode || row.Dcode;
			var dname = row.dname || row.Dname;
			var dtype = row.dtype || row.Dtype;
			var regularada = row.regularada || row.RegularADA;
			var specialedada = row.specialedada || row.SpecialEdADA;

			if(dcode in codeMap){
				if(dname !== codeMap[dcode].Dname){
					//console.log("Mismatch!", year, dcode, dname, " !== " , codeMap[dcode].Dname);
				}
			} else {
				codeMap[dcode] = {
					Ccode: ccode,
					Dcode: dcode,
					Dname: dname,
					Dtype: dtype,
					RegularADA: regularada,
					SpecialEdADA: specialedada
				};
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
		console.log('Inserting', Object.keys(codeMap).length, 'items to table');
		//return;
		
		db.serialize(function() {
			db.run("DROP TABLE IF EXISTS " + fileName);
			db.run("CREATE TABLE " + fileName + " (Ccode TEXT, Dcode TEXT PRIMARY KEY, Dname TEXT, Dtype TEXT, RegularADA TEXT, SpecialEdADA TEXT)", fillit);
		});

		function fillit(){
			var insert = db.prepare("INSERT INTO " + fileName + " VALUES (?,?,?,?,?,?)");
			Object.keys(codeMap).forEach(function(code){
				var row = codeMap[code];
				insert.run(row.Ccode, row.Dcode, row.Dname, row.Dtype, row.RegularADA, row.SpecialEdADA);
			});
		
			insert.finalize(function(){
				console.log('Finished loading', fileName, 'in', Date.now() - start, "ms");	
			});
		};
	}
}
