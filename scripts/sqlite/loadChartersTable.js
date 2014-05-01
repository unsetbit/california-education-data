// Inserts Charters tables to the DB, normalize old ones to new format
// Titles are normalized to use the latest definitions

var sqlite = require('sqlite3');
var csv = require('fast-csv');

var yearsOldFormat = [
	'2003-2004',
	'2004-2005',
	'2005-2006',
	'2006-2007',
	'2007-2008',
	'2008-2009'
].reverse();

var yearsNewFormat = [
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
		db.run("PRAGMA temp_store=MEMORY");
		db.run("DROP TABLE IF EXISTS Charters");
		db.run("CREATE TABLE Charters (StartingYear INTEGER, " + 
										"Ccode TEXT, " + 
										"Dcode TEXT, " +
										"SchoolID TEXT, " +
										"CharterNumber TEXT, " +
										"CharterName TEXT, " +
										"ReportType TEXT, " +
										"ReportLevel TEXT, " +
										"FundUsed TEXT, " +
										"RegularADA REAL, " +
										"SpecialEdADA REAL, " +
										"PRIMARY KEY (SchoolID, StartingYear))", start);
	});
});

function start(){
	function loadNewFormat(){
		var yearCount = yearsNewFormat.length;
		var insert = db.prepare("INSERT INTO Charters VALUES (?,?,?,?,?,?,?,?,?,?,?)");
		yearsNewFormat.forEach(function(year){
			var StartingYear = parseInt(year.split('-')[0]);

			csv.fromPath('../../data/csv/sacs/' + year + '/Charters.csv', {headers:true}).on('record', function(row){
				insert.run(
					StartingYear,
					row.Ccode, 
					row.Dcode, 
					row.SchoolID, 
					row.CharterNumber,
					row.CharterName,
					row.ReportType,
					row.ReportLevel,
					row.FundUsed, 
					row.RegularADA, 
					row.SpecialEdADA
				);
			}).on('end', function(){
				yearCount--;
				if(!yearCount){
					insert.finalize(function(){
						console.log("Done writing new format");
					});		
				}
			});
		});
	}

	function loadOldFormat(){
		var fileCount = yearsNewFormat.length * 3;
		var insert = db.prepare("INSERT INTO Charters VALUES (?,?,?,?,?,?,?,?,?,?,?)");
		var rows = [];		
		yearsOldFormat.forEach(function(year){
			var startingYear = parseInt(year.split('-')[0]);

			csv.fromPath('../../data/csv/sacs/' + year + '/CharterFilingAlternativeForm.csv', {headers:true}).on('record', function(row){
				row.startingYear = startingYear;
				row.reportType = "ALT Form";
				row.RegularADA = null;
				row.SpecialEdADA = null;
				rows.push(row);
			}).on('end', function(){
				fileCount--;
				if(!fileCount){ writeIt(); }
			});

			csv.fromPath('../../data/csv/sacs/' + year + '/CharterFilingSACS.csv', {headers:true}).on('record', function(row){
				row.startingYear = startingYear;
				row.reportType = "SACS";
				row.RegularADA = null;
				row.SpecialEdADA = null;
				rows.push(row);
			}).on('end', function(){
				fileCount--;
				if(!fileCount){ writeIt(); }
			});

			csv.fromPath('../../data/csv/sacs/' + year + '/CharterNotFiling.csv', {headers:true}).on('record', function(row){
				row.StartingYear = startingYear;
				row.ReportType = "Did not report";
				row.RegularADA = null;
				row.SpecialEdADA = null;
				rows.push(row);
			}).on('end', function(){
				fileCount--;
				if(!fileCount){ writeIt(); }
			});			
		});

		function writeIt(){
			rows.forEach(function(row){
				insert.run(
					row.StartingYear,
					row.Ccode, 
					row.Dcode, 
					row.SchoolID, 
					row.CharterNumber,
					row.CharterName,
					row.ReportType,
					row.ReportLevel,
					row.FundUsed, 
					row.RegularADA, 
					row.SpecialEdADA
				);
			});

			insert.finalize(function(){
				console.log("Done writing old format");
			});		
		}
	}

	console.log('Started loading');
	var insert = db.prepare("INSERT INTO Charters VALUES (?,?,?,?,?,?,?,?,?,?,?)");
	loadNewFormat();
	loadOldFormat();
}
