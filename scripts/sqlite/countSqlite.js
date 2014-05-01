var sqlite = require('sqlite3');

var db = new sqlite.Database('../../data/sacs.sqlite3', function(){
	db.serialize(function() {
		//var stmt = "SELECT * FROM UserGL where Fiscalyear = '2003' AND Ccode = '01' AND Dcode = '99901'";
		var stmt = 'DESCRIBE UserGL';
		db.get(stmt, function(){
			console.log(arguments);
		});
		
	});
});