// Creates some useful indices

var sqlite = require('sqlite3');

var db = new sqlite.Database('../../data/sqlite/cal-ed-annual-financial-data.sqlite3', function(){
	db.serialize(function() {
		db.run("CREATE INDEX IF NOT EXISTS Dcode_idx ON UserGL(Dcode ASC)");
		db.run("CREATE INDEX IF NOT EXISTS Value_idx ON UserGL(Value ASC)");
		db.run("CREATE INDEX IF NOT EXISTS Object_idx ON UserGL(Object ASC)");
		db.run("CREATE INDEX IF NOT EXISTS Fiscalyear_idx ON UserGL(Fiscalyear ASC)");
	});
});
