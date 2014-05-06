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
		db.run("DROP TABLE IF EXISTS Alternate_Form_Data");
		db.run("CREATE TABLE Alternate_Form_Data (Ccode TEXT, Dcode TEXT, SchoolID TEXT, ObjectCode TEXT, restricted NUMERIC, unrestricted NUMERIC, total NUMERIC)", fillit);
	});
});

function fillit(){
	var insert = db.prepare("INSERT INTO Alternate_Form_Data VALUES (?,?,?,?,?,?,?)");

	var year = years.pop();
	console.log('Started loading', year);	

	csv.fromPath('../../data/csv/alt/' + year + '/Alternate_Form_Data.csv', {headers:true}).on('record', function(row){
		insert.run(
			row.Ccode === undefined? row.ccode : row.Ccode, 
			row.Dcode === undefined? row.dcode : row.Dcode, 
			row.SchoolID === undefined? row.schoolid : row.SchoolID, 
			row.ObjectCode === undefined? row.objectcode : row.ObjectCode, 
			row.restricted, 
			row.unrestricted, 
			row.total
		);
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

function transformRow(year, row){
	switch year {
		case "2003":
			normalize2003(row);
			break;
		case "2004":
			normalize2004(row);
			break;
		case "2005":
			normalize2005(row);
			break;
		case "2006":
			normalize2006(row);
			break;
		case "2007":
			normalize2007(row);
			break;
		case "2008":
			normalize2008(row);
			break;
		case "2009":
			normalize2009(row);
			break;
		case "2010":
			normalize2010(row);
			break;
		case "2011":
			normalize2011(row);
			break;
		case "2012":
			normalize2012(row);
			break;
	}
}

function normalize2012(row){
	row.API_BASE = row.API12B;
	delete row.API12B;

	// stopped being reported after 2010
	row.CMA_ADJ_ELA = null;
	row.CMA_ADJ_MATH = null;

	// stopped being reported after 2009
	row.CMA_ADJ_SCI = null;

	// stopped being reported after 2007
	row.VNRT_R28 = null; 
	row.PNRT_R28 = null; 
	row.CW_NRTR = null; 
	row.VNRT_L28 = null; 
	row.PNRT_L28 = null; 
	row.CW_NRTL = null; 
	row.VNRT_S28 = null; 
	row.PNRT_S28 = null; 
	row.CW_NRTS = null; 
	row.VNRT_M28 = null; 
	row.PNRT_M28 = null; 
	row.CW_NRTM = null; 
	row.CMA_ADJ = null;
}

function normalize2011(row){
	row.API_BASE = row.API11B;
	delete row.API11B;

	// stopped being reported after 2010
	row.CMA_ADJ_ELA = null;
	row.CMA_ADJ_MATH = null;

	// stopped being reported after 2009
	row.CMA_ADJ_SCI = null;

	// stopped being reported after 2007
	row.VNRT_R28 = null; 
	row.PNRT_R28 = null; 
	row.CW_NRTR = null; 
	row.VNRT_L28 = null; 
	row.PNRT_L28 = null; 
	row.CW_NRTL = null; 
	row.VNRT_S28 = null; 
	row.PNRT_S28 = null; 
	row.CW_NRTS = null; 
	row.VNRT_M28 = null; 
	row.PNRT_M28 = null; 
	row.CW_NRTM = null; 
	row.CMA_ADJ = null;
}

function normalize2010(row){
	row.API_BASE = row.API10B;
	delete row.API10B;

	// stopped being reported after 2009
	row.CMA_ADJ_SCI = null;

	// stopped being reported after 2007
	row.VNRT_R28 = null; 
	row.PNRT_R28 = null; 
	row.CW_NRTR = null; 
	row.VNRT_L28 = null; 
	row.PNRT_L28 = null; 
	row.CW_NRTL = null; 
	row.VNRT_S28 = null; 
	row.PNRT_S28 = null; 
	row.CW_NRTS = null; 
	row.VNRT_M28 = null; 
	row.PNRT_M28 = null; 
	row.CW_NRTM = null; 
	row.CMA_ADJ = null;
}

function normalize2009(row){
	row.API_BASE = row.API09B;
	delete row.API09B;

	// stopped being reported after 2007
	row.VNRT_R28 = null; 
	row.PNRT_R28 = null; 
	row.CW_NRTR = null; 
	row.VNRT_L28 = null; 
	row.PNRT_L28 = null; 
	row.CW_NRTL = null; 
	row.VNRT_S28 = null; 
	row.PNRT_S28 = null; 
	row.CW_NRTS = null; 
	row.VNRT_M28 = null; 
	row.PNRT_M28 = null; 
	row.CW_NRTM = null; 
	row.CMA_ADJ = null;
}

function normalize2008(row){
	row.API_BASE = row.API08B;
	delete row.API08B;

	// started being reported after 2008
	row.MR_NUM = null;
	row.MR_SIG = null;
	row.MR_API = null;
	row.MR_GT = null;
	row.MR_TARG = null;	
	row.PCT_MR = null;

	// stopped being reported after 2007
	row.VNRT_R28 = null; 
	row.PNRT_R28 = null; 
	row.CW_NRTR = null; 
	row.VNRT_L28 = null; 
	row.PNRT_L28 = null; 
	row.CW_NRTL = null; 
	row.VNRT_S28 = null; 
	row.PNRT_S28 = null; 
	row.CW_NRTS = null; 
	row.VNRT_M28 = null; 
	row.PNRT_M28 = null; 
	row.CW_NRTM = null;
	row.CMA_ADJ = null;
}

function normalize2007(row){
	row.API_BASE = row.API07B;
	delete row.API07B;

	// started being reported after 2008
	row.MR_NUM = null;
	row.MR_SIG = null;
	row.MR_API = null;
	row.MR_GT = null;
	row.MR_TARG = null;	
	row.PCT_MR = null;
}

function normalize2006(row){
	row.API_BASE = row.API06B;
	delete row.API06B;

	// started being reported after 2008
	row.MR_NUM = null;
	row.MR_SIG = null;
	row.MR_API = null;
	row.MR_GT = null;
	row.MR_TARG = null;	
	row.PCT_MR = null;

	// starter being reported after 2007
	row.CMA_ADJ = null;

	// changed names after 2006
	row.CW_SCI = row.CW_LS10;
	delete row.CW_LS10;
}


function normalize2003(row){
	row.API = row.API03;
	delete row.API03;
}