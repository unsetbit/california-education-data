var csv = require('csv'),
	fs = require('fs'),
	path = require('path'),
	winston = require('winston'),
	
	getRecordListing = require('./getRecordListing'),
	getLayout = require('./getLayout'),
	getDataCSVStream = require('./getDataCSVStream');


module.exports = function crawl(url, baseDir){
	function createDataFile(record){
		getLayout(record.layoutFileHref, function(err, layout){
			var stream = getDataCSVStream(record.txtZipFileHref, layout);
			
			var baseFilePath = path.resolve(baseDir, record.reportYear + "-" + record.type + '-api');
			
			var dataFilePath = baseFilePath + '.csv';
			winston.info('Started writing %s', dataFilePath);
			
			stream.pipe(fs.createWriteStream(dataFilePath)).on('finish', function(){
				winston.info('Finished writing %s', dataFilePath);
			});
			
			var layoutFilePath = baseFilePath + '-layout.json';
			winston.info('Started writing %s', layoutFilePath);
			fs.writeFile(layoutFilePath, JSON.stringify(layout), function(){
				winston.info('Finished writing %s', layoutFilePath);
			});

		});
	}

	getRecordListing(url, function(err, records){
		records.forEach(createDataFile);
	});
}
