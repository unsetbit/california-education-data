var crawl = require('./lib/crawl');
var fs = require('fs');

var recordsUrl = 'http://www.cde.ca.gov/ta/ac/ap/apidatafiles.asp';
var destinationDir =  __dirname + '/tmp';

if (!fs.existsSync(destinationDir)) {
	fs.mkdirSync(destinationDir);
}

crawl(recordsUrl , destinationDir);