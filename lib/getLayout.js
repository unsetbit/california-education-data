var request = require('request'),
	cheerio = require('cheerio'),
	winston = require('winston');

module.exports = function layoutFileHandler(url, callback){
	request(url, function(err, response, body){
		if(err) return callback(err);
		
		winston.info('Loading layout file handler from %s', url);

		var fields = [];

		$ = cheerio.load(body);
		
		// The row with the align attribute is the header, so we exclude that
		// but the rest are actual fields	
		$('#maincontent table tr:not([align])').each(function(index, row){
			$row = $(row);
			
			var index = parseInt($row.children(0).text().trim(), 10),
				name = $row.children(1).text().trim(),
				type = $row.children(2).text().trim(),
				width = parseInt($row.children(3).text().trim(), 10),
				description = $row.children(4).text().trim();

			fields.push({
				index: index,
				name: name,
				type: type,
				width: width,
				description: description
			});
		});

		fields = fields.sort(
			function sortByIndex(a,b){
				if(a.index < b.index) return -1;
				if(a.index > b.index) return 1;
				return 0;
			}
		);

		callback(null, fields);
	});
}