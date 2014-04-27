var request = require('request'),
	winston = require('winston'),
	zip = require('zip'),
	csv = require('csv'),
	es = require('event-stream');

module.exports = function getDataFileCSVStream(url, layout){
	winston.info('Loading data file from %s', url);

	var stream = es.through();

	request(url, {encoding: null}, function(err, response, body){
		
		var reader = zip.Reader(body);
		
		reader.forEach(function(entry){
			var rows = [];
			var row;

			// Header
			var row = layout.map(function(item){
				return item.name;
			});
			rows.push(row);

			// Body
			entry.getData().toString().split('\n').forEach(function(line){
				var index = 0;
				var row = layout.map(function(item){
					var value = line.substr(index, item.width).trim();
					index += item.width;
					return value;
				});
				rows.push(row);
			});

			csv().from(rows).to.stream(stream);
		});
	});

	return stream;
}
