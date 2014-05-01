var zlib = require('zlib');
var fs = require('fs');
var path = require('path');

var walk = function(dir, done) {
  var results = [];
  fs.readdir(dir, function(err, list) {
    if (err) return done(err);
    var pending = list.length;
    if (!pending) return done(null, results);
    list.forEach(function(file) {
      file = dir + '/' + file;
      fs.stat(file, function(err, stat) {
        if (stat && stat.isDirectory()) {
          walk(file, function(err, res) {
            results = results.concat(res);
            if (!--pending) done(null, results);
          });
        } else {
          results.push(file);
          if (!--pending) done(null, results);
        }
      });
    });
  });
};

walk('../../data/csv', function(err, files){
	files.forEach(function(file){
		if(path.extname(file) !== '.csv') return;

		var inp = fs.createReadStream(file);
		var out = fs.createWriteStream(file + '.gz');
		inp.pipe(zlib.createGzip()).pipe(out);
	});
});