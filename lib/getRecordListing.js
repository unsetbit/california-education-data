var request = require('request'),
	cheerio = require('cheerio'),
	urllib = require('url'),
	winston = require('winston');

module.exports = function getRecordListing(url, callback){
	request(url, function bodyHandler(err, response, body){
		if(err) return callback(err);
		
		winston.info('Loading record listing from %s', url);

		var records = [];

		$ = cheerio.load(body);

		// Find the <li>s for layout files, they're basically the anchor
		// point for this crawl. The rest of the crawling will be relative
		// to these nodes
		var layoutFileLis = $('#maincontent ul li:first-child').filter(
			function recordLayoutLiSelectionFilter(){
				return ~$(this).text().indexOf('Record Layout');
			}
		);

		winston.info('Found %s entries',  layoutFileLis.length);

		layoutFileLis.each(function(){
			$layoutFileLi = $(this);

			// The <h6> containing the record title
			var recordTitle = $layoutFileLi.parent().prev().text();

			// The text zip file <li>
			var txtZipFileLi = $layoutFileLi.siblings('li').filter(
				function txtZipFileLiSelectionFilter(){
					return ~$(this).text().indexOf('Data File') && ~$(this).text().indexOf('TXT');
				}
			);

			// In case we couldn't find the text zip file node
			if(!~txtZipFileLi.length){
				winston.error('Unable to find data file for %s', recordTitle);
				return;
			}

			var type;
			if(~recordTitle.indexOf('Growth')){
				type = 'growth';
			} else if(~recordTitle.indexOf('Base')) {
				type = 'base';
			} else {
				winston.error('Unable to determine record type for %s', recordTitle);
				return;
			}

			// Determine the report year. For some of the old ones, they were
			// ranges. That's what that if block handles to normalize them to
			// a single year like they did in the later years
			var dateRangeExp = /-(\d+)\s/;
			var reportYear;
			if(dateRangeExp.test(recordTitle)){
				reportYear = parseInt('20' + dateRangeExp.exec(recordTitle)[1], 10);
			} else {
				reportYear = parseInt(recordTitle.split(' ')[0], 10);
			}

			// Get the actual hrefs
			var layoutFileHref = $layoutFileLi.find('a').attr('href');
			layoutFileHref = urllib.resolve(url, layoutFileHref);

			var txtZipFileHref = $(txtZipFileLi).find('a').attr('href');
			txtZipFileHref = urllib.resolve(url, txtZipFileHref);

			records.push({
				type: type,
				reportYear: reportYear,
				title: recordTitle,
				layoutFileHref: layoutFileHref,
				txtZipFileHref: txtZipFileHref
			});
		});
		
		// Sort by report year
		records = records.sort(
			function sortByReportYear(a, b){
				if(a.reportYear > b.reportYear) return -1;
				if(a.reportYear < b.reportYear) return 1;
				return 0;
			}
		);

		callback(null, records);
	});
}