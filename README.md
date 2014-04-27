Data crawled from California Department

# Academic Performance Index (API) Data
This data is in `data/csv/api`

The origin for this data is here: http://www.cde.ca.gov/ta/ac/ap/apidatafiles.asp

To update this data: download this git repo, execute npm install, then run `node run.js`. The data will be written to the tmp folder within the repo.

The files are retrieved from http://www.cde.ca.gov/ta/ac/ap/apidatafiles.asp. The data files are converted to CSV and the layout html pages are converted to JSON files. For convenience, a header line is added to the CSV which is the names of the fields from the layout file.

# Annual Financial  Data (SACS and ALT)
This data is in `data/csv/sacs` and `data/csv/alt`. Due to githubs limitation on large files I had to gzip the csvs, you can run `node unzip.js` to unzip them all.

The origin for this data is here: http://www.cde.ca.gov/ds/fd/fd/

This data is a bit trickier to update since they're distributed as self-extracting exes. I've downloaded the exes, extracted them, resulting in .mdb files, then ran [mdbToCsv](https://github.com/oztu/mdbToCsv) against them to generate to csv files. I then ran `node zip.js` to gzip them in order to appease github's limitation on large file sizes.

