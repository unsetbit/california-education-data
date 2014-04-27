# Public CDE Data
CDE is California Department of Education

## Usage
You must first install [Node.js](http://nodejs.org/)

```
git clone https://github.com/oztu/california-education-data.git
cd california-education-data
node unzipCsvs.js
```

The data will be available in `data/csv`. 
* `data/csv/api` is the Acedemic Performance Index data from http://www.cde.ca.gov/ta/ac/ap/apidatafiles.asp . 
* `data/csv/sacs` and `data/csv/alt` are the Annual Financial Data from http://www.cde.ca.gov/ds/fd/fd/ .

## Updating the data sets
Here is how the data was generated (and how to update it):

### Academic Performance Index (API)
The Academic Performance Index data can be updated by running `npm install; node getAPIFiles.js;`. The data will be automatically crawled and put inside the `tmp` folder.

The files are retrieved from http://www.cde.ca.gov/ta/ac/ap/apidatafiles.asp. The data files are converted to CSV and the layout html pages are converted to JSON files. For convenience, a header line is added to the CSV which is the names of the fields from the layout file.

### Annual Financial  Data (SACS and ALT)
This data is a bit trickier to update since they're distributed as self-extracting .EXEs here: http://www.cde.ca.gov/ds/fd/fd/. I've downloaded the exes, extracted them, then ran the resulting .mdb files against [mdbToCsv](https://github.com/oztu/mdbToCsv) to generate to csv files. Finally, I ran `node zipCsvs.js` to gzip them in order to appease GitHub's limitation on large file sizes.
