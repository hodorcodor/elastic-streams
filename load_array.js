'use strict';

var JSONStream = require('jsonstream');
var TransformStream = require('./lib/transformstream');
var ToBulkStream = require('./lib/tobulkstream');
var BulkStream = require('./lib/bulkstream');

var argv = process.argv.slice(2);
if (argv.length !== 3) {
    console.error("HOST INDEX TYPE are required");
    console.error("Usage: node load_array.js HOST INDEX TYPE < data.json");
    process.exit(-1);
    
}
var HOST  = argv[0];
var INDEX = argv[1];
var TYPE  = argv[2];

var esSettings = {host: HOST};

var ts = new TransformStream(
    function(record) {
        var returnValue = {
            _index: INDEX,
            _type: TYPE,
            _source: record
        };

        // Do some transforming here


        return returnValue;
    }
);

var tb = new ToBulkStream('index', 100);
var bs = new BulkStream( copy(esSettings) );

// -- Logging things
var counter = 0;
var updateCounter = 0;
var bulkCounter = 0;

ts.on('data', function() {updateCounter++});
tb.on('data', function() {bulkCounter++});


ts.on('end', function() {console.log('[ts] done. %s records transformed', updateCounter)});
tb.on('end', function() {console.log('[tb] done. %s bulk requests prepared', bulkCounter)});
bs.on('finish', function() {console.log('[bs] done.'); process.exit(0); });

// -- Actual piping
process.stdin.resume();
process.stdin.pipe(JSONStream.parse('*')).pipe(ts).pipe(tb).pipe(bs);


// Util
function copy(o) { return JSON.parse(JSON.stringify(o)); }