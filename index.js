'use strict';

var ScrollStream = require('./lib/scrollstream');
var TransformStream = require('./lib/transformstream');
var ToBulkStream = require('./lib/tobulkstream');
var BulkStream = require('./lib/bulkstream');

var argv = process.argv.slice(2);
if (argv.length !== 4) {
    console.error("HOST INDEX TYPE QUERY are required");
    process.exit(-1);
    
}
var HOST  = argv[0];
var INDEX = argv[1];
var TYPE  = argv[2];

var QUERY = JSON.parse(argv[3]);
if (QUERY === undefined) QUERY = { match_all: {} };

var esSettings = {host: HOST};
var esQuery = {
    index: INDEX,
    type: TYPE,
    // Set to 30 seconds because we are calling right back
    scroll: '30s',
    size: 100,
    body: QUERY
};

var ss = new ScrollStream(copy(esSettings), esQuery);
var ts = new TransformStream(
    function(record) {
        var metadata = record._source.metadata;
        if (!metadata || !metadata.properties) return;
        
        var changed = false;
        Object.keys(metadata.properties).forEach(function(property) {
            var value = metadata.properties[property];
            
            if (value === -99999999 || value === -99999997 || value === -99999996) {
                delete metadata.properties[property];
                changed = true;
            }
        });
        
        if (changed) return record;
    }
);

var tb = new ToBulkStream('index', 100);
var bs = new BulkStream( copy(esSettings) );


var counter = 0;
var updateCounter = 0;
var bulkCounter = 0;
ss.on('data', function() {counter++;});
ts.on('data', function() {updateCounter++});
tb.on('data', function() {bulkCounter++});

ss.on('error', function(error) {console.error(error)});

ss.on('end', function() {console.log('[ss] done. %s records read', counter);});
ts.on('end', function() {console.log('[ts] done. %s records transformed', updateCounter)});
tb.on('end', function() {console.log('[tb] done. %s bulk requests prepared', bulkCounter)});
bs.on('finish', function() {console.log('[bs] done.'); process.exit(0); });

ss.pipe(ts).pipe(tb).pipe(bs);

// Util
function copy(o) { return JSON.parse(JSON.stringify(o)); }