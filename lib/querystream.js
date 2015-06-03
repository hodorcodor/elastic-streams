var Readable = require('stream').Readable;
var util = require('util');

module.exports = QueryStream;
util.inherits(QueryStream, Readable);

function QueryStream(searchFn) {
    // Query Stream will return elasticsearch documents
    Readable.call(this, {objectMode: true});
    
    this._searchFn = searchFn;
}

QueryStream.prototype._read = function(size) {
    this._searchFn(null, handle(this));
    
    function handle(thisArg) {
        var self = thisArg;

        return function (error, result) {
            if (error) {
                self.emit('error', new Error(error.message));
                return;
            }
            var hits = result.hits.hits;
            if (hits.length > 0) {
                hits.forEach(
                    function (hit) {
                        this.push(hit);
                    }
                    , self
                );
            } else {
                self.push(null);
            }
        }
    }
};
