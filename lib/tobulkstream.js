var util = require('util');
var Transform = require('stream').Transform;
util.inherits(ToBulkStream, Transform);

module.exports = ToBulkStream;

function ToBulkStream(type, bulkSize) {
    Transform.call(this, { objectMode: true });

    this._type = type; // only index supported right now
    this._bulk_sz = (bulkSize * 2) || 200; // take header into account
    this._buffer = [];
}

ToBulkStream.prototype._transform = function(data, encoding, done) {
    if (this._buffer.length > this._bulk_sz) {
        this.push(this._buffer);
        this._buffer = [];
    }
    
    var header = {};
    header[this._type] = {_index: data._index, _type: data._type, _id: data._id};
    
    this._buffer.push(header);
    this._buffer.push(data._source);
    
    done();
};

ToBulkStream.prototype._flush = function(done) {
    if(this._buffer.length > 0) {
        this.push(this._buffer);
    }
    
    done();
};
