var util = require('util');
var Writable = require('stream').Writable;
var elasticsearch = require('elasticsearch');
util.inherits(BulkStream, Writable);

module.exports = BulkStream;

function BulkStream(settings) {

    Writable.call(this, { objectMode: true });

    this._client = new elasticsearch.Client(settings);
}

BulkStream.prototype._write = function(data, encoding, done) {
    var self = this;
    this._client.bulk({body: data}, function (error, response) {
        if (error) {
            console.log(data);
            self.emit('error', new Error(error));
        }
        done();
    });
};
