var util = require('util');
var Transform = require('stream').Transform;
util.inherits(TransformStream, Transform);

module.exports = TransformStream;

function TransformStream(transformFn) {

  Transform.call(this, { objectMode : true });

  this._buffer = [];
  this._transformFn = transformFn;
}

TransformStream.prototype._transform = function(data, encoding, done) {
    var newData = this._transformFn(data);
    if (newData !== undefined) this.push(newData);
    done();
};
