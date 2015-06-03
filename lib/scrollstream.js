var Readable = require('stream').Readable;
var util = require('util');
var elasticsearch = require('elasticsearch');

module.exports = ScrollStream;
util.inherits(ScrollStream, Readable);

function ScrollStream(settings, searchBody) {
    // Scroll Stream will return elasticsearch documents
    Readable.call(this, {objectMode: true});
    
    this._client = new elasticsearch.Client(settings);
    this._scroll_id = undefined;
    
    // Create a copy of the search body to prevent tempering afterwards
    this._search_body = JSON.parse(JSON.stringify(searchBody));
    // Add a scroll option if it's not there already
    if (!this._search_body.scroll) this._search_body.scroll = '30s';
    
    this._buffer = [];
}

ScrollStream.prototype._read = function(size) {
    // Push stuff to the buffer
    var more = true;
    while (this._buffer.length > 0) {
        more = this.push(this._buffer.shift());
        if (!more) {
            console.debug('too much data!');
            break;
        }
    }

    // When buffer can handle more data start a new request.
    // First search
    if (more && this._scroll_id === undefined) {
        this._scroll_id = null;
        this._client.search(this._search_body, handleEsResponse);
    }
    
    // Then scroll
    else if (more && this._scroll_id !== null) {
        var scrollBody = {scrollId: this._scroll_id, scroll: '30s'};
        this._scroll_id = null;
        
        this._client.scroll(scrollBody, handleEsResponse);
    }
    
    // Helper function
    var self = this;
    function handleEsResponse (error, response) {
        if (error) {
            self.emit('error', new Error(error.message));
            self.push(null);
            return;
        }
        
        var results = response.hits.hits;
        
        if (results.length === 0) {
            self.push(null);  // No more things to scroll
        } else {
            self._scroll_id = response._scroll_id;
            
            var more = true;
            while (more && results.length > 0) {
                more = self.push(results.shift());
            }
            
            if (!more && results.length > 0) {
                self._buffer.concat(results); // Add everything to the buffer
            }
        }
    }
};
