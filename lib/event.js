var Event = function(id, type, data, metadata) {
	this.id = id;
	this.type = type;
	this.data = data;
	this.timestamp = new Date();
	this.metadata = metadata || {};
	this.revision = null; //filled by eventstore on append
}

module.exports = Event;