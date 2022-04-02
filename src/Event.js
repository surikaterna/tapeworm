class Event {
	constructor(id, type, data, metadata) {
		this.id = id;
		this.type = type;
		this.data = data;
		this.timestamp = new Date();
		this.metadata = metadata || {};
		// Filled by EventStore on append
		this.revision = null;
	}
}

export default Event;