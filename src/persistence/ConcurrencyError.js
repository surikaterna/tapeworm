export class ConcurrencyError extends Error {
	constructor(message) {
		super();
		this.message = message;
	}
}
