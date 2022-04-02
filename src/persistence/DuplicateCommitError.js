export class DuplicateCommitError extends Error {
  constructor(message) {
    super();
    this.message = message;
  }
}
