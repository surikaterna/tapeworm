export class DuplicateCommitError extends Error {
  constructor(message: string) {
    super(message);
    this.message = message;
  }
}
