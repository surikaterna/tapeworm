import util from "node:util";

interface IDuplicateCommitError extends Error {
  message: string;
}

var DuplicateCommitError = function (this: IDuplicateCommitError, message: string) {
  Error.call(this);
  this.message = message;
} as unknown as new (
  message: string
) => IDuplicateCommitError;

util.inherits(DuplicateCommitError, Error);

export default DuplicateCommitError;
