import util from "node:util";

interface IConcurrencyError extends Error {
  message: string;
}

var ConcurrencyError = function (this: IConcurrencyError, message: string) {
  Error.call(this);
  this.message = message;
} as unknown as new (
  message: string
) => IConcurrencyError;

util.inherits(ConcurrencyError, Error);

export default ConcurrencyError;
