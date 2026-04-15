var indexeddbjs = require('fake-indexeddb');
global.IDBKeyRange = require("fake-indexeddb/lib/FDBKeyRange");
module.exports = function() {
  indexeddbjs._databases.clear();
  return indexeddbjs;
}
