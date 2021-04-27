var should = require("should");
var Promise = require("bluebird");
var uuid = require("node-uuid").v4;

var EventStore = require("..");
var Commit = EventStore.Commit;

describe("Partition", function () {
  describe("#append", function (done) {
    it("should return commit if added", function (done) {
      var es = new EventStore();
      es.openPartition("location")
        .then(function (partition) {
          partition
            .append(new Commit("1", "location", "1", 0, []))
            .then(function (c) {
              c[0].events.length.should.equal(0);
              done();
            });
        })
        .catch(function (err) {
          done(err);
        });
    });
    it("should return after all commits are persisted", function (done) {
      var es = new EventStore();
      es.openPartition("location")
        .then(function (partition) {
          return partition
            .append([
              new Commit("1", "location", "1", 0, []),
              new Commit("2", "location", "1", 1, []),
            ])
            .then(function (c) {
              c.length.should.equal(2);
              done();
            });
        })
        .catch(function (err) {
          done(err);
        });
    });
  });
  describe("#_truncateStreamFrom", function () {
    it("should remove all commits ", function (done) {
      var es = new EventStore();
      es.openPartition("location")
        .then(function (partition) {
          return partition
            .append([
              new Commit("1", "location", "1", 0, []),
              new Commit("2", "location", "1", 1, []),
            ])
            .then(function (c) {
              return partition._truncateStreamFrom("1", 0).then(function () {
                return partition.openStream("1").then(function (stream) {
                  stream.getVersion().should.equal(-1);
                  done();
                });
              });
            });
        })
        .catch(function (err) {
          done(err);
        });
    });
    it("should remove all commits after commitSequence", function (done) {
      var es = new EventStore();
      es.openPartition("location")
        .then(function (partition) {
          return partition
            .append([
              new Commit("1", "location", "1", 0, [{}]),
              new Commit("2", "location", "1", 1, [{}]),
            ])
            .then(function (c) {
              return partition._truncateStreamFrom("1", 1).then(function () {
                return partition.openStream("1").then(function (stream) {
                  stream.getVersion().should.equal(1);
                  done();
                });
              });
            });
        })
        .catch(function (err) {
          done(err);
        });
    });
  });
  describe("#_applyCommitHeader", function () {
    it("should add to commit ", function (done) {
      var es = new EventStore();
      es.openPartition("location")
        .then(function (partition) {
          var commit = new Commit("1", "location", "1", 0, []);
          return partition
            .append([commit, new Commit("2", "location", "1", 1, [])])
            .then(function (c) {
              return partition
                ._applyCommitHeader(commit.id, { authorative: true })
                .then(function (commit) {
                  commit.authorative.should.be.ok;
                  done();
                });
            });
        })
        .catch(function (err) {
          done(err);
        });
    });
    it("should throw if commit id is unknown", function (done) {
      var es = new EventStore();
      es.openPartition("location")
        .then(function (partition) {
          var commit = new Commit("1", "location", "1", 0, []);
          return partition
            .append([commit, new Commit("2", "location", "1", 1, [])])
            .then(function (c) {
              return partition
                ._applyCommitHeader("ID MISSING", { authorative: true })
                .then(function (commit) {
                  done(new Error("Unreachable code"));
                });
            });
        })
        .catch(function (err) {
          done();
        });
    });
  });
  describe("#queryStreamWithSnapshot", function () {
    it("queryStreamWithSnapshot should return snapshot and missing commits", function (done) {
      var es = new EventStore();
      var streamId = "1";
      var stream;
      es.openPartition("location")
        .call("openStream", streamId)
        .then(function (stream1) {
          stream = stream1;
          stream.append({ event: "123" });
          stream.append({ event: "999" });
          return stream.commit(uuid());
        })
        .then(function () {
          stream.append({ event: "666" });
          stream.append({ event: "777" });
          return stream.commit(uuid());
        })
        .then(function () {
          es.openPartition("location").then(function (part) {
            part.storeSnapshot(streamId, { test: "snapshot" }, 2);
            part.queryStreamWithSnapshot(streamId).then(function (res) {
              res.snapshot.version.should.eql(2);
              res.commits.length.should.eql(1);
              res.commits[0].events.length.should.eql(2);
              res.commits[0].events[0].version.should.eql(2);
              done();
            });
          });
        })
        .catch(function (err) {
          done(err);
        });
    });
    it("queryStreamWithSnapshot should return snapshot and no commit if up to date", function (done) {
      var es = new EventStore();
      var streamId = "1";
      es.openPartition("location")
        .call("openStream", streamId)
        .then(function (stream) {
          stream.append({ event: "123" });
          stream.append({ event: "999" });
          return stream.commit(uuid());
        })
        .then(function () {
          es.openPartition("location").then(function (part) {
            part.storeSnapshot(streamId, { test: "snapshot" }, 2);
            part.queryStreamWithSnapshot(streamId, function (err, res) {
              res.snapshot.version.should.eql(2);
              res.commits.length.should.eql(0);
              done();
            });
          });
        })
        .catch(function (err) {
          done(err);
        });
    });
  });
  describe("#delete", function () {
    it("should delete stream and all commits", function (done) {
      var didIGetaDeleteEvent = false;
      var es = new EventStore(null, (commit) => {
        if(commit.events[0].type === '$stream.deleted.event') {
          didIGetaDeleteEvent = true;
        }
      });
      es.openPartition("location")
        .then(function (partition) {
          return partition
            .append([
              new Commit("1", "location", "1", 0, [{type:'dummy.event'}]),
              new Commit("2", "location", "1", 1, [{type:'dummy2.event'}]),
            ])
            .then(function (c) {
              return partition
                .delete("1", { some: "header-value" })
                .then(function () {
                  return partition
                    .openStream("1")
                    .then(function (stream) {
                      done(new Error("able to open deleted stream"));
                    })
                    .catch(function (error) {
                      error.message.should.equal("Stream is deleted");
                      didIGetaDeleteEvent.should.be.true;
                      done();
                    });
                });
            });
        })
        .catch(function (err) {
          done(err);
        });
    });
  });
});
