import Promise from 'bluebird';
import {v4 as uuid} from 'uuid';
import {EventStream} from '../src/EventStream';
import {EventStore} from '../src/EventStore';

describe('event_stream', function () {
  describe('#openStream', function (done) {
    it('should return 0 commits for new stream', function (done) {
      var es = new EventStore();
      es.openPartition('location').call('openStream', '1').then(function (stream) {
        expect(stream.getCommittedEvents()).toHaveLength(0);
        done();
      }).catch(function (err) {
        done(err);
      });
    });
  });
  describe('#commit', function (done) {
    it('should do nothing if nothing has been appended', function (done) {
      var es = new EventStore();
      es.openPartition('location').call('openStream', '1').then(function (stream) {
        stream.commit(uuid());
        expect(stream.getCommittedEvents()).toHaveLength(0);
        done();
      }).catch(function (err) {
        done(err);
      });
    });

    it('should call commit on partition', function (done) {
      var mockPartition = {
        called: false,
        append: function (commit, callback) {
          this.called = true;
          return Promise.resolve().nodeify(callback);
        },
        _queryStream: function (streamId, callback) {
          return Promise.resolve([]).nodeify(callback);
        }
      };
      var stream = new EventStream(mockPartition, '11');
      stream.append({event: '123'});
      stream.commit(uuid());
      expect(mockPartition.called).toBe(true);
      done();
    });
    it('should keep track of uncommitted events', function (done) {
      var es = new EventStore();
      es.openPartition('location').call('openStream', '1').then(function (stream) {
        stream.append({event: '123'});
        expect(stream.getUncommittedEvents()).toHaveLength(1);
        done();
      }).catch(function (err) {
        done(err);
      });
    });
    it('should move uncommitted events to committed on commit', function (done) {
      var es = new EventStore();
      var stream;
      es.openPartition('location').call('openStream', '1').then(function (stream1) {
        stream = stream1;
        stream.append({event: '123'});
        return stream.commit(uuid());
      })
        .then(function () {
          expect(stream.getUncommittedEvents()).toHaveLength(0);
          expect(stream.getCommittedEvents()).toHaveLength(1);
          done();
        }).catch(function (err) {
        done(err);
      });
    });
    it('two events becomes one commit', function (done) {
      var es = new EventStore();
      var stream;
      es.openPartition('location').call('openStream', '1').then(function (stream1) {
        stream = stream1;
        stream.append({event: '123'});
        stream.append({event: '999'});
        return stream.commit(uuid());
      })
        .then(function () {
          expect(stream.getCommittedEvents()).toHaveLength(2);
          expect(stream._commitSequence).toBe(0);
          done();
        }).catch(function (err) {
        done(err);
      });
    });
    it('two commits gets increasing commit sequence', function (done) {
      var es = new EventStore();
      var stream;
      es.openPartition('location').call('openStream', '1').then(function (stream1) {
        stream = stream1;
        stream.append({event: '123'});
        stream.append({event: '999'});
        return stream.commit(uuid());
      })
        .then(function () {
          stream.append({event: '666'});
          stream.append({event: '777'});
          return stream.commit(uuid());
        })
        .then(function () {
          expect(stream.getCommittedEvents()).toHaveLength(4);
          expect(stream._commitSequence).toBe(1);
          done();
        }).catch(function (err) {
        done(err);
      });
    });
    it('event stream writeOnly', function (done) {
      var es = new EventStore();
      var stream;
      es.openPartition('location').then(function (partition) {
        partition.openStream('1', true)
          .then(function (stream1) {
            stream = stream1;
            stream.append({event: '123'});
            return stream.commit(uuid());
          })
          .then(function () {
            expect(stream._commitSequence).toBe(0);
            stream.append({event: '666'});
            stream.append({event: '777'});
            return stream.commit(uuid());
          })
          .then(function () {
            expect(stream._commitSequence).toBe(1);
            done();
          })
          .catch(function (err) {
            done(err);
          });
      });
    });
    it('committed events should have increasing version', function (done) {
      var es = new EventStore();
      var stream;
      es.openPartition('location').call('openStream', '1').then(function (stream1) {
        stream = stream1;
        stream.append({event: '123'});
        stream.append({event: '999'});
        return stream.commit(uuid());
      })
        .then(function () {
          stream.append({event: '666'});
          stream.append({event: '777'});
          return stream.commit(uuid());
        })
        .then(function () {
          expect(stream.getCommittedEvents()[3].version).toBe(3);
          expect(stream._version).toBe(4);
          done();
        }).catch(function (err) {
        done(err);
      });
    });
    it('committed events should have increasing version (writeOnly)', function (done) {
      var es = new EventStore();
      var stream;
      es.openPartition('location').then(function (partition) {
        partition.openStream('1', true)
          .then(function (stream1) {
            stream = stream1;
            stream.append({event: '123'});
            stream.append({event: '999'});
            return stream.commit(uuid());
          })
          .then(function () {
            stream.append({event: '666'});
            stream.append({event: '777'});
            return stream.commit(uuid());
          })
          .then(function () {
            expect(stream._version).toBe(4);
            done();
          })
          .catch(function (err) {
            done(err);
          });
      });
    });
    it('published events should have increasing version', function (done) {
      var commitCount = 0;

      var es = new EventStore(null, function (commit) {
        expect(commit.events[0]).toHaveProperty('version');
        if (++commitCount == 2) {
          done();
        }
      });
      var stream;
      es.openPartition('location').call('openStream', '1').then(function (stream1) {
        stream = stream1;
        stream.append({event: '123'});
        stream.append({event: '999'});
        return stream.commit(uuid());
      })
        .then(function () {
          stream.append({event: '666'});
          stream.append({event: '777'});
          return stream.commit(uuid());
        })
        .then(function () {
          expect(stream.getCommittedEvents()[1].version).toBe(1);
        }).catch(function (err) {
        done(err);
      });
    });
  });
});
