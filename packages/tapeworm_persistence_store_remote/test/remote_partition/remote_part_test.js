var should = require('should');
var Tapeworm = require('tapeworm');
var Commit = require('tapeworm').Commit;
var SocketMock = require('socket.io-mock');
var TapewormSyncServer = require('../../lib/server');
var RemoteDriver = require('../../lib/client').RemotePersistence;
var Client = require('../../lib/client').RemoteClient;

var autobus = {
  leave: function () {}
};

describe('Remote partition server/client', function () {
  var socketServer, socketClient, clientTapeworm, tapeworm, serverPartition = null, clientPartition = null;

  beforeEach(function (done) {
    socketServer = new SocketMock();
    socketClient = socketServer.socketClient;
    var rrtwClient = new Client(socketClient, '/tw');
    var remotePartition = new RemoteDriver(rrtwClient, 'blondie');

    clientTapeworm = new Tapeworm(remotePartition);
    tapeworm = new Tapeworm();
    tapeworm.openPartition('blondie').then(function(blondie) {
      serverPartition = blondie;
      clientTapeworm.openPartition('blondie').then(function (blondie2) {
        clientPartition = blondie2;
        done();
      });
    });
  });

  it('#socketMock should work', function (done) {
    socketClient.on('ping', function (message) {
      message.should.equal('Hello');
      socketClient.emit('pong', 'heya');
    });
    socketServer.on('pong', function (message) {
      message.should.equal('heya');
      done();
    });
    socketServer.emit('ping', 'Hello');
  });

  it('#loadSnapshot should work on remote partition', function (done) {
    const tapewormSyncServer = new TapewormSyncServer(serverPartition, autobus);
    serverPartition.storeSnapshot('1', { test: 'success' }, 2, function () {
      clientPartition.loadSnapshot('1', function (err, res) {
        res.snapshot.test.should.equal('success');
        done();
      });
    });
    tapewormSyncServer.addSocket(socketServer);
  });
  it('#loadSnapshot should work on remote partition with promise', function (done) {
    const tapewormSyncServer = new TapewormSyncServer(serverPartition, autobus);
    serverPartition.storeSnapshot('1', { test: 'success' }, 2, function () {
      clientPartition.loadSnapshot('1').then(function (res) {
        res.snapshot.test.should.equal('success');
        done();
      });
    });
    tapewormSyncServer.addSocket(socketServer);
  });
  it('#loadSnapshot should include events not persisted in snapshot', function (done) {
    var commits = [
      new Commit('1', 'blondie', '1', 0, [{id:'1', type:'registered'}]),
      new Commit('2', 'blondie', '1', 1, [{id:'1', type:'amended1'}, {id:'1', type:'amended2'}]),
      new Commit('3', 'blondie', '1', 2, [{id:'1', type:'amended3'}, {id:'1', type:'amended4'}]),
    ];
    const tapewormSyncServer = new TapewormSyncServer(serverPartition, autobus);
    serverPartition.storeSnapshot('1', { test: 'success' }, 2, function () {
      serverPartition.append(commits).then(function() {
        clientPartition.loadSnapshot('1', true).then(function (res) {
          res.snapshot.snapshot.test.should.equal('success');
          res.commits[0].streamId.should.equal('1');
          res.commits[0].events.length.should.equal(2);
          done();
        });
      });
    });
    tapewormSyncServer.addSocket(socketServer);
  });
  it('#queryStreamWithSnapshot should include events not persisted in snapshot', function (done) {
    var commits = [
      new Commit('1', 'blondie', '1', 0, [{id:'1', type:'registered'}]),
      new Commit('2', 'blondie', '1', 1, [{id:'1', type:'amended1'}, {id:'1', type:'amended2'}]),
      new Commit('3', 'blondie', '1', 2, [{id:'1', type:'amended3'}, {id:'1', type:'amended4'}]),
    ];
    const tapewormSyncServer = new TapewormSyncServer(serverPartition, autobus);
    serverPartition.storeSnapshot('1', { test: 'success' }, 2, function () {
      serverPartition.append(commits).then(function() {
        clientPartition.queryStreamWithSnapshot('1').then(function (res) {
          res.snapshot.snapshot.test.should.equal('success');
          res.commits[0].streamId.should.equal('1');
          res.commits[0].events.length.should.equal(2);
          done();
        });
      });
    });
    tapewormSyncServer.addSocket(socketServer);
  });
  it('#queryStream should work on remote partition', function (done) {
    var commits = [new Commit('1', 'blondie', '1', 0, [{ id: '1', type: 'registered' }]), new Commit('2', 'blondie', '1', 1, [{ id: '1', type: 'amended' }])];
    const tapewormSyncServer = new TapewormSyncServer(serverPartition, autobus);
    serverPartition.append(commits).then(function () {
      clientPartition.queryStream('1', function (err, res) {
        res.length.should.equal(2);
        res[0].events[0].type.should.equal('registered');
        res[1].events[0].type.should.equal('amended');
        done();
      });
    });
    tapewormSyncServer.addSocket(socketServer);
  });
  it('#queryStream should work on remote partition with promise', function (done) {
    var commits = [new Commit('1', 'blondie', '1', 0, [{ id: '1', type: 'registered' }]), new Commit('2', 'blondie', '1', 1, [{ id: '1', type: 'amended' }])];
    const tapewormSyncServer = new TapewormSyncServer(serverPartition, autobus);
    serverPartition.append(commits).then(function () {
      clientPartition.queryStream('1').then(function (res) {
        res.length.should.equal(2);
        res[0].events[0].type.should.equal('registered');
        res[1].events[0].type.should.equal('amended');
        done();
      });
    });
    tapewormSyncServer.addSocket(socketServer);
  });
  it('#queryStream should work on remote partition from commit sequence', function (done) {
    var commits = [new Commit('1', 'blondie', '1', 0, [{ id: '1', type: 'registered' }]), new Commit('2', 'blondie', '1', 1, [{ id: '1', type: 'amended' }])];
    const tapewormSyncServer = new TapewormSyncServer(serverPartition, autobus);
    serverPartition.append(commits).then(function () {
      clientPartition.queryStream('1', 0, function (err, res) {
        res.length.should.equal(1);
        res[0].events[0].type.should.equal('amended');
        done();
      });
    });
    tapewormSyncServer.addSocket(socketServer);
  });
});
