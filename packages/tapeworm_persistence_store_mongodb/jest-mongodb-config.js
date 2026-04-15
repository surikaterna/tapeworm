module.exports = {
  mongodbMemoryServerOptions: {
    binary: {
      version: '5.0.15',
      skipMD5: true
    },
    autoStart: false,
    instance: {},
    replSet: {
      count: 1,
      storageEngine: 'wiredTiger'
    }
  }
};
