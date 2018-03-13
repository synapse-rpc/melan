let amqp = require('amqplib');
let async = require('async');
let util = require('./utils');
let EventServer = require('./event_server');
let RpcServer = require('./rpc_server');
let EventClient = require('./event_client');
let RpcClient = require('./rpc_client');

let self;

class Synapse {
    constructor() {
        self = this;
        self.mq_host = null;
        self.mq_port = 5672;
        self.mq_vhost = '/';
        self.mq_user = null;
        self.mq_pass = null;
        self.sys_name = null;
        self.app_name = null;
        self.app_id = null;
        self.rpc_timeout = 3;
        self.event_process_num = 20;
        self.rpc_process_num = 20;
        self.disable_event_client = false;
        self.disable_rpc_client = false;
        self.debug = false;
        self.rpc_callback = null;
        self.event_callback = null;
        self.callback = null;

        self._conn = null;
        self._event_client = null;
        self._rpc_client = null;

    }

    serve() {
        if (this.sys_name == null || this.app_name == null) {
            util.log('Must Set SysName and AppName system exit .', util.LogError);
            return;
        } else {
            util.log(util.format("System Name: {0}", this.sys_name));
            util.log(util.format("App Name: {0}", this.app_name));
        }
        if (this.app_id == null) {
            this.app_id = util.randomString();
        }
        util.log(util.format("App ID: {0}", this.app_id))
        if (this.debug) {
            util.log("App Run Mode: Debug", util.LogDebug);
        } else {
            util.log("App Run Mode: Production");
        }
        async.waterfall([
            self._createConnection,
            callback => {
                self._createChannel(0, 'Exchange', callback)
            },
            self._checkAndCreateExchange,
            //事件Server
            callback => {
                if (self.event_callback == null) {
                    util.log("Event Server Disabled: event_callback not set", util.LogWarn);
                    callback(null, null);
                } else {
                    self._createChannel(self.event_process_num, 'EventServer', callback)
                }
            },
            (ch, callback) => {
                if (self.event_callback == null) {
                    callback(null, null);
                } else {
                    new EventServer(self, ch, callback);
                }
            },
            (event_server, callback) => {
                if (self.event_callback == null) {
                    callback(null, null);
                } else {
                    event_server.checkAndCreateQueue(callback);
                }
            },
            (event_server, callback) => {
                if (self.event_callback == null) {
                    callback();
                } else {
                    event_server.run(callback);
                }
            },
            //RpcServer
            callback => {
                if (self.rpc_callback == null) {
                    util.log("Rpc Server Disabled: rpc_callback not set", util.LogWarn);
                    callback(null, null);
                } else {
                    self._createChannel(self.rpc_process_num, 'RpcServer', callback);
                }
            },
            (ch, callback) => {
                if (self.rpc_callback == null) {
                    callback(null, null);
                } else {
                    new RpcServer(self, ch, callback);
                }
            },
            (rpc_server, callback) => {
                if (self.rpc_callback == null) {
                    callback(null, null);
                } else {
                    rpc_server.checkAndCreateQueue(callback);
                }
            },
            (rpc_server, callback) => {
                if (self.rpc_callback == null) {
                    callback();
                } else {
                    rpc_server.run(callback);
                }
            },
            //事件Client
            callback => {
                if (self.disable_event_client) {
                    util.log("Event Client Disabled: disable_event_client set true", util.LogWarn);
                    callback(null, null);
                } else {
                    self._createChannel(0, 'EventClient', callback)
                }
            },
            (ch, callback) => {
                if (self.disable_event_client) {
                    callback(null, null);
                } else {
                    new EventClient(self, ch, callback);
                }
            },
            (event_client, callback) => {
                if (self.disable_event_client) {
                    callback();
                } else {
                    self._event_client = event_client;
                    event_client.run(callback);
                }
            },
            //RpcClient
            callback => {
                if (self.disable_rpc_client) {
                    util.log("Rpc Client Disabled: disable_rpc_client set true", util.LogWarn);
                    callback(null, null);
                } else {
                    self._createChannel(0, 'RpcClient', callback);
                }
            },
            (ch, callback) => {
                if (self.disable_rpc_client) {
                    callback(null, null);
                } else {
                    new RpcClient(self, ch, callback);
                }
            },
            (rpc_client, callback) => {
                if (self.disable_rpc_client) {
                    callback(null, null);
                } else {
                    self._rpc_client = rpc_client;
                    rpc_client.checkAndCreateQueue(callback);
                }
            },
            (rpc_client, callback) => {
                if (self.disable_rpc_client) {
                    callback();
                } else {
                    rpc_client.run(callback);
                }
            }
        ], res => {
            if (self.callback != null) {
                self.callback(self)
            }
        });
    }

    sendEvent(event, param) {
        self._event_client.send(event, param);
    }

    sendRpc(app, action, param) {
        return self._rpc_client.send(app, action, param);
    }

    _createConnection(callback) {
        let config = {
            protocol: 'amqp',
            hostname: self.mq_host,
            port: self.mq_port,
            username: self.mq_user,
            password: self.mq_pass,
            frameMax: 0,
            heartbeat: 0,
            vhost: self.mq_vhost,
        };
        amqp.connect(config).then(conn => {
            self._conn = conn;
            util.log("Rabbit MQ Connection Created.");
            callback()
        });
    }

    _createChannel(process_num, desc = 'unknow', callback) {
        self._conn.createChannel().then(ch => {
            util.log(util.format("{0} Channel Created", desc));
            if (process_num > 0) {
                ch.prefetch(process_num, false).then(() => {
                    util.log(util.format("{1} MaxProcessNum: {0}", process_num, desc));
                    callback(null, ch)
                });
            } else {
                callback(null, ch)
            }
        });
    }

    _checkAndCreateExchange(ch, callback) {
        ch.assertExchange(self.sys_name, 'topic', {durable: true, autoDelete: true}).then(() => {
            util.log("Register Exchange Successed.");
            ch.close().then(() => {
                util.log("Exchange Channel Closed");
                callback()
            });
        });
    }
}

module.exports = Synapse;