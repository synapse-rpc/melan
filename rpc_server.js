let util = require('./utils');

let self;

class RpcServer {
    constructor(synapse, ch, callback) {
        self = this;
        self._synapse = synapse;
        self._queue_name = util.format("{0}_{1}_server", synapse.sys_name, synapse.app_name);
        self._router = util.format("server.{0}", synapse.app_name);
        self._channel = ch;
        callback(null, self)
    }

    checkAndCreateQueue(callback) {
        self._channel.assertQueue(self._queue_name, {durable: true, autoDelete: true}).then(() => {
            self._channel.bindQueue(self._queue_name, self._synapse.sys_name, self._router).then(() => {
                for (var item in self._synapse.rpc_callback) {
                    util.log(util.format("*RPC: {0} -> {1}", item, util.getFuncName(self._synapse.rpc_callback[item])));
                }
                callback(null, self)
            });
        })
    }

    run(callback) {
        self._channel.consume(self._queue_name, msg => {
            if (self._synapse.debug) {
                util.log(util.format("RPC Receive: ({0}){1}->{2}@{3} {4}", msg.properties.messageId, msg.properties.replyTo, msg.properties.type, self._synapse.app_name, msg.content), util.LogDebug);
            }
            let res = {'rpc_error': 'method not found'};
            if (self._synapse.rpc_callback.hasOwnProperty(msg.properties.type)) {
                res = self._synapse.rpc_callback[msg.properties.type](JSON.parse(msg.content), msg);
            }
            let reply = util.format("client.{0}.{1}", msg.properties.replyTo, msg.properties.appId);
            let props = {
                appId: self._synapse.app_id,
                messageId: util.randomString(),
                correlationId: msg.properties.messageId,
                replyTo: self._synapse.app_name,
                type: msg.properties.type
            };
            let body = JSON.stringify(res);
            self._channel.publish(self._synapse.sys_name, reply, new Buffer(body), props)
            util.log(util.format("Rpc Return: ({0}){1}@{2}->{3} {4}", msg.properties.messageId, msg.properties.type, self._synapse.app_name, msg.properties.replyTo, body), util.LogDebug);
        }, {noAck: true}).then(() => {
            callback()
        });
    }
}

module.exports = RpcServer;
