let util = require('./utils');

let self;

class RpcClient {
    constructor(synapse, ch, callback) {
        self = this;
        self._synapse = synapse;
        self._queue_name = util.format("{0}_{1}_client_{2}", synapse.sys_name, synapse.app_name, synapse.app_id);
        self._router = util.format("client.{0}.{1}", synapse.app_id);
        self._response_cache = {};
        self._channel = ch;
        callback(null, self)
    }

    checkAndCreateQueue(callback) {
        self._channel.assertQueue(self._queue_name, {durable: true, autoDelete: true}).then(() => {
            self._channel.bindQueue(self._queue_name, self._synapse.sys_name, self._router).then(() => {
                callback(null, self)
            });
        })
    }

    run(callback) {
        self._channel.consume(self._queue_name, msg => {
            if (self._synapse.debug) {
                util.log(util.format("RPC Response: ({0}){1}@{2}->{3} {4}", msg.properties.correlationId, msg.properties.type, self._synapse.app_name, msg.properties.replyTo, msg.content), util.LogDebug);
            }
            self._response_cache[msg.properties.correlationId] = JSON.parse(msg.content);
        }, {noAck: true}).then(() => {
            util.log("Event Client Ready");
            callback()
        });
    }

    send(app, method, param) {
        let router = util.format("server.{0}", app);
        let props = {
            appId: self._synapse.app_id,
            messageId: util.randomString(),
            replyTo: self._synapse.app_name,
            type: method
        };
        let body = JSON.stringify(param);
        self._channel.publish(self._synapse.sys_name, router, new Buffer(body), props);
        util.log(util.format("Rpc Request: ({0}){1}->{2}@{3} {4}", props.messageId, app, props.type, self._synapse.app_name, body), util.LogDebug);
        while (true){
            if(self._response_cache.hasOwnProperty(props.messageId)){
                console.log(self._response_cache[props.messageId]);
            }
        }
    }
}

module.exports = RpcClient;
