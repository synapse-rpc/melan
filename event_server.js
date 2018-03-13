let util = require('./utils');

let self;

class EventServer {
    constructor(synapse, ch, callback) {
        self = this;
        self._synapse = synapse;
        self._queue_name = util.format("{0}_{1}_event", synapse.sys_name, synapse.app_name);
        self._channel = ch;
        callback(null, self)
    }

    checkAndCreateQueue(callback) {
        self._channel.assertQueue(self._queue_name, {durable: true, autoDelete: true}).then(() => {
            for (let item in  self._synapse.event_callback) {
                util.log(util.format("*EVT: {0} -> {1}", item, util.getFuncName(self._synapse.event_callback[item])));
                self._channel.bindQueue(self._queue_name, self._synapse.sys_name, 'event.' + item);
            }
            callback(null, self)
        })
    }

    run(callback) {
        self._channel.consume(self._queue_name, msg => {
            if (self._synapse.debug) {
                util.log(util.format("Event Receive: {0}@{1} {2}", msg.properties.type, msg.properties.replyTo, msg.content), util.LogDebug);
            }
            let key = util.format("{0}.{1}", msg.properties.replyTo, msg.properties.type);
            let res = self._synapse.event_callback[key](eval('(' + msg.content.toString() + ')'), msg);
            if (res) {
                self._channel.ack(msg)
            } else {
                self._channel.nack(msg, false, true);
            }
        }, {noAck: false}).then(() => {
            callback()
        });
    }
}

module.exports = EventServer;
