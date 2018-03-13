let util = require('./utils');

let self;

class EventClient {
    constructor(synapse, ch, callback) {
        self = this;
        self._synapse = synapse;
        self._channel = ch;
        callback(null, self)
    }

    run(callback) {
        util.log("Event Client Ready");
        callback();
    }

    send(event, param) {
        let router = util.format("event.{0}.{1}", self._synapse.app_name, event);
        let props = {
            appId: self._synapse.app_id,
            messageId: util.randomString(),
            replyTo: self._synapse.app_name,
            type: event
        };
        let body = JSON.stringify(param);
        self._channel.publish(self._synapse.sys_name, router, new Buffer(body), props);
        util.log(util.format("Event Publish: {0}@{1} {2}", props.type, self._synapse.app_name, body), util.LogDebug);
    }
}

module.exports = EventClient;
