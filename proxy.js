// for SOCKS-proxy connection
const Agent = require('socks5-https-client/lib/Agent');

module.exports = {
    getProxy() {
        return {    // use SOCKS-proxy
            agentClass: Agent,
            agentOptions: {
                socksHost: '127.0.0.1',
                socksPort: '1080',
                // If authorization is needed:
                // socksUsername: process.env.PROXY_SOCKS5_USERNAME,
                // socksPassword: process.env.PROXY_SOCKS5_PASSWORD
            }
        }
    }
}

