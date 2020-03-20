const Twitter = require('twitter');

function TwObject() {
    if (process.env.TWIT_CON_KEY && process.env.TWIT_CON_SECRET && process.env.TWIT_ACCESS_KEY && process.env.TWIT_ACCESS_SECRET) {
        let config = {
            consumer_key: process.env.TWIT_CON_KEY,
            consumer_secret: process.env.TWIT_CON_SECRET,
            access_token_key: process.env.TWIT_ACCESS_KEY,
            access_token_secret: process.env.TWIT_ACCESS_SECRET
        };
        this.twitter = new Twitter(config);
    } else {
        console.log('twitter variables are not set');
        this.twitter = null;
    }
}

TwObject.prototype.setParams = function setParams(params) {
    this.params = params;
};

TwObject.prototype.getTweetId = function getTweetId(url) {
    let tws = url.split('/');
    let tws2 = tws[tws.length-1].split('?');
    // console.log(tws);
    // console.log('twId: ' + tws2[0]);
    return tws2[0];
};

TwObject.prototype.getTweet = function getTweet(tweet) {
    // https://twitter.com/Krypto_SouLKinG/status/1225124386128171010?s=19
    let tweetId = this.getTweetId(tweet);
    let params = {id: tweetId};
    let T = this;
    return new Promise((resolve, reject) => {
        T.twitter.get('statuses/show', params, function (err, data) {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
};

TwObject.prototype.getReTweets = function getReTweets(tweet) {
    let tweetId = this.getTweetId(tweet);
    let params = {id: tweetId, count: 100};
    let T = this;
    return new Promise((resolve, reject) => {
        T.twitter.get('statuses/retweets', params, function (err, data) {
            if (err) {
                reject(err);
            } else {
                let objs = [];
                for(let i = 0; i < data.length; i++) {
                    let twObj = data[i];
                    let obj = {};
                    obj.tweet_id = tweetId;
                    obj.retweet_id = twObj.id_str;
                    obj.twit_user_id =  twObj.user.id_str;
                    // obj.id = twObj.id_str;
                    // obj.src_id = tweetId;
                    // obj.user = {name: twObj.user.name, id: twObj.user.id_str, screen_name: twObj.user.screen_name};
                    objs.push(obj);
                }
                resolve(objs);
                // console.log(data);
            }
        });
    });
};

TwObject.prototype.startStream = function startStream(tweet) {
    let tweetId = this.getTweetId(tweet);
    let params = {id: tweetId, count: 100};
    let T = this;

    T.twitter.stream('statuses/retweets', params, function (stream) {
        stream.on('data', function (tweet) {
            console.log(tweet.text);
        });

        stream.on('error', function (error) {
            console.log(error);
        });
    });
};

TwObject.prototype.getTwitterId = function getTwitterId(userName) {
    let uName = userName.replace('@', '');
    let params = {screen_name: uName};
    let T = this;
    return new Promise((resolve, reject) => {
        T.twitter.get('users/show', params, function (err, data) {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
};

module.exports = TwObject;