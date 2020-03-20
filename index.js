// Fix "Automatic enabling of cancellation of promises is deprecated" error.
// See:
//  1. https://github.com/yagop/node-telegram-bot-api/issues/319
//  2. https://github.com/yagop/node-telegram-bot-api/issues/484
process.env.NTBA_FIX_319 = '1';

const TelegramBot = require('node-telegram-bot-api');
const MongoClient = require('mongodb').MongoClient;
const Exec = require('child_process').exec;
const helper = require("./helpers");
const Twit = require('./tweets');

helper.logStart();

let token;
// Globals
if (process.env.BOT_TOKEN) {
    token = process.env.BOT_TOKEN;
} else {
        console.error('Error: Token is empty!\nPlease set your token in BOT_TOKEN env variable');
        process.exit(1);
}

const dbconfig = require('./config.json');
const dburl = process.env.MONGO_URI ? process.env.MONGO_URI : 'mongodb://localhost:27017';
const dbName = dbconfig.mongo.db;
const rpcHost = dbconfig.rpcServer.host;
const catchPhrases = require('./CatchPhrasesEn.json') ;
const ChatStateEnum = {"NormalState":0,
    "StartState":3,
    "WaitDefValueState":6,
    "SetWalletState": 7,
    "SetTwitUserState":8,
    "SetKeyState":9};
Object.freeze(ChatStateEnum);

// config params
const devWallet = dbconfig.devFee.wallet;
const toDeleteMessage = dbconfig.global.deleteMessages; // delete bot messages after timeout
let deleteMessageInterval = dbconfig.global.deleteInterval;
const giveAutoMult = dbconfig.global.giveAutoMult;
const isDebugMode = dbconfig.global.debugMode;
let txInterval = dbconfig.global.txInterval;
let giveFee = dbconfig.devFee.giveFee;
let tipFee  = dbconfig.devFee.tipFee;
let rainFee  = dbconfig.devFee.rainFee;
let retweetInterval = dbconfig.global.retweetInterval;
let devChatId =  dbconfig.devFee.devChat;
const pm2name = dbconfig.global.pm2name;
const txExplorerLink = dbconfig.global.txExplorer;
const chatCmdMod = dbconfig.global.cmdMod;

let restartTimeOut = true;

const stdTxFee = 0.0003;
// const oneHourMs = 60 * 60 * 1000;

// blockchain lib
const BnbApiClient = require('@binance-chain/javascript-sdk');
const bnbClient = new BnbApiClient(rpcHost);
// noinspection JSUnresolvedFunction
bnbClient.chooseNetwork("mainnet");
// noinspection JSUnresolvedFunction
bnbClient.initChain();

let chatStateMap = new Map();
let giveawayStateMap = new Map();
let globalBotUserName;
let globalBotId;
let chatDbMap = new Map();
let userGiveTakeMap = new Map();
let giveAutoMap = new Map();
let giveAutoTimeoutHandle = new Map();
let twitterTimeoutHandle = new Map();
let chatAdminsSet = new Set();

let globalTxQuery = [];

// Create a new MongoClient
const mongoClient = new MongoClient(dburl, { useUnifiedTopology: true });
let mongoCloDb = null;
let mongoUsersTable = null;
let mongoOperationsTable = null;
let mongoChatsTable = null;
let mongoQueueTable = null;
let mongoTweetsTable = null;
let mongoRetweetsTable = null;

// Connect to Mongo DB
mongoClient.connect()
    .then(function (client){
            console.log("Connected successfully to Mongo server");
            mongoCloDb = client.db(dbName);
            mongoUsersTable = mongoCloDb.collection('users');
            mongoOperationsTable = mongoCloDb.collection('operations');
            mongoChatsTable = mongoCloDb.collection('chats');
            mongoQueueTable = mongoCloDb.collection('rain_que');
            mongoTweetsTable = mongoCloDb.collection('tweets');
            mongoRetweetsTable = mongoCloDb.collection('retweets');
    })
    .catch(e => {
            console.log(e);
    });

const twitter = new Twit();

// const options = {
//     webHook: {
//         port: 8443,
//         key: '/bnbbot/goog_tg_bot_pk.key', // Path to file with PEM private key
//         cert: '/bnbbot/goog_tg_bot_pk.pem' // Path to file with PEM certificate
//     }
// };
//
// const url = "https://bot.ip:8443";
// const bot = new TelegramBot(token, options);
//
// bot.setWebHook(`${url}/bot${token}`, {
//     certificate: options.webHook.cert,
// });

let botOptions = {};
if (dbconfig.global.useProxy) {
    const proxy = require("./proxy");
    botOptions = proxy.getProxy();
}

const bot = new TelegramBot(token, {
    polling: {
        interval: 300,
        autoStart: true,
        params: {
            timeout: 10
        }
    },
    request: botOptions
});

bot.getMe().then(function(me)
{
    console.log('Hello! My name is %s!', me.first_name);
    console.log('My id is %s.', me.id);
    console.log('And my username is @%s.', me.username);
    globalBotUserName = '@' + me.username;
    globalBotId = me.id;
    let msg = 'Bot (re)started!';
    sendMessageByBot(devChatId, msg, 0, false);
    getChatAdmins();
    sendTxByInterval().then(() => {}); // start Tx process timer
    checkRetweetTimer();
    setTimeout(finishedRestart, 1000);
});

// Bot receives Message from Channel
bot.on('channel_post', function(msg)
{
    console.log('got channel_post');
    parseMessage(msg, true);
});

// Bot receives TextMessage (from chat or from user)
bot.on('text', function(msg)
{
    let chatId = msg.chat.id;
    let userId = msg.from.id;
    if (chatId !== userId) {
        bot.getChatMember(chatId, userId)
            .then((chatUser, error) => {
               if (error) {
                   console.log('telegram error: ', error);
               } else {
                   let isAdmin = false;
                   if ((chatUser.status === 'creator') || (chatUser.status === 'administrator')) isAdmin = true;
                   parseMessage(msg, isAdmin);
               }
            });
    } else {
        parseMessage(msg, false);
    }
});

// Bot respond on CallBack (e.g. InlineKeyboard)
bot.on('callback_query', function onCallbackQuery(callbackQuery)
{
    console.log('got callback');
    let action = callbackQuery.data;
    let strs = action.split(/(\s+)/).filter( e => e.trim().length > 0);
    if (strs[0] === 'give') { // giveaway msg button
        checkGiveCallback(callbackQuery, strs);
    } else if (strs[0] === 'tks') { // select token
        finSelTokenCallback(callbackQuery, strs);
    } else {
        bot.answerCallbackQuery(callbackQuery.id)
            .catch(error => {
                console.log('TG error: ' + error);
            });
    }
});

function hideCallbackButtons(aCallback) {
    let msg = aCallback.message;
    bot.answerCallbackQuery(aCallback.id)
        .catch(error => {
            console.log('TG error: ' + error);
        });
    let messageText = msg.text;
    let options = {
        chat_id: msg.chat.id,
        message_id: msg.message_id,
        parse_mode: 'HTML',
        reply_markup: null
    };
    bot.editMessageText(messageText, options)
        .catch(error => {
            console.log('TG error: ' + error);
        });
}

function finishedRestart() {
    restartTimeOut = false;
}

function updateTokenName(aUserId, aChatId, aTokenName) {
    let token = aTokenName.toUpperCase().trim();

    let knownToken = false;
    Object.keys(dbconfig.tokens).forEach(function (key) {
        console.log(key, ' | ', dbconfig.tokens[key]);
        if (key === token) {
            knownToken = true;
        }
    });

    let messageText;
    if (knownToken) {
        mongoUsersTable.updateOne({user_id: aUserId},
            {$set: {def_token: token}})
            .catch(e => {
                console.log('mongo error: ', e);
            });
        messageText = catchPhrases.privateDialogMessages[35] + '<code>' + token + '</code>';
    } else {
        messageText = catchPhrases.debugCommandMessages[16];
    }
    sendMessageByBot(aChatId, messageText);
}

function finSelTokenCallback(aCallback, aParams) {
    let messageUserId = aCallback.from.id;
    let msg = aCallback.message;
    hideCallbackButtons(aCallback);
    updateTokenName(messageUserId.toString(), msg.chat.id, aParams[1]);
}

// Process giveaway callback
function checkGiveCallback(aCallback, aParams) {
    console.log('claim giveaway : ', aCallback.from.id);
    let messageUserId = aCallback.from.id;
    let msg = aCallback.message;
    if (aParams[2] === 'cancel') {
        if ((messageUserId.toString() === aParams[1]) || (getAdminRights(aCallback.from.username))) {
            console.log('Cancelled by ' + messageUserId);
            let messageText = msg.text + '\nCancelled.';
            let options = {
                chat_id: msg.chat.id,
                message_id: msg.message_id,
                parse_mode: 'HTML',
                reply_markup: null
            };
            giveawayStateMap.delete(msg.message_id);
            bot.editMessageText(messageText, options)
                .catch(error => {
                    console.log('TG error: ' + error);
                });
            if (toDeleteMessage ) {
                setTimeout(deleteBotMessage, deleteMessageInterval, msg);
            }
        } else {
            let options = {
                text: 'You can not cancel the claim',
                show_alert: true
            };
            bot.answerCallbackQuery( aCallback.id, options)
                .catch(error => {
                    console.log('TG error: ' + error);
                });
        }
    } else {
        if (messageUserId.toString() === aParams[1]) {
            let options = {
                text: 'You can not claim - but You can Cancel :)',
                show_alert: true
            };
            bot.answerCallbackQuery( aCallback.id, options)
                .catch(error => {
                    console.log('TG error: ' + error);
                });
        } else {
            if (!(giveawayStateMap.has(msg.message_id))) {
                processGiveawayClaim(aCallback);
            } else {
    //            console.log('not now');
               bot.answerCallbackQuery(aCallback.id)
                   .catch(error => {
                       console.log('TG error: ' + error);
                   });
            }
        }
    }
}

// GiveAway operation final
function cbkFinalizeGive(aSourceItem, aDestItem, aGiveAmount, aCallbackQuery, aTxId) {
    let aMsg = aCallbackQuery.message;
    let aMessageChatId = aMsg.chat.id;
    let messageUserId = aCallbackQuery.from.id;

    logOperationDb("3", aSourceItem, aDestItem, aGiveAmount, aTxId, aMessageChatId);
    let mUserName = formatUserName2 (aDestItem);
    let tokenName = getTokenName(aSourceItem);
    let messageText = mUserName + ' claimed ' + aGiveAmount + ' ' + tokenName;
    console.log(messageText);
    sendMessageByBot(aMessageChatId, messageText, messageUserId, true);//, generateMainKeyboard(messageUserId));

    // if dest User is registered - send him private MSG
    if (aDestItem.user_id) {
        let mmUserName = formatUserName2 (aSourceItem);
        messageText = 'You received ' + aGiveAmount + ' ' + tokenName + ' from ' +  mmUserName;
        sendMessageByBot(aDestItem.user_id, messageText, aDestItem.user_id, false);
    }

    messageText = aMsg.text + '\nClaimed by ' + mUserName;
    let options = {
        chat_id: aMsg.chat.id,
        message_id: aMsg.message_id,
        parse_mode: 'HTML',
        reply_markup: null
    };
    bot.editMessageText(messageText, options)
        .catch(function(error) {
           console.log('error editing Tg message: ' + error);
        });
    if (toDeleteMessage ) {
        setTimeout(deleteBotMessage, deleteMessageInterval, aMsg);
    }
}

function prepareTx(aSourceItem, aDestItem, aValue, aPcnt, aTokenName, aMsg, aCallBack) {
    let dest = [];
    let destItems = [];
    let token = dbconfig.tokens[aTokenName];
    let fee = calcFee(aValue, aPcnt, aSourceItem.user_id);

    console.log('fee: ', fee);
    let val = aValue - fee;
    console.log('val: ', val);
    if (fee > 0) {
        dest.push({
            to: devWallet,
            coins: [{
                denom: token,
                amount: fee
            }]
        });
    }
    dest.push({to: aDestItem.wallet_address,
        coins: [{
            denom: token,
            amount: val
        }]
    });
    destItems.push(aDestItem);

    addTxToQuery(aSourceItem, dest, destItems, aValue, aMsg, aCallBack);
}

function checkGiveUser(aCallbackQuery, aItem, aSourceItem) {
    let messageUserId = aCallbackQuery.from.id;
    let messageChatId = aCallbackQuery.message.chat.id;
    if (aItem) {
        let action = aCallbackQuery.data;
        let strs = action.split(/(\s+)/).filter( e => e.trim().length > 0);
        let giveAmount = Number(strs[2].replace(',', '.'));
        let tokenName = strs[3];
        prepareTx(aSourceItem, aItem, giveAmount, giveFee, tokenName, aCallbackQuery, cbkFinalizeGive);
    } else {
        let messageText = catchPhrases.debugCommandMessages[12] + globalBotUserName;
        sendMessageByBot(messageChatId, messageText, messageUserId, true);
    }
}

function checkDonorGiveUser(aCallbackQuery, aItem) {
    let messageUserId = aCallbackQuery.from.id;
    isUserIdRegistered(messageUserId, checkGiveUser, aCallbackQuery, aItem);
}

function processGiveawayClaim(aCallbackQuery) {
    giveawayStateMap.set(aCallbackQuery.message.message_id, 0);
    console.log('locked giveaway: ' + aCallbackQuery.message.message_id);
    // test if user already take it in 24h
    let hours = 25;
    if (userGiveTakeMap.has(aCallbackQuery.from.id)) {
        let lastDate = userGiveTakeMap.get(aCallbackQuery.from.id);
        let nowDate = new Date();
        hours = (nowDate.getTime() - lastDate.getTime()) / 3600000;
        console.log('hours since claim :' + hours);
    }
    if ((hours > 24) || (getAdminRights(aCallbackQuery.from.username))) {
        userGiveTakeMap.set(aCallbackQuery.from.id, new Date());
        let action = aCallbackQuery.data;
        let strs = action.split(/(\s+)/).filter(e => e.trim().length > 0);
        let donorId = (strs[1]);
//        console.log('giveaway 1 : ' + donorId);
        isUserIdRegistered(donorId, checkDonorGiveUser, aCallbackQuery);
    } else {
        giveawayStateMap.delete(aCallbackQuery.message.message_id);
        let options = {
            text: 'Wait ' + Math.round(24-hours) + ' hours before next Claim',
            show_alert: true
        };
        bot.answerCallbackQuery( aCallbackQuery.id, options)
            .catch(error => {
                console.log('TG error: ' + error);
            });
    }
}

function checkChatId(aMsg) {
    let chat = aMsg.chat;
    if (!(chatDbMap.has(chat.id))) {
        console.log('new chat : ' + chat.id);
        mongoChatsTable.updateOne({ chat_id: chat.id},
            {$set: { chat_id: chat.id,
            user_name: chat.username,
            type: chat.type,
            title: chat.title,
            description: chat.description
        }},
            { upsert: true } ).catch(e => {
                console.log(e);
        });
        let item = {chat_id: chat.id,
            user_name: chat.username,
            type: chat.type,
            title: chat.title,
            description: chat.description};
        chatDbMap.set(chat.id, item);
    }
}

// parse messages (from users, chats, channels
function parseMessage(msg, aAdmin) {
    // Set main variables
    let messageText = msg.text;
    let messageChatId = msg.chat.id;
    let messageId = msg.message_id;
    let chatType = msg.chat.type;
    let messageUserName = '';

    // noinspection JSUnresolvedVariable
    if (msg.forward_date || messageChatId === devChatId) { // Skip All Forwarded Messages
        return;
    }

    if (chatType !== 'private') {
        checkChatId(msg);
        if ((messageText.indexOf(`/${chatCmdMod}stat`) === 0) || (messageText.indexOf(`/${chatCmdMod}stat${globalBotUserName}`) === 0 )) {
            console.log('statistics');
            doStat(msg);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, msg);
            }
            return;
        }

        if (!aAdmin) return; // all chat operations only for admins
        if ((messageText.indexOf(`/${chatCmdMod}tip`) === 0)) {
            console.log('tip user');
                doTip(msg);
            return;
            // Call giveaway function
        } else if ((messageText.indexOf(`/${chatCmdMod}giveaway`) === 0)) {
            console.log('giveaway value');
            doGiveaway(msg);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, msg);
            }
            return;
        } else if ((messageText.indexOf('/token') === 0)) {
            console.log('get active token');
            doCheckToken(msg);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, msg);
            }
            return;
        } else if ((messageText.indexOf(`/${chatCmdMod}rain`) === 0)) {
            console.log('rain value');
            doRain(msg);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, msg);
            }
            return;
        } else if ((messageText.indexOf(`/${chatCmdMod}rtrain`) === 0)) {
            console.log('twitter rain');
            doTwitterRain(msg);
            // if (toDeleteMessage) {
            //     setTimeout(deleteBotMessage, deleteMessageInterval * 2, msg);
            // }
            return;
        } else if ((messageText.indexOf(`/${chatCmdMod}tweet`) === 0) || (messageText.indexOf(`/${chatCmdMod}twit`) === 0) ) {
            console.log('start tweet monitoring');
            startTweet(msg);
            // if (toDeleteMessage) {
            //     setTimeout(deleteBotMessage, deleteMessageInterval * 2, msg);
            // }
            return;
        } else if ((messageText.indexOf(`/${chatCmdMod}giveauto`) === 0)) {
            console.log('giveaway cyclic');
            doGiveawayAuto(msg);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, msg);
            }
            return;
        } else if ((messageText.indexOf(`/${chatCmdMod}givestop`) === 0)) {
            console.log('stop giveaway cyclic');
            doGiveawayStop(msg);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, msg);
            }
            return;
        } else {  // skip all commands if called from chat
            return;
        }
    } else {
        messageUserName = msg.from.username;
    }

    // Register new User
    if ((messageText.indexOf('/register') === 0) || (messageText === catchPhrases.mainKeyboard[0])) {
        console.log('register wallet');
        setWalletAddress(msg);
    }

    // START COMMAND
    else if (messageText === '/start') {
        console.log('start command');
        isChatAdminUser(msg);
        setTimeout(startCommand, 2000, msg);
    }

    // main menu
    else if ((messageText === '/mainmenu') || (messageText === catchPhrases.mainKeyboard[3])) {
        sendMessageByBot(messageChatId, catchPhrases.mainKeyboard[5],
            messageChatId, false, generateMainKeyboard(messageChatId));
    }

    // settings menu
    else if ((messageText === '/settings') || (messageText === catchPhrases.mainKeyboard[4])) {
        sendMessageByBot(messageChatId, '⚙️ Settings Menu',
            messageChatId, false, generateSettingsKeyboard(messageChatId));
    }

    // default tip value
    else if ((messageText === '/defval') || (messageText === catchPhrases.settingsKeyboard[0])) {
        getDefaultTip(msg);
    }

    // default tip value
    else if ((messageText === '/seltoken') || (messageText === catchPhrases.settingsKeyboard[5])) {
        getDefaultToken(msg);
    }

    else if ((messageText === '/twituser') || (messageText === catchPhrases.settingsKeyboard[3]) ) {
        console.log('get twitter user info');
        setTwitUser(msg);
        // View user's balance
    } else if ((messageText.indexOf('/balance') === 0) || (messageText === catchPhrases.mainKeyboard[2])) {
        console.log('querying balance');
        getUserBalance(msg);
    }

    // Call GiveAway
    else if ((messageText.indexOf(`/${chatCmdMod}giveaway`) === 0) || (messageText.indexOf(`/${chatCmdMod}rain`) === 0)
    || (messageText.indexOf(`/${chatCmdMod}giveauto`) === 0) ||  (messageText.indexOf(`/${chatCmdMod}stat`) === 0)) {
        sendReplyMessageByBot(messageChatId, messageText + catchPhrases.debugCommandMessages[6], messageId);
    }

    // View HELP
    else if ((messageText.indexOf('/help') === 0) || (messageText === catchPhrases.mainKeyboard[1])) {
        console.log('show command list');
        // noinspection JSUnresolvedVariable
        console.log('language code :' + msg.from.language_code);
        sendMessageByBot(messageChatId, generateHelpString(messageUserName),
            messageChatId, false, generateMainKeyboard(messageChatId));
    }

    // View OPERATIONS
    else if ((messageText.indexOf('/operations') === 0) || (messageText === catchPhrases.settingsKeyboard[4])) {
        console.log('show operations list');
        queryOperationsFromDb(msg);
    }

    // View PrivateKey
    else if ((messageText === catchPhrases.settingsKeyboard[2])) {
        console.log('set private key');
        setPrivateKey(msg);
    }

    // process user REPLY results
    else if (chatStateMap.has(messageChatId)) {
        console.log('reply ? : ' + messageChatId);
        if (!isBlank( messageText.trim())) {
            if (chatStateMap.get(messageChatId) !== ChatStateEnum.NormalState) {
                doOperationByState(msg)
            } else {
                sendMessageByBot(messageChatId, catchPhrases.mainKeyboard[5],
                    messageChatId, false, generateMainKeyboard(messageChatId));
            }
        } else {
            sendMessageByBot(messageChatId, catchPhrases.mainKeyboard[5],
                messageChatId, false, generateMainKeyboard(messageChatId));
        }
    }

    // default
    else  {
        console.log('default reply');
        if (getAdminRights(messageUserName)) { // admin commands
            console.log('check admin commands');
            if ((messageText.indexOf('/message') === 0) ) {
                console.log('send message to user');
                adminMsgUser(msg);
            }
            else if ((messageText.indexOf('/msgallusers') === 0) ) {
                console.log('send message to all user');
                adminMsgAllUsers(msg);
            }
            else if ((messageText.indexOf('/msgallchats') === 0) ) {
                console.log('send message to all Chats');
                adminMsgAllChats(msg);
            }
            else if ((messageText.indexOf('/chatlist') === 0) ) {
                console.log('get chat list');
                adminGetChatList(msg);
            }
            else if ((messageText.indexOf('/config') === 0) ) {
                console.log('get config list');
                adminGetConfig(msg);
            }
            else if ((messageText.indexOf('/botrestart') === 0) ) {
                console.log('restart the bot');
                adminBotRestart(msg);
            }
            else if ((messageText.indexOf('/botstop') === 0) ) {
                console.log('stopping the bot');
                adminBotStop(msg);
            }
        } else {
            console.log('default message');
            sendMessageByBot(messageChatId, catchPhrases.mainKeyboard[5], messageChatId,
                false, generateMainKeyboard(messageChatId));
        }
    }
}

function setPrivateKey(aMsg) {
    let chatId = aMsg.chat.id;
    let userId = aMsg.from.id;
    let id = userId.toString();
    mongoUsersTable.findOne({user_id: id}, {}, function(err, item) {
        if (err) {
            console.log('mongo error: ' + err);
        }
        let messageText = catchPhrases.privateDialogMessages[28];
        if (item) {
            if (item.wallet_key) {
                let key = item.wallet_address;
                messageText = catchPhrases.privateDialogMessages[29];
                messageText += `<code>${key}</code>\n`;
                messageText += catchPhrases.privateDialogMessages[30];
            }
        }
        sendMessageByBot(chatId, messageText, userId, false);
        chatStateMap.set(userId, ChatStateEnum.SetKeyState);
    });
}

function setWalletAddress(aMsg) {
    let chatId = aMsg.chat.id;
    let userId = aMsg.from.id;
    let id = userId.toString();
    mongoUsersTable.findOne({user_id: id}, {}, function(err, item) {
        if (err) {
            console.log('mongo error: ' + err);
        }
        let messageText = catchPhrases.privateDialogMessages[23] + catchPhrases.privateDialogMessages[24];
        if (item) {
            if (item.wallet_address) {
                let adr = item.wallet_address;
                messageText = catchPhrases.privateDialogMessages[25];
                messageText += `<code>${adr}</code>\n`;
                messageText += catchPhrases.privateDialogMessages[26];
            }
        }
        sendMessageByBot(chatId, messageText, userId, false);
        chatStateMap.set(userId, ChatStateEnum.SetWalletState);
    });
}

function setTwitUser(aMsg) {
    let chatId = aMsg.chat.id;
    let userId = aMsg.from.id;
    let id = userId.toString();
    mongoUsersTable.findOne({user_id: id}, {}, function(err, item) {
        if (err) {
            console.log('mongo error: ' + err);
        }
        let messageText = catchPhrases.privateDialogMessages[8] + ' ' + catchPhrases.privateDialogMessages[11];
        if (item) {
            if (item.twit_user_name) {
                let twName = item.twit_user_name;
                messageText = catchPhrases.privateDialogMessages[9] + '@' + twName;
                messageText += '\n' + catchPhrases.privateDialogMessages[11];
                messageText += '\n' + catchPhrases.privateDialogMessages[10];
            }
        }
        sendMessageByBot(chatId, messageText, userId, false);
        chatStateMap.set(userId, ChatStateEnum.SetTwitUserState);
    });
}

// Start tweet monitoring
function startTweet(aMsg) {
    // check if tweet is already active
    let text = aMsg.text;
    let chatId = aMsg.chat.id;
    let strs = text.split(/(\s+)/).filter( e => e.trim().length > 0);
    let cntr = 0;
    let twLink ;
    if (strs.length > 2) { // assume 2nd param as Target counter
        cntr = Number(strs[1]);
        twLink = strs[2];
    } else {
        twLink = strs[1];
        cntr = 0;
    }
    console.log(`Tweet: ${cntr} ${twLink}`);
    let tweetId = twitter.getTweetId(twLink);
    console.log(`Tweet ID: ${tweetId}`);
    let messageText;
    mongoTweetsTable.findOne({tweet_id: tweetId}, (err, dbItem) => {
        if (err) {
            console.log('mongo error: ' + err);
        } else {
            if (dbItem) { // tweet exists
                if (dbItem.is_active) {                     // if active - show retweets count
                    // call ReTweet counter function
                    console.log(`Active tweet`);
                    getRetweets(aMsg, dbItem);
                } else {
                    console.log(`Finished tweet`);
                    messageText = catchPhrases.twitterMessages[0]; // tweet finished
                    sendMessageByBot(chatId, messageText, chatId, false);
                }
            } else { // if not active - query retweets
                console.log(`New tweet`);
                let tweetObj = {};
                tweetObj.tweet_id = tweetId;
                tweetObj.tweet_link = twLink;
                tweetObj.retweet_tagret = cntr;
                tweetObj.is_active = 1;
                tweetObj.chat_id = chatId;
                tweetObj.date_reg = new Date();
                // call ReTweet counter function
                getRetweets(aMsg, tweetObj);
            }
        }
    });
}

function checkRetweetTimer() {
    if (twitterTimeoutHandle.has(0)) {
        // do nothing - timer is active
    } else {
        startRetweetTimer();
    }
}

function startRetweetTimer() {
    if (retweetInterval > 0) {
        let handle = setTimeout(onRetweetTimer, retweetInterval * 60 * 1000); // every 5 minutes
        twitterTimeoutHandle.set(0, handle);
    }
}

function onRetweetTimer() {
    twitterTimeoutHandle.delete(0);
    // get active tweets list
    mongoTweetsTable.find({is_active: 1}).toArray((err, result) => {
       if (err) {
           console.log(`mongo error ${err}`);
       } else {
           if (result.length > 0 ) {
               let msg = {chat: {id:0}, from: {id:0}};
               result.forEach((item) => {
                   getRetweets(msg, item);
               });
               checkRetweetTimer();
           } else {                // do not restart timer
               console.log('no active tweets - no timer started');
           }
       }
    });
}

function getRetweetsCount(aMsg, aTweetObj) {
    let chatId = aMsg.chat.id;
    mongoRetweetsTable.countDocuments({tweet_id: aTweetObj.tweet_id}, {}, (err, dbCount) => {
        if (err) {
            console.log('mongo error %s', err);
        } else {
            aTweetObj.retweet_count = dbCount;
            console.log(`RetweetsCount = ${dbCount}`);
            mongoTweetsTable.updateOne({tweet_id: aTweetObj.tweet_id}, {     // save tweet to DB
                $set: {
                    // tweet_id: aTweetObj.tweet_id,
                    tweet_link: aTweetObj.tweet_link,
                    user_id: aTweetObj.user_id,
                    chat_id: aTweetObj.chat_id,
                    retweet_tagret: aTweetObj.retweet_tagret,
                    retweet_count: aTweetObj.retweet_count,
                    is_active: aTweetObj.is_active,
                    date_reg:  aTweetObj.date_reg
                }
            }, { upsert: true } )
                .catch(e => {
                    console.log(e);
                });
            // show current retweets count
            if (chatId) {
                let messageText = catchPhrases.twitterMessages[1] + dbCount;
                sendMessageByBot(chatId, messageText, chatId, false);
                // start Retweet timer
                checkRetweetTimer();
            }
        }
    });
}

function getRetweets(aMsg, aTweetObj) {
    twitter.getReTweets(aTweetObj.tweet_id)
        .then((data) => {
            for (let i = 0; i < data.length; i++) {
                let obj = data[i];     // save retweets  (upsert - to)
                mongoRetweetsTable.updateOne({retweet_id: obj.retweet_id}, {
                    $set: {
                        tweet_id: obj.tweet_id,
                        twit_user_id: obj.twit_user_id
                    }},
                    { upsert: true } )
                    .catch(e => {
                        console.log(e);
                    });
            }
            setTimeout(getRetweetsCount, 1000, aMsg, aTweetObj);
        })
        .catch(e => {
            console.log('tweet error: ' + e);
        })
}

// Get Chats known to bot
function adminGetChatList(aMsg) {
    let message = "";
    mongoChatsTable.find().forEach(function (doc1) {
        message += 'ID: ' + doc1.chat_id;
        message += ' | NAME: @' + doc1.user_name;
        message += ' | Title: ' + doc1.title + '\n';
    }, function (err) {
        if (!err) {
            sendMessageByBot(aMsg.chat.id, message);
        } else {
            console.log(err);
        }
    })
}

function adminMsgAllChats(aMsg) {
    let message = "";
    let list = "";
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    let ps = messageText.indexOf(strs[1]);
    if (ps > 0) {
        message += messageText.substr(ps, messageText.length);
    }
    if (message.length) {
        mongoChatsTable.find().forEach(function (doc1) {
            list += 'Msg sent to ';
            list += ' @' + doc1.user_name;
            list += ' | Title: ' + doc1.title + '\n';
            sendMessageByBot(doc1.chat_id, message);
        }, function (err) {
            if (!err) {
                sendMessageByBot(aMsg.chat.id, list);
            }
        })
    }
}

function adminMsgAllUsers(aMsg) {
    let message = "";
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    let ps = messageText.indexOf(strs[1]);
    if (ps > 0) {
        message += messageText.substr(ps, messageText.length);
    }
    let cnt = 0;
    if (message.length) {
        mongoUsersTable.find({inactive:null}).forEach(function (doc1) {
            sendMessageByBot(doc1.user_id, message, doc1.user_id, false, generateMainKeyboard(doc1.user_id));
            cnt++;
        }, function (err) {
            if (!err) {
                sendMessageByBot(aMsg.chat.id, 'send msg Finished : ' + cnt);
            }
        })
    }
}

function adminMsgUser(aMsg) {
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    let dbUserId = Number(strs[1]);
    let message = "";
    let ps = messageText.indexOf(strs[2]);
    if (ps > 0) {
        message += messageText.substr(ps, messageText.length);
    }
    sendMessageByBot(dbUserId, message, dbUserId);
}

// list chat users registered in db
function getChatUsersList(aMsg, aCallback, aCallBackParam, aUsersList, aGetAll = false, aChatId) {
    let chatList = [];
    let fixedNum = 0;
    if (aUsersList) {
        aUsersList.forEach(function (item) {
            chatList.push(item);
            console.log(item.userName);
        });
        fixedNum = chatList.length;
        console.log("fixed list counts : " + fixedNum);
    }
    let chatId = aGetAll ? aChatId : aMsg.chat.id;
    console.log('chatID: ' + chatId);
    // let userId = 0;
    // if (aMsg.chat.type !== 'channel') {
        let userId = aMsg.from.id;
    // }
    console.log('query users from DB');
    // select all except values from rain_que
    mongoQueueTable.distinct("user_id", {chat_id: aGetAll ? 0 : aMsg.chat.id}, function (err, res) {
        if (err) {
            console.log(err);
        }
        let queue;
        if (aGetAll) {
            queue = [{
                $match: {
                    user_id: {$nin: res}
                }
            }];
        } else {
        queue = [{
            $match: {
                user_id: {$nin: res},
                $or: [{inactive: null}, {inactive: "nochat"}]
            }
        }];
        }
        mongoUsersTable.aggregate(queue).toArray()
            .then( function (doc1) {
            let docLng = doc1.length;
            let cnt = 0;
            doc1.forEach(function (item) {
                let dbUserId = item.user_id;
                if ((dbUserId !== userId.toString())  ) { // exclude calling user
                    bot.getChatMember(chatId, dbUserId)
                        .then(function (chatUser) {
                            console.log('chat user: ' + item.user_id + ' : ' + chatUser.user.id + ' : ' + item.user_name );
                            if ((chatUser &&  (chatUser.status !== 'left')
                                && (chatUser.status !== 'kicked')) || (aGetAll)) {
                                if (item.wallet_address) { // exclude users without wallets
                                    let userName = formatUserName2(item);
                                    let objUser = {
                                        userId: item.user_id,
                                        userName: userName,
                                        userItem: item,
                                        userStatus: chatUser.status,
                                        userActive: item.inactive,
                                        userActDate: item.inactive_date,
                                        twit_user_id: item.twit_user_id
                                    };
                                    chatList.push(objUser);
                                }
                            }
                            cnt++;
                            if (cnt === docLng) {
                                aCallback(aMsg, chatList, aCallBackParam, fixedNum);
                            }
                        })
                        .catch(function () {
                            cnt++;
                            if (cnt === docLng) {
                                aCallback(aMsg, chatList, aCallBackParam, fixedNum);
                            }
                        });
                } else {
                    docLng--;
                    if (cnt === docLng) {
                        aCallback(aMsg, chatList, aCallBackParam, fixedNum);
                    }
                }
            });
        })
    })
}

function getRandomInt(aMin, aMax) {
    return Math.floor(Math.random() * (aMax - aMin + 1)) + aMin;
}

function isBlank(aString) {
    return (!aString || /^\s*$/.test(aString));
}

// get admins list from DB on start
function getChatAdmins() {
    mongoUsersTable.find({is_admin: {$ne: null}}, function(err, doc) {
        if (err) {
            console.log('mongo error: ', err);
        } else {
            doc.forEach(function (item) {
                chatAdminsSet.add(Number(item.user_id));
            });
        }
    });
}

function startCommand(aMsg) {
    let userId = aMsg.from.id;
    let isAdmin = chatAdminsSet.has(userId);
    let messageText = isAdmin ? catchPhrases.privateDialogMessages[21] : catchPhrases.privateDialogMessages[22];
    sendMessageByBot(userId, messageText, userId, false, generateMainKeyboard(userId) );
}

function isChatAdminUser(aMsg) {
    let aUserId = aMsg.from.id;
    if (chatAdminsSet.has(aUserId)) return;
    mongoChatsTable.find({}, function(err, doc) {
        if (err) {
            console.log('mongo error: ', err);
        } else {
            doc.forEach(function (item) {
                bot.getChatMember(item.chat_id, aUserId)
                    .then(function (chatUser) {
                        if ((chatUser.status === 'creator') || (chatUser.status === 'administrator')) {
                            chatAdminsSet.add(aUserId);
                            addUserToDB(aMsg);
                        }
                    });
            });
        }
    });
}

function generateSettingsKeyboard(aUserId) {
    let isAdmin = chatAdminsSet.has(aUserId);
    let row = [];
    let kbd = [];
    row.push(catchPhrases.settingsKeyboard[3]); // Twitter
    if (isAdmin) {
        row.push(catchPhrases.settingsKeyboard[4]); // Operations
        kbd.push(row);
        row = [];
        row.push(catchPhrases.settingsKeyboard[2]); // PrivateKey
        row.push(catchPhrases.settingsKeyboard[5]); // Switch coin
    }
    kbd.push(row);
    row = [];
    row.push(catchPhrases.mainKeyboard[3]); // Main menu
    kbd.push(row);
    let keyboard = {
        keyboard: kbd,
        resize_keyboard : true,
        one_time_keyboard: false
    };
    chatStateMap.delete(aUserId);
    return keyboard;
}

// Keyboard on First Screen
function generateMainKeyboard(aUserId) {
    let row = [];
    let kbd = [];
    row.push(catchPhrases.mainKeyboard[0]); // Register Wallet
    row.push(catchPhrases.mainKeyboard[1]); // help
    row.push(catchPhrases.mainKeyboard[2]); // balance
    kbd.push(row);
    row = [];
    row.push(catchPhrases.mainKeyboard[4]); // settings
    kbd.push(row);
    let keyboard = {
        keyboard: kbd,
        resize_keyboard : true,
        one_time_keyboard: false
    };
    chatStateMap.delete(aUserId);
    return keyboard;
}

function logOperationDb(aTypeId, aItemSrc, aItemDest, aValue, aTxId, aChatId) {
    let srcUserName = formatUserName (aItemSrc, '@');
    let destUserName = formatUserName (aItemDest, '@');
    let tokenName = getTokenName(aItemSrc);
    mongoOperationsTable.insertOne({ op_date: new Date(),
        op_type: aTypeId,
        from_address: aItemSrc.wallet_address,
        from_user: srcUserName,
        from_user_id: aItemSrc.user_id,
        to_address: aItemDest.wallet_address,
        to_user: destUserName,
        to_user_id: aItemDest.user_id,
        op_amount: aValue,
        tx_id: aTxId,
        chat_id: aChatId,
        token: tokenName
    })
        .catch(e => {
            console.log(e);
        })
}

function doOperationByState(msg) {
    let messageText = msg.text;
    let messageUserId = msg.chat.id;

    if (!(isBlank(messageText.trim()))) {
        if (chatStateMap.get(messageUserId) === ChatStateEnum.WaitDefValueState) {
            console.log('current state - Default value');
            setDefaultTip(msg);
        } else if (chatStateMap.get(messageUserId) === ChatStateEnum.SetTwitUserState) {
            console.log('current state - Twitter name');
            setTwitterName(msg);
        } else if (chatStateMap.get(messageUserId) === ChatStateEnum.SetWalletState) {
            console.log('current state - Wallet setting');
            addUserToDB(msg);
            setTimeout(setNewWallet, 1500, msg);
        } else if (chatStateMap.get(messageUserId) === ChatStateEnum.SetKeyState) {
            console.log('current state - Wallet setting');
            setNewKey(msg)
                .catch(() => {});
        }
    }
}

function formatUserName2(aItem) {
    let name;
    if (aItem.user_first_name) {
        name = aItem.user_first_name;
        if (aItem.user_last_name) {
            name += ' ' + aItem.user_last_name;
        }
    } else {
        name = aItem.user_name;
    }
    return '<a href="tg://user?id=' + aItem.user_id + '">' + name + '</a>';
}

function formatUserName(aItem, aPrefix) {
    let mUserName = aItem.user_name;
    if (mUserName) {
    if (aPrefix) {
        mUserName = aPrefix + mUserName;
    }
    } else {
        mUserName = aItem.user_first_name;
        if (aItem.user_last_name ) {
            mUserName += ' ' + aItem.user_last_name;
        }
    }
    return mUserName;
}

function calcFee (aValue, aPcnt, aUserId, aToken) {
    console.log('calcFee ' + aValue + ' : ' + aPcnt);
    let minValue = dbconfig.minValues[aToken];
    let feeAmount = (aValue / 100) * aPcnt;
    if ((feeAmount < minValue) && (aPcnt > 0)) {
        feeAmount = minValue;
    }
    return feeAmount;
}

function cbkFinalizeTip(aSourceItem, aDestItem, aWitAmount, aMsg, aTxId) {
    let aMessageChatId = aMsg.chat.id;
    logOperationDb("2", aSourceItem, aDestItem, aWitAmount, aTxId, aMessageChatId);
    let mUserName = formatUserName2 (aDestItem);
    let tokenName = getTokenName(aSourceItem);
    let messageText = `User ${mUserName} tipped by ${aWitAmount} ${tokenName}`;
    console.log(messageText);
    sendMessageByBot(aMessageChatId, messageText, aMessageChatId, true);//, generateMainKeyboard(messageUserId));

    // if dest User is registered - send him private MSG
    if (aDestItem.user_id) {
        let mmUserName = formatUserName2 (aSourceItem);
        messageText = `You received ${aWitAmount} ${tokenName} from ${mmUserName}`;
        sendMessageByBot(aDestItem.user_id, messageText, aDestItem.user_id, false, generateMainKeyboard(aMessageChatId));
    }
    chatStateMap.delete(aMessageChatId);
}

function checkBalance(sourceItem, tipAmount, tokenName, count) {
    return new Promise((resolve, reject) => {
        let wallet = sourceItem.wallet_address;
        // noinspection JSUnresolvedFunction
        bnbClient.getBalance(wallet)
            .then(x => {
                let bnbVal = count * stdTxFee;
                let tokenVal = tipAmount;
                if (tokenName === 'BNB') {
                    bnbVal += tipAmount;
                    tokenVal = 0;
                }
                for (let i = 0; i < x.length; i++) {
                    let bal = x[i];
                    let val = bal.free;
                    let tkn = splitTokenName(bal.symbol);
                    // console.log(bal);
                    if (tkn === 'BNB') {
                        // console.log('BNB');
                        if (val < bnbVal) {
                            let err = catchPhrases.debugCommandMessages[9];
                            reject(err);
                        }
                    } else if (tkn === tokenName) {
                        // console.log(tokenName);
                        if (val < tokenVal) {
                            let err = catchPhrases.debugCommandMessages[10] + tokenName;
                            reject(err);
                        }
                    }
                }
                if (x.length === 0) {
                    let err = catchPhrases.debugCommandMessages[11];
                    reject(err);
                }
                resolve();
            })
            .catch( err => {
                console.log('bal error');
                reject(err);
            });
    });
}


function checkTipUser(aMsg, aItem, aObj) { // item - dest User
    let messageChatId = aMsg.chat.id;
    if (aItem) {
        let tipAmount = aObj.value;
        let sourceItem = aObj.item;

        let tokenName = getTokenName(sourceItem);

        checkBalance(sourceItem, tipAmount, tokenName, 1)
            .then(() => {
                prepareTx(sourceItem, aItem, tipAmount, tipFee, tokenName, aMsg, cbkFinalizeTip);
            })
            .catch(e => {
                sendReplyMessageByBot(messageChatId, e, aMsg.message_id);
            });
    } else {
        let messageText = 'Can not tip User not registered in ' + globalBotUserName;
        sendReplyMessageByBot(messageChatId, messageText, aMsg.message_id);
        if (toDeleteMessage) {
            setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
        }
    }
}

function getTokenName(aDbItem) {
    return aDbItem.def_token ? aDbItem.def_token : Object.keys(dbconfig.tokens)[0];
}

// check Tipping params
function checkTip(aMsg, aItem) { // item = Source User
    let chatId = aMsg.chat.id;
    let messageId = aMsg.id;
    if (aItem) {

        if (!aItem.wallet_key) {
            console.log('no private key : ');
            let message = `${catchPhrases.debugCommandMessages[13]}`;
            message += '\n' + catchPhrases.debugCommandMessages[14] + globalBotUserName + catchPhrases.debugCommandMessages[15];
            sendReplyMessageByBot(chatId, message,messageId);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }

        let messageText = aMsg.text;
        let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
        let toUserId = strs[1]; // tip userID
        let tipAmount = checkCoinsVal(strs[2], chatId, messageId);
        if (isNaN(tipAmount)) {  // check amount - not a number
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }
        let tokenName = getTokenName(aItem);
        let minValue = dbconfig.minValues[tokenName];
        if (tipAmount < 0) {
            if (aItem.def_value) {
                tipAmount = aItem.def_value;
            } else {
                tipAmount = minValue; // by default
            }
        }

        if (tipAmount < minValue) {
            console.log('wrong amount : ' + strs[2]);
            let message = `Minimum value to Tip is ${minValue} ${tokenName}`;
            sendReplyMessageByBot(chatId, message,messageId);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }
        let obj = {item: aItem, value: tipAmount};
        isUserIdRegistered (toUserId, checkTipUser, aMsg, obj);
    } else {
        console.log('user not registered');
        let message = 'Open ' + globalBotUserName + ' to register';
        sendReplyMessageByBot(chatId, message,messageId);
        if (toDeleteMessage) {
            setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
        }
        chatStateMap.delete(chatId);
    }
}

function cbkFinalizeRain(aSourceItem, aDestItem, aWitAmount, aMsg, aTxId) {
    let aMessageChatId = aMsg.chat.id;

    logOperationDb("4", aSourceItem, aDestItem, aWitAmount, aTxId, aMessageChatId);
    let mUserName = formatUserName2 (aDestItem);
    let tokenName = getTokenName(aSourceItem);
    let messageText = `${mUserName} got ${aWitAmount} ${tokenName} from Rain`;
    console.log(messageText);
    if (aDestItem.user_id) {
        let mmUserName = formatUserName2 (aSourceItem);
        messageText = `You received ${aWitAmount} ${tokenName} from Rain by ${mmUserName}`;
        sendMessageByBot(aDestItem.user_id, messageText, aDestItem.user_id);
    }
}

function coinFormat(aValue, aPos)
{
    let pow = Math.pow(10,aPos);
    return Math.floor(aValue * pow)/(pow);
}

function dbWriteRainQueue(aMsg, aUsersList, aCallback, aCallbackParam) {
    let chatId = aMsg.chat.id;
    let insArray = [];
    aUsersList.forEach(function(item) {
        insArray.push({chat_id: chatId, user_id: item.userId});
    });
    console.log("storing queue");
    if (insArray.length) {
        mongoQueueTable.insertMany(insArray)
            .then(function () {
                if (aCallback) {
                    // generate list again
                    console.log("repeating users query");
                    getChatUsersList(aMsg, aCallback, aCallbackParam, aUsersList);
                }
            })
            .catch(function (error) {
                console.log(error);
            });
    } else {
        if (aCallback) {
            // generate list again
            console.log("repeating users query");
            getChatUsersList(aMsg, aCallback, aCallbackParam, aUsersList);
        }
    }
}

function generateTwRainQueue1(aMsg, aUsersList, aObj) {
    console.log('generateTwRainQueue1: ' + aObj.id + ' : arr : ' + aUsersList.length);
    if (aUsersList.length) {
        aObj.users = aUsersList.length;
        generateRainQueue2(aMsg, aUsersList, aObj, 0);
        mongoTweetsTable.updateOne({tweet_id: aObj.id}, {$set: {is_active: 0}})
            .catch(e => {
                console.log(`mongo error: ${e}`);
            })
    } else {
        let messageText = catchPhrases.twitterMessages[2];
        sendMessageByBot(aMsg.chat.id, messageText, aMsg.chat.id, true);
    }
}

function generateRainQueue1(aMsg, aUsersList, aObj) {
    let valUsers = aObj.users;
    let cnt = aUsersList.length;
    console.log("to queue : " + cnt + ' : ' + valUsers);
    if (cnt < valUsers) {
        // clean queue
        let chatId = aMsg.chat.id;
        mongoQueueTable.deleteMany({chat_id: chatId})
            .then(function () {
                console.log("cleaning queu");
                // update queue list
                dbWriteRainQueue(aMsg, aUsersList, generateRainQueue2, aObj);
            })
            .catch(function (error) {
                console.log(error);
            });
    } else {
        generateRainQueue2(aMsg, aUsersList, aObj, 0);
    }
}

function splitLongMessage(aLongMsg, aNewMsg, aChatId, aKbd) {
  //  console.log('split long msg:' + aNewMsg);
    let replyMessage = aLongMsg;
    let longMsg = replyMessage + aNewMsg + '\n';
    if (longMsg.length > 4096) {
        console.log(replyMessage);
        setTimeout(sendMessageByBot, 500, aChatId, replyMessage,  0, false, aKbd);
        replyMessage = ''  + aNewMsg + '\n';
    } else {
        replyMessage += aNewMsg + '\n';
    }
    return replyMessage;
}

function generateRainQueue2(aMsg, aUsersList, aObj, aFixedUsers) {
    let messageChatId = aMsg.chat.id;
    let msgId = aMsg.message_id;
    let cnt = aUsersList.length;
    let message;
    console.log('users in chat : ' + cnt);
    if (cnt === 0) {
        message = 'Found ' + cnt + ' chat users, registered in TipBot';
        message += '\nCan not process Rain';
        sendReplyMessageByBot(messageChatId, message, msgId);
        return;
    } else {
        message = 'Found ' + cnt + ' chat users, registered in TipBot';
        message += '\nNow making list for Rain';
        sendReplyMessageByBot(messageChatId, message, msgId);
    }
    let rainValue = aObj.value;
    let valUsers = aObj.users;
    let sourceItem = aObj.item;
    let userName = formatUserName2(sourceItem);
    let n = (cnt < valUsers) ? cnt : valUsers;
    let tokenName = aObj.token;
    let minValue = dbconfig.minValues[tokenName];
    if ((rainValue / n) < minValue) { // if < 1 per user
        console.log('Rain per User is less');
        message = `Rain per User is less than ${minValue} ${tokenName}`;
        message += '\nCan not process Rain';
        sendReplyMessageByBot(messageChatId, message, msgId);
        return;
    }

    checkBalance(sourceItem, rainValue, tokenName, n)
        .then(() => {
            let fee = calcFee(rainValue, rainFee, sourceItem.user_id);
            console.log('rain fee: ' + fee + ' | n : ' + n);
            message = `${userName} calls Rain with ${rainValue} ${tokenName} for ${n} users:\n`;
            let decrVal = (tokenName === "BNB") ? stdTxFee : 0;
            let oneValue = (rainValue - fee - (n) * decrVal) /n;
            console.log('oneValue : ' + oneValue);
            oneValue = coinFormat(oneValue, 4);
            let usersQueue = [];

            // copy first aFixedUsers users - they were not used in prev Rains
            console.log();
            if (aFixedUsers > 0) {
                for (let i = 0; i < aFixedUsers; i++) {
                    let msgAdd = `${i+1}. ${aUsersList[i].userName} gets ${oneValue} ${tokenName}`;
                    message = splitLongMessage(message, msgAdd, messageChatId);
                    usersQueue.push(aUsersList[i]);
                }
            }

            if (cnt < valUsers) {
                console.log('not enough users - use full list');
                for (let i = aFixedUsers; i < cnt; i++) {
                    let msgAdd = `${i+1}. ${aUsersList[i].userName} gets ${oneValue} ${tokenName}`;
                    message = splitLongMessage(message, msgAdd, messageChatId);
                    usersQueue.push(aUsersList[i]);
                }
            } else {
                console.log('get ' + valUsers + ' random from ' + cnt + ' users');
                let idSet = new Set();
                for (let i = aFixedUsers; i < valUsers; i++) {
                    let n = getRandomInt(aFixedUsers, cnt - 1);
                    while (idSet.has(n)) {
                        n = getRandomInt(aFixedUsers, cnt - 1);
                    }
                    idSet.add(n);
                    let msgAdd = `${i+1}. ${aUsersList[n].userName} gets ${oneValue} ${tokenName}`;
                    message = splitLongMessage(message, msgAdd, messageChatId);
                    usersQueue.push(aUsersList[n]);
                }
            }
            console.log('message : ' + message);
            setTimeout(sendMessageByBot, 500, messageChatId, message, messageChatId,  false);
            dbWriteRainQueue(aMsg, usersQueue);

            let dest = [];
            let destItems = [];
            let token = dbconfig.tokens[tokenName];
            cnt = 0;

            for (let i = 0; i < usersQueue.length; i++) {
                cnt++;
                let wallet = usersQueue[i].userItem.wallet_address;
                if (wallet) {
                    dest.push({
                        to: wallet,
                        coins: [{denom: token, amount: oneValue}]
                    });
                    destItems.push(usersQueue[i].userItem);
                    if (cnt === 100) { // split by 100 Tx
                        let d = dest.slice();
                        let di = destItems.slice();
                        addTxToQuery(sourceItem, d, di, oneValue, aMsg, cbkFinalizeRain);
                        dest = [];
                        destItems = [];
                        cnt = 0;
                    }
                }
            }

            if (cnt > 0) {
                addTxToQuery(sourceItem, dest, destItems, oneValue, aMsg, cbkFinalizeRain);
            }
        })
        .catch(e => {
            sendReplyMessageByBot(messageChatId, e, msgId);
        });
 }

function getTwitUsersList(aMsg, aCallback, aCallBackParam) {
    let twitId = aCallBackParam.id;
    let userId = 0;
    if (aMsg.chat.type !== 'channel') {
        userId = aMsg.from.id.toString();
    }
    mongoRetweetsTable.distinct("twit_user_id", {tweet_id: twitId}, function (err, res) {
        if (err) {
            console.log('mongo error: ' + err);
        } else {
            let queue = [{
                $match: {
                    twit_user_id: {$in: res}
                }
            }];
            console.log ('retweets: ' + res.length);
            let chatList = [];
            mongoUsersTable.aggregate(queue).toArray((err, data) => {
                if (err) {
                    console.log('mongo error: ' + err);
                } else {
                    console.log ('retweeters: ' + data.length);
                    for (let i = 0; i < data.length; i++) {
                        let item = data[i];
                        // noinspection JSUnresolvedVariable
                        if (item.user_id !== userId) {  // exclude caller id
                            let userName = formatUserName2(item);
                            // noinspection JSUnresolvedVariable
                            let objUser = {
                                userId: item.user_id,
                                userName: userName,
                                userItem: item,
                                userStatus: 'member',
                                userActive: item.inactive,
                                userActDate: item.inactive_date,
                                twit_user_id: item.twit_user_id
                            };
                            chatList.push(objUser);
                        }
                    }
                    aCallback(aMsg, chatList, aCallBackParam);
                }
            });
        }
    });
}

function startTwRain (aMsg, aObj) {
    getTwitUsersList(aMsg, generateTwRainQueue1, aObj);
}

function startRain(aMsg, aObj) {
    getChatUsersList(aMsg, generateRainQueue1, aObj);
}

function checkCoinsVal(aStr, aChatId, aMsgId) {
    console.log('check val: ' + aStr);
    let strReplace = aStr.replace(',', '.');
    strReplace = strReplace.toLowerCase().trim();
    let val = Number(strReplace);
    if (isNaN(val)) {  // check value
        console.log('wrong value : ' + aStr);
        let message = 'Provide correct Number (Value) in param: ' + aStr;
        sendReplyMessageByBot(aChatId, message, aMsgId);
    }
    return val;
}

function checkTwRain(aMsg, aItem) {
    let messageChatId = aMsg.chat.id;
    let msgId = aMsg.message_id;
    if (aItem)
    {
        if (!aItem.wallet_key) {
            console.log('no private key : ');
            let message = `${catchPhrases.debugCommandMessages[13]}`;
            message += '\n' + catchPhrases.debugCommandMessages[14] + globalBotUserName + catchPhrases.debugCommandMessages[15];
            sendReplyMessageByBot(messageChatId, message,msgId);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }

        let messageText = aMsg.text;
        let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
        let rainValue = checkCoinsVal(strs[1], messageChatId, msgId);
        if (isNaN(rainValue)) {  // check value
            return;
        }
        let tweetId = twitter.getTweetId(strs[2]);

        let tokenName = getTokenName(aItem);
        let minValue = dbconfig.minValues[tokenName];
        if (rainValue < minValue*10) {
            console.log('wrong value : ' + strs[1]);
            let message = `Minimum value for Rain is ${minValue*10} ${tokenName}`;
            sendReplyMessageByBot(messageChatId, message, msgId);
            return;
        }
        let message = 'Generating users list for Twitter Rain';
        message += '\nThis can take some time';
        sendReplyMessageByBot(messageChatId, message, msgId);
        let obj = {item: aItem, value: rainValue, id: tweetId, token: tokenName};
        setTimeout(startTwRain, 1000, aMsg, obj);
    } else {
        console.log('user not registered');
        let message = 'Open ' + globalBotUserName + ' to register';
        sendReplyMessageByBot(messageChatId, message, msgId);
    }
}


function checkRain(aMsg, aItem) {
    let messageChatId = aMsg.chat.id;
    let msgId = aMsg.message_id;
    if (aItem)
    {
        if (!aItem.wallet_key) {
            console.log('no private key : ');
            let message = `${catchPhrases.debugCommandMessages[13]}`;
            message += '\n' + catchPhrases.debugCommandMessages[14] + globalBotUserName + catchPhrases.debugCommandMessages[15];
            sendReplyMessageByBot(messageChatId, message,msgId);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }

        let messageText = aMsg.text;
        let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
        let rainValue = checkCoinsVal(strs[1], messageChatId, msgId);
        if (isNaN(rainValue)) {  // check value
            return;
        }
        let numberUsers = Number(strs[2].replace(',', '.'));
        if (isNaN(numberUsers)) {  // check value
            let str = strs[2].toUpperCase().trim();
            if ((str.indexOf('ALL') === 0) || (str.indexOf('MAX') === 0)) {
                numberUsers = 10000;
            } else {
                console.log('wrong value : ' + strs[2]);
                let message = 'Provide correct Number of Users in param: ' + strs[2];
                sendReplyMessageByBot(messageChatId, message, msgId);
                return;
            }
        }
        let valUsers = Math.round(numberUsers);
        if (valUsers < 2) {  // check value
            console.log('wrong value : ' + valUsers);
            let message = 'Number of Users must be greater than 1';
            sendReplyMessageByBot(messageChatId, message, msgId);
            return;
        }
        let tokenName = getTokenName(aItem);
        let minValue = dbconfig.minValues[tokenName];
        if (rainValue < minValue*10) {
            console.log('wrong value : ' + strs[1]);
            let message = `Minimum value for Rain is ${minValue*10} ${tokenName}`;
            sendReplyMessageByBot(messageChatId, message, msgId);
            return;
        }

        let message = 'Generating users list for Rain';
        message += '\nThis can take some time';
        sendReplyMessageByBot(messageChatId, message, msgId);
        let obj = {item: aItem, value: rainValue, users: valUsers, token: tokenName};
        setTimeout(startRain, 1000, aMsg, obj);
    } else {
        console.log('user not registered');
        let message = 'Open ' + globalBotUserName + ' to register';
        sendReplyMessageByBot(messageChatId, message, msgId);
    }
}

function findChannelAdmin(aMsg, aCallbackFunc) {
    console.log('findChannelAdmin');
    bot.getChatAdministrators(aMsg.chat.id)
        .then(res => {
            if (res) {
                console.log('admins: ', res.length);
                let ids = [];
                for (let i = 0; i < res.length; i++) {
                    let id = res[i].user.id.toString();
                    ids.push(id);
                }
                mongoUsersTable.find({user_id: {$in: ids}}).toArray()
                    .then(function (dbItems) {
                        console.log('mongo admins: ', dbItems.length);
                        for (let k = 0; k < dbItems.length; k++) {
                            let dbItem = dbItems[k];
                            if (dbItem.wallet_key) {
                                console.log('admin found: ', dbItem.user_id);
                                aMsg.from = {id: Number(dbItem.user_id),
                                username: dbItem.user_name,
                                first_name: dbItem.user_first_name,
                                last_name: dbItem.user_last_name};
                                aCallbackFunc(aMsg, dbItem);
                                break;
                            }
                        }
                    });
            }
        })
        .catch(e => {
            console.log('TG error: ', e);
        })
}

// Giveaway functions
function doGiveaway(aMsg) {
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    let userId = 0;
    if (aMsg.chat.type !== 'channel') {
        userId = aMsg.from.id;
    }
    // parse message params
    if (strs.length === 1) { // if param  exists in message
        aMsg.text = aMsg.text + ' -1';
    }
    if (userId) {
        isUserIdRegistered (userId, checkGiveaway, aMsg);
    } else {
        findChannelAdmin(aMsg, checkGiveaway);
    }
}

function checkGiveaway(aMsg, aItem) {
    let messageChatId = aMsg.chat.id;
    let msgId = aMsg.id;
    if (aItem)
    {
        if (!aItem.wallet_key) {
            console.log('no private key : ');
            let message = `${catchPhrases.debugCommandMessages[13]}`;
            message += '\n' + catchPhrases.debugCommandMessages[14] + globalBotUserName + catchPhrases.debugCommandMessages[15];
            sendReplyMessageByBot(messageChatId, message,msgId);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }

        let tokenName = getTokenName(aItem);
        let minValue = dbconfig.minValues[tokenName];
        let messageText = aMsg.text;
        let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
        let giveValue = checkCoinsVal(strs[1], messageChatId, msgId);
        if (giveValue === -1) {
            if (aItem.def_value) {
                giveValue = aItem.def_value;
            } else {
                giveValue = minValue; // std default
            }
        }
        if (isNaN(giveValue)) {  // check amount
            return;
        }

        if (giveValue < minValue) {
            console.log('wrong value : ' + strs[1]);
            let message = `Minimum value to Giveaway is ${minValue} ${tokenName}`;
            sendReplyMessageByBot(messageChatId, message, msgId);
            return;
        }

        checkBalance(aItem, giveValue, tokenName, 1)
            .then(() => {
                generateGiveawayMessage(aMsg, giveValue, tokenName);
            })
            .catch(e => {
                sendReplyMessageByBot(messageChatId, e, msgId);
            });

    } else {
        console.log('user not registered');
        let message = 'Open ' + globalBotUserName + ' to register';
        sendReplyMessageByBot(messageChatId, message, msgId);
    }
}

function generateGiveawayMessage(aMsg, aValue, aTokenName) {
    let userId = aMsg.from.id;
    let chatId = aMsg.chat.id;
    let options = {
        reply_markup: JSON.stringify({
            inline_keyboard: [
                [{ text: 'Claim', callback_data: 'give ' + userId + ' ' + aValue.toString() + ' ' + aTokenName },
                    { text: 'Cancel', callback_data: 'give ' + userId + ' cancel' }]
            ]
        }),
        parse_mode: 'HTML'
    };
    let item = {
        user_id: userId,
        user_name: aMsg.from.username,
        user_first_name: aMsg.from.first_name,
        user_last_name: aMsg.from.last_name};
    let mUserName = formatUserName2 (item);
    console.log(mUserName);
    let message = `User ${mUserName} is giving ${aValue} ${aTokenName}. `;
    message += '\nClick the \'Claim\' button to claim it.';
    bot.sendMessage(chatId, message , options)
        .catch(e => {
            console.log('error sending message : ' + e);
        });
}

function doGiveawayAuto (aMsg) {
    // check command params -
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    // parse message params
    if (strs.length > 2) { // if param  exists in message
        // check user registered
        isUserIdRegistered (aMsg.from.id, checkGiveAuto, aMsg);
    } else {
        let messageText = 'Not enough params\n';
        messageText += `Use GiveAuto command in format: <code>/${chatCmdMod}giveauto each-time-value Interval</code>`;
        sendReplyMessageByBot(aMsg.chat.id, messageText, aMsg.message_id);
    }
}

function checkGiveAuto(aMsg, aTtem) {
    let messageChatId = aMsg.chat.id;
    let messageUserId = aMsg.from.id;
    let msgId = aMsg.id;
    if (aTtem)
    {
        if (!aTtem.wallet_key) {
            console.log('no private key : ');
            let message = `${catchPhrases.debugCommandMessages[13]}`;
            message += '\n' + catchPhrases.debugCommandMessages[14] + globalBotUserName + catchPhrases.debugCommandMessages[15];
            sendReplyMessageByBot(messageChatId, message,msgId);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }

        let messageText = aMsg.text;
        let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
        let giveValue = checkCoinsVal(strs[1], messageChatId, msgId);
        let giveTimeOut = Number(strs[2].replace(',', '.'));
        if (isNaN(giveTimeOut)) {
            console.log('wrong value : ' + strs[2]);
            let message = 'Provide correct Number (time interval in Hours) in param: ' + strs[2];
            sendReplyMessageByBot(messageChatId, message, msgId);
            return;
        }
        if (isNaN(giveValue)) {  // check amount
            return;
        }
        let tokenName = getTokenName(aTtem);
        let minValue = dbconfig.minValues[tokenName];

        if (giveValue < minValue) {
            console.log('wrong value : ' + strs[1]);
            let message = `Minimum value to Giveaway is 1 ${tokenName}`;
            sendReplyMessageByBot(messageChatId, message, msgId);
            return;
        }

        checkBalance(aTtem, giveValue, tokenName, 1)
            .then(() => {
                // store GiveAway params
                if (giveAutoMap.has(messageUserId)) {
                    giveAutoMap.delete(messageUserId);
                    let handle = giveAutoTimeoutHandle.get(messageUserId);
                    clearTimeout(handle);
                }
                let obj = {msg: aMsg, value: giveValue, token: tokenName};
                giveAutoMap.set(messageUserId, obj);
                let handle = setTimeout(checkGiveAutoQuery, giveAutoMult * giveTimeOut, messageUserId); //, giveValue);
                giveAutoTimeoutHandle.set(messageUserId, handle);
                let message = 'GiveAuto (do giveaway every ' + giveTimeOut + ' hours) started !';
                sendReplyMessageByBot(messageChatId, message, msgId);
                generateGiveawayMessage(aMsg, giveValue, tokenName);
            })
            .catch(e => {
                sendReplyMessageByBot(messageChatId, e, msgId);
            });


    } else {
        console.log('user not registered');
        let message = 'Open ' + globalBotUserName + ' to register';
        sendReplyMessageByBot(messageChatId, message, msgId);
    }
}

function checkGiveAutoQ (aMsg, aTtem, aObject) {
    let messageChatId = aMsg.chat.id;
    let messageUserId = aMsg.from.id;
    if (aTtem) {
        let userName = formatUserName2(aTtem);
        let messageText = aMsg.text;
        let strs = messageText.split(/(\s+)/).filter(e => e.trim().length > 0);
        let giveValue = aObject.value;
        let giveTimeOut = Number(strs[2].replace(',', '.'));

        checkBalance(aTtem, giveValue, aObject.token, 1)
            .then(() => {
                let handle = setTimeout(checkGiveAutoQuery, giveAutoMult * giveTimeOut, messageUserId);//, giveValue);
                giveAutoTimeoutHandle.set(messageUserId, handle);
                // let tokenName = getTokenName(aTtem);
                generateGiveawayMessage(aMsg, giveValue, aObject.token);
            })
            .catch(() => {
                    let message = userName + ' has not enough coins on Balance';
                    message += '\nStopping GiveAuto';
                    sendMessageByBot(messageChatId, message, messageUserId,  true);
            });
    } else {
        // stop GiveAuto
        giveAutoMap.delete(messageUserId);
        let handle =  giveAutoTimeoutHandle.get(messageUserId);
        clearTimeout(handle);
    }
}

function checkGiveAutoQuery (aUserId) {
    if (giveAutoMap.has(aUserId)) {
        let obj = giveAutoMap.get(aUserId);
        let msg = obj.msg;
        isUserIdRegistered(msg.from.id, checkGiveAutoQ, msg, obj);
    } else {
        //
    }
}

function doGiveawayStop(aMsg) {
    let userId = aMsg.from.id;
    let messageChatId = aMsg.chat.id;
    let msgId = aMsg.message_id;
    let userName = aMsg.from.username ? aMsg.from.username : aMsg.from.first_name + ' ' + aMsg.from.last_name;
    if (giveAutoMap.has(userId)) {
        giveAutoMap.delete(userId);
        let handle = giveAutoTimeoutHandle.get(userId);
        clearTimeout(handle);
        let message = 'User ' + userName + ' stopped GiveAuto!';
        sendReplyMessageByBot(messageChatId, message, msgId);
            } else {
        let message = 'You have no active GiveAuto running';
        sendReplyMessageByBot(messageChatId, message, msgId);
    }
}

function doTwitterRain(aMsg) {
    // check command params -
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    // parse message params
    if (strs.length > 2) { // if param  exists in message
        // check user registered
        let userId = 0;
        if (aMsg.chat.type !== 'channel') {
            userId = aMsg.from.id;
        }
        if (userId) {
            isUserIdRegistered (userId, checkTwRain, aMsg);
        } else {
            findChannelAdmin(aMsg, checkTwRain);
        }
    } else {
        let messageText = 'Not enough params\n';
        messageText += `Use rtRain command in format: <code>/${chatCmdMod}rtrain value Tweet_URL</code>`;
        sendReplyMessageByBot(aMsg.chat.id, messageText, aMsg.message_id);
    }
}

function finCheckToken(aMsg, aItem) {
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    if (strs.length > 1) {
        updateTokenName(aItem.user_id, aMsg.chat.id, strs[1])
    } else {
        let tokenName = getTokenName(aItem);
        let textMessage = `${catchPhrases.debugCommandMessages[8]} <code>${tokenName}</code>`;
        sendReplyMessageByBot(aMsg.chat.id, textMessage, aMsg.message_id);
    }
}

function doCheckToken(aMsg) {
    let userId = 0;
    if (aMsg.chat.type !== 'channel') {
        userId = aMsg.from.id;
    }
    if (userId) {
        isUserIdRegistered (userId, finCheckToken, aMsg);
    } else {
        findChannelAdmin(aMsg, finCheckToken);
    }
}

function doRain(aMsg) {
    // check command params -
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    // parse message params
    if (strs.length > 2) { // if param  exists in message
        // check user registered
        let userId = 0;
        if (aMsg.chat.type !== 'channel') {
            userId = aMsg.from.id;
        }
        if (userId) {
            isUserIdRegistered (userId, checkRain, aMsg);
        } else {
            findChannelAdmin(aMsg, checkRain);
        }
    } else {
        let messageText = 'Not enough params\n';
        messageText += `Use Rain command in format: <code>/${chatCmdMod}rain value NUMBER_of_Users</code>`;
        sendReplyMessageByBot(aMsg.chat.id, messageText, aMsg.message_id);
    }
}

function doStat(aMsg) {
    let chatId = aMsg.chat.id;
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    let tokenName = Object.keys(dbconfig.tokens)[0];
    console.log(strs);
    if (strs.length > 1) {
        tokenName = strs[1].toUpperCase().trim();
    }
    console.log('token name: ', tokenName);
    let message = '📊 <b>Stat of</b> <a href="tg://user?id=' + globalBotId + '">TipBot</a> (for <b>' + tokenName + '</b>)\n-\n';
    // query total spent
    let query = [{$match: {op_type: {$in: ["2", "3", "4", "5"]}, chat_id: chatId, token: tokenName}},
        {$group: {_id: "$op_type", pp: {$sum: "$op_amount"}}},
        {$sort: {_id: 1}}];
    mongoOperationsTable.aggregate(query).toArray()
        .then(function (result) {
        if (result.length) {
            let sum = 0;
            for (let i = 0; i < result.length; i++) {
                let dbItem = result[i];
                let typ = dbItem._id;
                let val = coinFormat(dbItem.pp, 4);
                sum += val;
                if (typ === "2") {
                    message += `☕️ Tips: ${val} ${tokenName}\n`;
                } else if (typ === "3") {
                    message += `💸 Giveaways: ${val} ${tokenName}\n`;
                } else if (typ === "4") {
                    message += `🌧 Rains: ${val} ${tokenName}\n`;
                }
            }
            message += ` = SUM : ${sum} ${tokenName} = \n-\n`;
        } else {
            console.log('Total spent - not found');
        }

        // query Spent most
        query = [{$match: {op_type: {$in: ["2", "3", "4"]},chat_id: chatId, token: tokenName}},
            {$group: {
                _id: {uid: "$from_user_id", unm: "$from_user"},
                pp: {$sum: "$op_amount"}
            }},
            {$sort: {pp: -1}},
            {$limit: 3}];
        mongoOperationsTable.aggregate(query).toArray()
            .then(function ( result) {
            if (result.length) {
                let dbItem = result[0];
                let val = coinFormat(dbItem.pp, 4);
                console.log('second stat step: ' + dbItem._id.unm + ' = ' + val);
                message += '<b>Most spending</b>\n';
                message += `1️⃣ ${dbItem._id.unm} spent ${val} ${tokenName}\n`;
                if (result.length > 1) {
                    dbItem = result[1];
                    val = coinFormat(dbItem.pp, 4);
                    message += `2️⃣ ${dbItem._id.unm} spent ${val} ${tokenName}\n`;
                }
                if (result.length > 2) {
                    dbItem = result[2];
                    val = coinFormat(dbItem.pp, 4);
                    message += `3️⃣ ${dbItem._id.unm} spent ${val} ${tokenName}\n`;
                }
            } else {
                console.log('Spent most - not found');
            }

               // query Recieved most
                query = [{$match: {op_type: {$in: ["2", "3", "4"]},chat_id: chatId, token: tokenName}},
                    {$group: {
                            _id: {uid: "$to_user_id", unm: "$to_user"},
                            pp: {$sum: "$op_amount"}
                        }},
                    {$sort: {pp: -1}},
                    {$limit: 3}];
                mongoOperationsTable.aggregate(query).toArray()
                    .then(function (result) {
                    if (result.length) {
                        let dbItem = result[0];
                        let val = coinFormat(dbItem.pp, 4);
                        console.log('4th stat step: ' + dbItem._id.unm + ' = ' + val);
                        message += '<b>Most gaining</b>\n';
                        message += `1️⃣ ${dbItem._id.unm} received ${val} ${tokenName}\n`;
                        if (result.length > 1) {
                            dbItem = result[1];
                            val = coinFormat(dbItem.pp, 4);
                            message += `2️⃣ ${dbItem._id.unm} received ${val} ${tokenName}\n`;
                        }
                        if (result.length > 2) {
                            dbItem = result[2];
                            val = coinFormat(dbItem.pp, 4);
                            message += `3️⃣ ${dbItem._id.unm} received ${val} ${tokenName}\n`;
                        }
                    } else {
                        console.log('received most - not found');
                    }

                    // query Tipped most 2
                    query = [{$match: {op_type: {$in: ["2"]},chat_id: chatId, token: tokenName}},
                        {
                            $group: {
                                _id: {uid: "$to_user_id", unm: "$to_user"},
                                pp: {$sum: "$op_amount"},
                                count: {$sum: 1}
                            }
                        },
                        {$sort: {count: -1}},
                        {$limit: 3}];
                    mongoOperationsTable.aggregate(query).toArray()
                        .then(function ( result) {
                        if (result.length) {
                            let dbItem = result[0];
                            console.log('5th stat step: ' + dbItem._id.unm);
                            message += '<b>Most respected</b>\n';
                            message += '1️⃣ ' + dbItem._id.unm + ' was tipped ' + dbItem.count + ' times\n';
                            if (result.length > 1) {
                                dbItem = result[1];
                                message += '2️⃣ ' + dbItem._id.unm + ' was tipped ' + dbItem.count + ' times\n';
                            }
                            if (result.length > 2) {
                                dbItem = result[2];
                                message += '3️⃣ ' + dbItem._id.unm + ' was tipped ' + dbItem.count + ' times\n';
                            }
                        } else {
                            console.log('tipped most - not found');
                        }

                        // query Rains Lucky
                        query = [{$match: {op_type: {$in: ["4"]},chat_id: chatId, token: tokenName}},
                            {
                                $group: {
                                    _id: {uid: "$to_user_id", unm: "$to_user"},
                                    pp: {$sum: "$op_amount"},
                                    count: {$sum: 1}
                                }
                            },
                            {$sort: {pp: -1}},
                            {$limit: 1}];
                        mongoOperationsTable.aggregate(query).toArray()
                            .then(function (result) {
                            if (result.length) {
                                let dbItem = result[0];
                                let val = coinFormat(dbItem.pp, 4);
                                console.log('6th stat step: ' + dbItem._id.unm + ' : ' + val);
                                message += '<b>Lucky One</b>\n';
                                message += '🍀 ' + dbItem._id.unm + ' received rains ' + dbItem.count;
                                message += ` times (sum = ${val} ${tokenName})\n`;
                            } else {
                                console.log('Rains champion - not found');
                            }

                        // query Claims champion
                        query = [{$match: {op_type: {$in: ["3"]},chat_id: chatId, token: tokenName}},
                            {
                                $group: {
                                    _id: {uid: "$to_user_id", unm: "$to_user"},
                                    pp: {$sum: "$op_amount"},
                                    count: {$sum: 1}
                                }
                            },
                            {$sort: {count: -1}},
                            {$limit: 1}];
                        mongoOperationsTable.aggregate(query).toArray()
                            .then(function (result) {
                            if (result.length) {
                                let dbItem = result[0];
                                let val = coinFormat(dbItem.pp, 4);
                                console.log('7th stat step: ' + dbItem._id.unm + ' : ' + val);
                                message += '<b>Claims champion</b>\n';
                                message += '🏅 ' + dbItem._id.unm + ' claimed ' + dbItem.count;
                                message += ` times (sum = ${val} ${tokenName})\n`;
                            } else {
                                console.log('Claims champion - not found');
                            }
                                 sendMessageByBot(chatId, message, chatId,  false);
                            });
                        });
                        });
                    });
                });
            });
 }

function getDefaultToken (aMsg) {
    let chatId = aMsg.chat.id;
    let userId = aMsg.from.id;
    let id = userId.toString();
    mongoUsersTable.findOne({user_id: id}, {}, function(err, item) {
        if (err) {
            console.log('mongo error: ' + err);
        } else {
            let val;
            if (item) {
                val = item.def_token;
            }
            let messageText = catchPhrases.privateDialogMessages[36];
            if (!val) {
                val = Object.keys(dbconfig.tokens)[0];
            }
            messageText = catchPhrases.privateDialogMessages[35] + '<code>' + val + '</code>\n' + messageText;

            let row = [];
            let kbd = [];
            let cnt = 0;
            Object.keys(dbconfig.tokens).forEach(function (key) {
                console.log(key, ' | ', dbconfig.tokens[key]);
                row.push({text: key, callback_data: 'tks ' + key});
                cnt++;
                if (cnt === 3) {
                    cnt = 0;
                    kbd.push(row);
                    row = [];
                }
            });
            kbd.push(row);

            let options = {
                reply_markup: JSON.stringify({
                    inline_keyboard: kbd
                }),
                parse_mode: 'HTML'
            };
            bot.sendMessage(chatId, messageText, options)
                .catch(e => {
                    console.log('error sending message : ' + e);
                });
        }

    })
}

function getDefaultTip(aMsg) {
    let chatId = aMsg.chat.id;
    let userId = aMsg.from.id;
    let id = userId.toString();
    mongoUsersTable.findOne({user_id: id}, {}, function(err, item) {
        if (err) {
            console.log('mongo error: ' + err);
        }
        let messageText = catchPhrases.privateDialogMessages[0];
        if (item) {
            let val = item.def_value;
            if (val) {
                messageText = `Default (tip/give) value = ${val} 
You can enter New value to be used by Default:`;
            }
        }
        sendMessageByBot(chatId, messageText, userId, false);
        chatStateMap.set(userId, ChatStateEnum.WaitDefValueState);
    })
}

async function setNewKey(aMsg) {
    let chatId = aMsg.chat.id;
    let userId = aMsg.from.id;
    let text = aMsg.text.trim();
    console.log('set key : *** ');
    let bnbObj;

    try {
        bnbObj = await bnbClient.recoverAccountFromMnemonic(text);
        if (bnbObj) {
            // console.log(bnbObj);
            deleteBotMessage(aMsg);
            mongoUsersTable.updateOne({user_id: userId.toString()},
                {$set: {wallet_address: bnbObj.address, wallet_key: bnbObj.privateKey}})
                .then(() => {
                    let messageText = catchPhrases.privateDialogMessages[31];
                    messageText += `\n<code>${bnbObj.address}</code>`;
                    sendMessageByBot(chatId, messageText, userId, false);
                })
                .catch(e => {
                    console.log('mongo error: ' + e);
                });
            chatStateMap.delete(userId);
        }
    } catch ({message}) {
        console.error(message);
        let messageText = catchPhrases.privateDialogMessages[33];
        sendMessageByBot(chatId, messageText, userId, false);
    }
}

function setNewWallet(aMsg) {
    let chatId = aMsg.chat.id;
    let userId = aMsg.from.id;
    let text = aMsg.text.trim();
    console.log('setWallet : ' + text);
    if (bnbClient.checkAddress(text, 'bnb')) {

        mongoUsersTable.updateOne({user_id: userId.toString()},
            {$set: {wallet_address: text}})
            .then(() => {
                let messageText = catchPhrases.privateDialogMessages[27];
                sendMessageByBot(chatId, messageText, userId, false);
            })
            .catch(e => {
                console.log('mongo error: ' + e);
            });
        chatStateMap.delete(userId);
    } else {
        let messageText = catchPhrases.privateDialogMessages[32];
        sendMessageByBot(chatId, messageText, userId, false);
    }
}

function setTwitterName(aMsg) {
    let chatId = aMsg.chat.id;
    let userId = aMsg.from.id;
    let text = aMsg.text;
    console.log('setTwitUser : ' + text);
    twitter.getTwitterId(text)
        .then((data) => {
            mongoUsersTable.findOne({twit_user_id: data.id_str}, (err, item) => {
                if (err) {
                    console.log(`mongo error: ${err}`);
                    chatStateMap.delete(userId);
                } else {
                    if (item) {
                        console.log(`dup twitter: ${item.twit_user_id}`);
                        let messageText = catchPhrases.twitterMessages[3];
                        sendMessageByBot(chatId, messageText, userId,  false);
                        chatStateMap.delete(userId);
                    } else {
                        mongoUsersTable.updateOne({user_id: userId.toString()},
                            {$set: {twit_user_name: data.screen_name, twit_user_id: data.id_str}})
                            .then(() => {
                                let messageText = catchPhrases.privateDialogMessages[12];
                                sendMessageByBot(chatId, messageText, userId,  false);
                            })
                            .catch(e => {
                                console.log('mongo error: ' + e);
                            });
                        chatStateMap.delete(userId);
                    }
                }
            });
        })
        .catch(e => {
            let messageText = text + ' ' + catchPhrases.privateDialogMessages[13];
            sendMessageByBot(chatId, messageText, userId,  false);
            console.log('tweet error: ' + e);
        });
}


// set default tip/give value
function setDefaultTip(aMsg) {
    let chatId = aMsg.chat.id;
    let userId = aMsg.from.id;
    let text = aMsg.text;
    let val = Number(text.replace(',', '.'));
    if (isNaN(val)) {
        let messageText = catchPhrases.privateDialogMessages[1];
        sendMessageByBot(chatId, messageText, userId,  false);
    } else {
        mongoUsersTable.updateOne({user_id: userId.toString()},
            {$set: {def_value: val}})
            .then(() => {
                let messageText = val + catchPhrases.privateDialogMessages[2];
                sendMessageByBot(chatId, messageText, userId,  false);
            })
            .catch(e => {
                console.log('mongo error: ' + e);
            });
        chatStateMap.delete(userId);
    }
}

function doTip(aMsg)
{
    let messageText = aMsg.text;
    if (aMsg.chat.type === 'channel') {
        console.log('tip in channel');
        messageText = 'Sorry. Can not tip in Channel!';
        sendReplyMessageByBot(aMsg.chat.id, messageText);
        if (toDeleteMessage) {
            setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
        }
        return;
    }
    // check command params -
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    // noinspection JSUnresolvedVariable
    let repMsg = aMsg.reply_to_message;

    if (repMsg) {  // if tip in reply
        // noinspection JSUnresolvedVariable
        if (repMsg.from.is_bot) {
            console.log('tip bot !');
            messageText = 'Sorry. Can not tip to Bot !';
            sendReplyMessageByBot(aMsg.chat.id, messageText, aMsg.message_id);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }
        if (repMsg.from.id === aMsg.from.id) {
            console.log('same user ID : ' + repMsg.from.id);
            messageText = 'Sorry. Can not tip Yourself';
            sendReplyMessageByBot(aMsg.chat.id, messageText, aMsg.message_id);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }
        let lMsg = aMsg;
        if (strs.length > 2) { // tip by Name
            console.log('tip in reply by name : ');
            isUserRegistered(strs[1], doTipByName, aMsg);
        } else {
            if (strs.length === 2) {

                lMsg.text = `/${chatCmdMod}tip ${repMsg.from.id} ${strs[1]}`;
                console.log('tip in reply : ' + lMsg.text);
            } else {
                lMsg.text = `/${chatCmdMod}tip ${repMsg.from.id} -1`; // -1 = take value from settings
            }
            isUserIdRegistered(aMsg.from.id, checkTip, lMsg);
        }
    } else {
        // parse message params
        if (strs.length > 1) {
            isUserRegistered(strs[1], doTipByName, aMsg);
        } else {
            let messageChatId = aMsg.chat.id;
            messageText = `Use tip command in format: <code>/${chatCmdMod}tip @ChatUser value</code>`;
            sendReplyMessageByBot(messageChatId, messageText, aMsg.message_id);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
        }
    }
}

function doTipByName (aMsg, aItem, aUserName) {
    let messageText = aMsg.text;
    let strs = messageText.split(/(\s+)/).filter( e => e.trim().length > 0);
    if (aItem) {
        let lMsg = aMsg;

        if (aItem.user_id === (aMsg.from.id).toString() ) {
            console.log('doTipByName same user ID : ' + aItem.user_id);
            messageText = 'Sorry. Can not tip Yourself';
            sendReplyMessageByBot(aMsg.chat.id, messageText, aMsg.message_id);
            if (toDeleteMessage) {
                setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
            }
            return;
        }
        if (strs.length > 2) {
            lMsg.text = `/${chatCmdMod}tip ${aItem.user_id} ${strs[2]}`;
        } else {
            lMsg.text = `/${chatCmdMod}tip ${aItem.user_id} -1`;
        }
        isUserIdRegistered(aMsg.from.id, checkTip, lMsg);
    } else {
        let messageChatId = aMsg.chat.id;
        messageText = 'User @' + aUserName  + ' not found in my DB';
        messageText += '\nAsk @' + aUserName  + ' to register in ' + globalBotUserName;
        sendReplyMessageByBot(messageChatId, messageText, aMsg.message_id);
        if (toDeleteMessage) {
            setTimeout(deleteBotMessage, deleteMessageInterval * 2, aMsg);
        }
    }
}

function queryOperationsFromDb(aMsg) {
    console.log('query ops for user : ' + aMsg.from.username);
    let replyMessage = '';
    let opTypeStr = '';
    let userId = aMsg.from.id.toString();
    mongoOperationsTable.find({ $or: [{from_user_id: userId}, {to_user_id: userId}]}, function(err, doc) {
        doc.forEach(function (item) {
            let locMsg = '';

            let txLink = `<a href="${txExplorerLink}${item.tx_id}">TxHash</a>`;
            if (item.op_type === '1') {
                opTypeStr = 'Withdraw';
                locMsg += opTypeStr + ' ' + item.op_amount + ' ' + item.token + ' | ' + txLink;
            } else {
                if (item.op_type === '2') {
                    opTypeStr = 'tip';
                } else if (item.op_type === '3') {
                    opTypeStr = 'giveaway';
                } else if (item.op_type === '4') {
                    opTypeStr = 'rain';

            } else if (item.op_type === '5' || item.op_type === '6') {
                opTypeStr = 'lottery';
            }
                if (userId === item.from_user_id) {
                    locMsg += 'Sent (' + opTypeStr + ') ' + item.op_amount + ' ' + item.token + ' to ';
                    locMsg += item.to_user + ' | ' + txLink;
                } else {
                    locMsg += 'Received (' + opTypeStr + ') ' + item.op_amount + ' ' + item.token + ' from ';
                    locMsg += item.from_user + ' | ' + txLink;
                }
            }

            replyMessage = splitLongMessage(replyMessage, locMsg, aMsg.chat.id) ;//,  generateMainKeyboard(aMsg.from.id));
        }, function(err) {
            if (err) {
                console.log('error in ForEach : ' + err);
            } else {
                if (!replyMessage) {
                    replyMessage = 'No operations yet';
                }
                console.log(replyMessage);
                setTimeout(sendMessageByBot, 1000, aMsg.chat.id, replyMessage, aMsg.from.id, false);//, generateMainKeyboard(aMsg.from.id));
            }
        });
    });
}

function generateHelpString(aUserName) {
    let botAnswer = '';
    for (let i = 0; i < catchPhrases.helpCommand.length; ++i) {
        let str = catchPhrases.helpCommand[i] + '\n';
        str = str.replace('zz', chatCmdMod);
        str = str.replace('zz', chatCmdMod);
        botAnswer += str;
    }
    if (getAdminRights(aUserName)) {
        for (let j = 0; j < catchPhrases.helpCommandAdmin.length; ++j) {
            botAnswer += catchPhrases.helpCommandAdmin[j] + '\n';
        }
    }
    // Delete last line break
    return botAnswer.trim();
}

function getAdminRights(aUserName)
{
    if (aUserName) {
        return aUserName.toUpperCase().trim() === 'KUTUK';
    } else {
        return false;
    }
}

function isUserIdRegistered(aUserId, aCallback, aMessage, aCallbackParam) {
    console.log('Finding by UserId ' + aUserId);
        let id = aUserId.toString();
        console.log('Looking DB for  ' + id);
        mongoUsersTable.findOne({user_id: id}, function (err, dbItem) {
            if (err) {
                console.log('An error has occurred ' + err);
            } else {
                if (dbItem) {
                } else {
                    console.log('no user in DB');
                }
                aCallback(aMessage, dbItem, aCallbackParam);
            }
        })
}

function isUserRegistered(aUserName, aCbkFunc, aMsg) {
    console.log('Finding by UserName ' +  aUserName);
    if (Number(aUserName)) {
        let messageText = 'wrong UserName : ' + aUserName;
        console.log(messageText);
        sendMessageByBot(aMsg.chat.id, messageText, aMsg.from.id, false);//, generateMainKeyboard(aMsg.from.id));
        return;
    }
    let toUserName = aUserName.replace('@', '');
    mongoUsersTable.find({user_name: toUserName}, { collation: {locale: 'en' , strength: 2}, limit: 1})
        .toArray(function(err, result)  {
        if (err) {
            console.log('An error has occurred ' + err);
        } else {
           // console.log(item);
            let item = null;
            if (result) {
                item = result[0];
            }
            aCbkFunc(aMsg, item, toUserName );
        }
    })
}

// Adding new User (msg - message from telegram, item - user information from Mongo)
function addU(aMessage, aDbItem, aCallback, aCallbackParam)
{
    let localMessage;
    // noinspection JSUnresolvedVariable
    let replyMessage = aMessage.reply_to_message;
    if (replyMessage) {
        localMessage = replyMessage;
    } else {
        localMessage = aMessage;
    }

    let userName = localMessage.from.username;
    let messageUserId = localMessage.from.id;

    let isAdmin = chatAdminsSet.has(messageUserId);
    let userId = localMessage.from.id.toString();
    if (!aDbItem) { // not in DB yet
        mongoUsersTable.insertOne({ user_id: userId,
            user_name: userName,
            user_first_name: localMessage.from.first_name,
            user_last_name: localMessage.from.last_name,
            user_reg_date: new Date(),
            is_admin: isAdmin ? 1 : null,
            wallet_amount: 0
        }).then(function() {
            if (aCallback) {
                // console.log('added NEW user to db : ' + walletAddress);
                isUserIdRegistered(userId, aCallback, aMessage, aCallbackParam);
            }
        });
     } else {
        mongoUsersTable.updateOne({ user_id: userId},
        {$set: {is_admin: isAdmin ? 1 : null}})
            .catch(e => {
                console.log('mongo error: ', e);
            })
    }
}

// Start Register process
function addUserToDB(aMessage) {
    isUserIdRegistered (aMessage.from.id, addU, aMessage);
}

function balU(aMsg, aItem) {
    let aMessageChatId = aMsg.chat.id;
    let messageUserId = aMsg.from.id;
    if (aItem)     {
        // query balance from blokchain
        if (aItem.wallet_address) {
            cloCheckBalance(aItem, aMsg, finalizeGetBalance);
        } else {
            let message = 'Please register Wallet';
            sendMessageByBot(aMessageChatId, message, messageUserId,false); //, generateMainKeyboard(messageUserId));
        }
    } else {
        console.log('User not registered');
        let message = 'Please register Wallet';
        sendMessageByBot(aMessageChatId, message, messageUserId,false); //, generateMainKeyboard(messageUserId));
    }
}

function getUserBalance(aMsg) {
    isUserIdRegistered (aMsg.from.id, balU, aMsg);
}

function sendReplyMessageByBot(aChatId, aMessage, aMsgId, aDeleteFlag = true) {
    if (aChatId && aMessage) {
        // Return Promise
        return new Promise(function(resolve) {
            return bot.sendMessage(aChatId, aMessage, {
                disable_web_page_preview: true,
                parse_mode: 'HTML',
                reply_to_message_id: aMsgId
            }).then(function(response) {
                resolve(response);
                if (toDeleteMessage && aDeleteFlag) {
                    setTimeout(deleteBotMessage, deleteMessageInterval, response);
                }
            }).catch(function (error) {
                console.log('send message error: ' + error)
            });
        });
    }
}

// Delete Telegram message
function deleteBotMessage (aMsg) {
    console.log('deleteBotMessage' + aMsg);
    bot.deleteMessage(aMsg.chat.id, aMsg.message_id)
        .catch(function (error) {
            console.log('delete message error: ' + error)
        });
}

function dbSetInactiveUser (aUserId, aInactiveTypeId) {
    console.log('marking user in DB as inactive');
    mongoUsersTable.updateOne({user_id: aUserId.toString(), inactive: null},
        {$set: {inactive: aInactiveTypeId, inactive_date: new Date()}})
        .catch(function(error) {
           console.log('error DB update:' + error);
        });
}

function parseSendMessageError(aUserId, error) {
    if (aUserId) {
        console.log('send message error. User: ' + aUserId + ' / ' + error);
        let strError = error.toString();
        if (strError.indexOf('blocked') !== -1) { // blocked by user
            dbSetInactiveUser(aUserId, "blocked");
        } else if (strError.indexOf('deactivated') !== -1) { // user deactivated
            dbSetInactiveUser(aUserId, "deactivated");
        } else if (strError.indexOf('initiate') !== -1) { // user not started chat
            dbSetInactiveUser(aUserId, "nochat");
        } else if (strError.indexOf('chat not found') !== -1) { // user not started chat
            dbSetInactiveUser(aUserId, "nochat");
        }
    } else {
        console.log('send message error: '+ error);
    }
}

// Send Telegram message
function sendMessageByBot(aChatId, aMessage, aUserId, aDeleteFlag = false, aKey, aCustomDeleteInterval) {
  if (aChatId && aMessage) {
      // Return Promise
      return new Promise(function(resolve) {
          let isBotChat = (aChatId === aUserId);
          return bot.sendMessage(aChatId, aMessage, {
              disable_web_page_preview: true,
              reply_markup: (isBotChat) ? ((aKey) ? aKey : null) : null, //{remove_keyboard: true},
              parse_mode: 'HTML'
          }).then(function(response) { // 1 sec delay .delay(1000).then(function(response) {
              resolve(response);
              if (toDeleteMessage && aDeleteFlag) {
                  if (aCustomDeleteInterval) {
                      setTimeout(deleteBotMessage, aCustomDeleteInterval, response); // delete unneeded message
                  } else {
                      setTimeout(deleteBotMessage, deleteMessageInterval, response); // delete unneeded message
                  }
              }
          }).catch(function (error) {
              parseSendMessageError(aUserId, error);
          });
      });
  }
}

function adminBotStop(aMsg) {
    if (globalTxQuery.length === 0) {
        if (restartTimeOut) {
            console.log('not stopping - restart active');
        } else {
            console.log('stopping process');
            Exec('pm2 stop ' + pm2name, function(err, stdout) {
                if (err) {
                    console.log(err);
                } else {
                    console.log(stdout);
                }
            });
        }
    } else {
        let messageText = 'Can not stop - txQuery is not empty';
        sendMessageByBot(aMsg.chat.id, messageText);
    }
}

function adminBotRestart(aMsg) {
    if (globalTxQuery.length === 0) {
        if (restartTimeOut) {
            console.log('not killing - restart active');
        } else {
            console.log('killing process');
            process.exit();
        }
    } else {
        let messageText = 'Can not restart - txQuery is not empty';
        sendMessageByBot(aMsg.chat.id, messageText);
    }
}

function adminGetConfig(aMsg) {
    let message = JSON.stringify(dbconfig, null, 4);
    sendMessageByBot(aMsg.chat.id, message, aMsg.chat.id);
}

function splitTokenName(aTokenName) {
    let ps = aTokenName.indexOf('-');
    let str = aTokenName;
    if (ps > 0) {
        str = aTokenName.substr(0, ps);
    }
    return str;
}

function finalizeGetBalance(aItem, aMsg, aBalance) {
    let messageChatId = aMsg.chat.id;
    let messageUserId = aMsg.from.id;
    // console.log('User balance : ' + aBalance);
    let message = `Your balance: `;
    for (let i = 0; i < aBalance.length; i++) {
        let bal = aBalance[i];
        let val = bal.free;
        let tkn = splitTokenName(bal.symbol);
        message += `\n${val} ${tkn}`
    }
    if (aBalance.length === 0) {
        message += `\n0 BNB`;
    }
    sendMessageByBot(messageChatId, message, messageUserId, false); //, generateMainKeyboard(messageUserId));
}

// ---- BlockChain functions
function cloCheckBalance(aItem, aMsg, aCallBack) {
    let wallet = aItem.wallet_address;
    // noinspection JSUnresolvedFunction
    bnbClient.getBalance(wallet)
        .then(x => {
            aCallBack(aItem, aMsg, x);
        })
        .catch( err => {
        console.log("error fetching balance : " + err);
    });
}

function cloneObj(obj) {
    if (null == obj || "object" != typeof obj) return obj;
    let copy = obj.constructor();
    for (let attr in obj) {
        if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
    }
    return copy;
}

function addTxToQuery(aItem, aOutputs, aDestItems, aValue, aMsg, aCallBack) {
    console.log('addTxToQuery');
    console.log('aItem: %s', aItem.wallet_address);
    let src = cloneObj(aItem);
    let msg = aMsg;
    let cbk = cloneObj(aCallBack);
    let txObj = {
        sourceItem: src,
        outputs: aOutputs,
        destItems: aDestItems,
        value: aValue,
        tgMessage: msg,
        fnCallBack: cbk
    };
    globalTxQuery.push(txObj);
}

async function sendTxByInterval() {
    if (globalTxQuery.length) {
        let txObj = globalTxQuery.shift();
        let aItem = txObj.sourceItem;
        let aDestItems = txObj.destItems;
        let aValue = txObj.value;
        let aMsg = txObj.tgMessage;
        let aOutputs = txObj.outputs;
        let aCallBack = txObj.fnCallBack;

        console.log('sendTxByInterval');
        console.log('aItem: %s', aItem.wallet_address);

        // new func for BNB
        bnbClient.setPrivateKey(aItem.wallet_key);
        if (isDebugMode) {
            console.log('debug mode - no Tx');
            if (aCallBack) {
                for (let i = 0; i < aDestItems.length; i++) {
                    let item = aDestItems[i];
                    aCallBack(aItem, item, aValue, aMsg, '0x0');
                }
            }
            setTimeout(sendTxByInterval, txInterval);
        } else {
            console.log(JSON.stringify(aOutputs));
            bnbClient.multiSend(aItem.wallet_address, aOutputs)
                .then(res => {
                    let txHash = res.result[0].hash;
                    console.log('tx success: ', txHash);
                    if (res.status === 200) {
                        if (aCallBack) {
                            for (let i = 0; i < aDestItems.length; i++) {
                                let item = aDestItems[i];
                                aCallBack(aItem, item, aValue, aMsg, txHash);
                            }
                        }
                    } else {
                        console.error('Tx error : ', res);
                    }
                    setTimeout(sendTxByInterval, txInterval);
                })
                .catch(err => {
                    setTimeout(sendTxByInterval, txInterval);
                    console.log(err);
                });
        }
    } else {
        setTimeout(sendTxByInterval, txInterval * 3);
    }
}