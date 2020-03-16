# TrustWallet.Tip.Bot
Bot for telegram groups used for tipping in Binance Chain coins and tokens

Tipping bot for TrustWallet (https://trustwallet.com/)

Bot discussion thread: BnbTelegramTipBot (https://t.me/BnbTelegramTipBot)


Work in channels supported (except TIP command)

/start — Start dialog with Bot

/help — This help menu

Commands for common chats: 

/twstat - show some common statistics

/twtip - tip user

( Usage: /twtip @toUserName value )

( Usage in "Reply" message: /twtip value )

/twgiveaway - Giveaway coins in a group to first who claim

( Usage: /twgiveaway value )

( every user can Claim once in 24h )

/twgiveauto - Auto create Giveaway with time interval (in Hours)

( Usage: /twgiveauto each-time-value interval)

( giveauto will be stopped if not enough coins )

/twgivestop - command to STOP /twgiveauto

/twrain - spread some N CLO among X users 

( Usage: /twrain _value_ NUMBER_of_Users )

( function can take a long time ) 

Commands to do Airdrop for retweets

/twit - start collection of retweets for a tweet (Usage: /twtweet tweet_URL)

/twrtrain - call Rain for users who retweeted a tweet (Usage: /twrtrain value tweet_URL)
