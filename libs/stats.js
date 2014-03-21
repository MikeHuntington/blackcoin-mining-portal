var redis = require('redis');
var async = require('async');

var os = require('os');
var Stratum = require('stratum-pool');


module.exports = function(logger, portalConfig, poolConfigs){

    var _this = this;

    var redisClients = [];

    var algoMultipliers = {
        'scrypt': Math.pow(2, 16),
        'sha256': Math.pow(2, 32)
    };

    Object.keys(poolConfigs).forEach(function(coin){
        var poolConfig = poolConfigs[coin];
        var internalConfig = poolConfig.shareProcessing.internal;
        var redisConfig = internalConfig.redis;

        for (var i = 0; i < redisClients.length; i++){
            var client = redisClients[i];
            if (client.client.port === redisConfig.port && client.client.host === redisConfig.host){
                client.coins.push(coin);
                return;
            }
        }
        redisClients.push({
            coins: [coin],
            client: redis.createClient(redisConfig.port, redisConfig.host)
        });
    });


    this.stats = {};
    this.poolConfigs = poolConfigs;


    this.getMinerStats = function(address, cback){

        var minerStats = {};



        async.each(redisClients[0].coins, function(coin, cb){


            var daemon = new Stratum.daemon.interface([_this.poolConfigs[coin].shareProcessing.internal.daemon]);


            minerStats[coin] = {};
            var client = redisClients[0].client;
            
            async.waterfall([

                /* Call redis to get an array of rounds - which are coinbase transactions and block heights from submitted
                   blocks. */
                function(callback){

                    client.smembers(coin + '_blocksPending', function(error, results){

                        if (error){
                            paymentLogger.error('redis', 'Could get blocks from redis ' + JSON.stringify(error));
                            callback('done - redis error for getting blocks');
                            return;
                        }
                        if (results.length === 0){
                            callback('done - no pending blocks in redis');
                            return;
                        }

                        var rounds = results.map(function(r){
                            var details = r.split(':');
                            return {txHash: details[0], height: details[1], reward: details[2], serialized: r};
                        });

                        callback(null, rounds);
                    });
                },

                /* Does a batch rpc call to daemon with all the transaction hashes to see if they are confirmed yet.
                It also adds the block reward amount to the round object - which the daemon gives also gives us. */
                function(rounds, callback){

                    var batchRPCcommand = rounds.map(function(r){
                        return ['gettransaction', [r.txHash]];
                    });

                    daemon.batchCmd(batchRPCcommand, function(error, txDetails){

                        if (error || !txDetails){
                            callback('done - daemon rpc error with batch gettransactions ' + JSON.stringify(error));
                            return;
                        }

                        txDetails = txDetails.filter(function(tx){
                            if (tx.error || !tx.result){
                                console.log('error with requesting transaction from block daemon: ' + JSON.stringify(t));
                                return false;
                            }
                            return true;
                        });

                        var orphanedRounds = [];
                        var confirmedRounds = [];
                        //Rounds that are not confirmed yet are removed from the round array
                        //We also get reward amount for each block from daemon reply
                        rounds.forEach(function(r){

                            var tx = txDetails.filter(function(tx){return tx.result.txid === r.txHash})[0];

                            if (!tx){
                                console.log('daemon did not give us back a transaction that we asked for: ' + r.txHash);
                                return;
                            }


                            r.category = tx.result.details[0].category;

                            if (r.category === 'orphan'){
                                orphanedRounds.push(r);

                            }
                            else if (r.category === 'generate'){
                                r.amount = tx.result.amount;
                                r.magnitude = r.reward / r.amount;
                                confirmedRounds.push(r);
                            }

                        });

                        if (orphanedRounds.length === 0 && confirmedRounds.length === 0){
                            callback('done - no confirmed or orhpaned rounds');
                        }
                        else{
                            callback(null, confirmedRounds, orphanedRounds);
                        }
                    });
                }

            ], function(err, confirmedRounds, orphanedRounds) {

                minerStats[coin].rounds = {
                    confirmed:confirmedRounds,
                    orphaned:orphanedRounds
                };

                cb();
            });

        }, function(err){
            _this.stats.minerStats = minerStats;
            cback();
        });
    };


    this.getStats = function(callback){

        var allCoinStats = [];

        async.each(redisClients, function(client, callback){
            var windowTime = (((Date.now() / 1000) - portalConfig.website.hashrateWindow) | 0).toString();
            var redisCommands = [];
            var commandsPerCoin = 4;

            //Clear out old hashrate stats for each coin from redis
            client.coins.forEach(function(coin){
                redisCommands.push(['zremrangebyscore', coin + '_hashrate', '-inf', '(' + windowTime]);
                redisCommands.push(['zrangebyscore', coin + '_hashrate', windowTime, '+inf']);
                redisCommands.push(['hgetall', coin + '_stats']);
                redisCommands.push(['scard', coin + '_blocksPending']);
            });


            client.client.multi(redisCommands).exec(function(err, replies){
                if (err){
                    console.log('error with getting hashrate stats ' + JSON.stringify(err));
                    callback(err);
                }
                else{
                    for(var i = 0; i < replies.length; i += commandsPerCoin){
                        var coinStats = {
                            coinName: client.coins[i / commandsPerCoin | 0],
                            hashrates: replies[i + 1],
                            poolStats: replies[i + 2],
                            poolPendingBlocks: replies[i + 3]
                        };
                        allCoinStats.push(coinStats)

                    }
                    callback();
                }
            });
        }, function(err){
            if (err){
                console.log('error getting all stats' + JSON.stringify(err));
                callback();
                return;
            }

            var portalStats = {
                global:{
                    workers: 0,
                    hashrate: 0
                },
                pools: allCoinStats
            };

            allCoinStats.forEach(function(coinStats){
                coinStats.workers = {};
                coinStats.shares = 0;
                coinStats.hashrates.forEach(function(ins){
                    var parts = ins.split(':');
                    var workerShares = parseInt(parts[0]);
                    coinStats.shares += workerShares;
                    var worker = parts[1];
                    if (worker in coinStats.workers)
                        coinStats.workers[worker] += workerShares
                    else
                        coinStats.workers[worker] = workerShares
                });
                var shareMultiplier = algoMultipliers[poolConfigs[coinStats.coinName].coin.algorithm];
                var hashratePre = shareMultiplier * coinStats.shares / portalConfig.website.hashrateWindow;
                coinStats.hashrate = hashratePre / 1e3 | 0;
                delete coinStats.hashrates;
                portalStats.global.hashrate += coinStats.hashrate;
                portalStats.global.workers += Object.keys(coinStats.workers).length;
            });
            _this.stats = portalStats;
            callback();
        });

    };
};

