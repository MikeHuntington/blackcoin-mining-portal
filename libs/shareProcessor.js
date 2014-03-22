var redis = require('redis');
var Stratum = require('stratum-pool');



/*
This module deals with handling shares when in internal payment processing mode. It connects to a redis
database and inserts shares with the database structure of:

key: coin_name + ':' + block_height
value: a hash with..
        key:

 */


module.exports = function(logger, poolConfig){

    var internalConfig = poolConfig.shareProcessing.internal;
    var redisConfig = internalConfig.redis;
    var coin = poolConfig.coin.name;

    var connection;

    function connect(){

        var reconnectTimeout;

        connection = redis.createClient(redisConfig.port, redisConfig.host);
        connection.on('ready', function(){
            clearTimeout(reconnectTimeout);
            logger.debug('redis', 'Successfully connected to redis database');
        });
        connection.on('error', function(err){
            logger.error('redis', 'Redis client had an error: ' + JSON.stringify(err))
        });
        connection.on('end', function(){
            logger.error('redis', 'Connection to redis database as been ended');
            logger.warning('redis', 'Trying reconnection in 3 seconds...');
            reconnectTimeout = setTimeout(function(){
                connect();
            }, 3000);
        });
    }
    connect();



    this.handleShare = function(isValidShare, isValidBlock, shareData){


        var redisCommands = [];

        if (isValidShare){
            redisCommands.push(['hincrby', coin + '_shares:roundCurrent', shareData.worker, shareData.difficulty]);
            redisCommands.push(['hincrby', coin + '_stats', 'validShares', 1]);

            logger.debug('SHARED', shareData);

            /* Stores share diff, worker, and unique value with a score that is the timestamp. Unique value ensures it
               doesn't overwrite an existing entry, and timestamp as score lets us query shares from last X minutes to
               generate hashrate for each worker and pool. */
            redisCommands.push(['zadd', coin + '_hashrate', Date.now() / 1000 | 0, [shareData.difficulty, shareData.worker, Math.random()].join(':')]);
        }
        else{
            redisCommands.push(['hincrby', coin + '_stats', 'invalidShares', 1]);
        }

        if (isValidBlock){
            redisCommands.push(['rename', coin + '_shares:roundCurrent', coin + '_shares:round' + shareData.height]);
            redisCommands.push(['sadd', coin + '_blocksPending', shareData.tx + ':' + shareData.height + ':' + shareData.reward]);
            redisCommands.push(['hincrby', coin + '_stats', 'validBlocks', 1]);
        }
        else if (shareData.solution){
            redisCommands.push(['hincrby', coin + '_stats', 'invalidBlocks', 1]);
        }

        connection.multi(redisCommands).exec(function(err, replies){
            if (err)
                logger.error('redis', 'error with share processor multi ' + JSON.stringify(err));
        });


    };

};