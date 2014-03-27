var Stratum = require('stratum-pool');
var Vardiff = require('stratum-pool/lib/varDiff.js');
var net     = require('net');



var MposCompatibility = require('./mposCompatibility.js');
var ShareProcessor = require('./shareProcessor.js');

module.exports = function(logger){


    var poolConfigs  = JSON.parse(process.env.pools);
    var portalConfig = JSON.parse(process.env.portalConfig);

    var forkId       = process.env.forkId;
    
    var pools        = {};

    var proxyStuff = {}
    //Handle messages from master process sent via IPC
    process.on('message', function(message) {
        switch(message.type){
            case 'blocknotify':
                var pool = pools[message.coin.toLowerCase()]
                if (pool) pool.processBlockNotify(message.hash)
                break;
            case 'switch':
                var newCoinPool = pools[message.coin.toLowerCase()];
                if (newCoinPool) {
                    var oldPool = pools[proxyStuff.curActivePool];
                    oldPool.relinquishMiners(
                        function (miner, cback) { 
                            // relinquish miners that are attached to one of the "Auto-switch" ports and leave the others there.
                            cback(typeof(portalConfig.proxy.ports[miner.client.socket.localPort]) !== 'undefined')
                        }, 
                        function (clients) {
                            newCoinPool.attachMiners(clients);
                            proxyStuff.curActivePool = message.coin.toLowerCase();
                        }
                    )
                    
                }
                break;
        }
    });


    Object.keys(poolConfigs).forEach(function(coin) {


        var poolOptions = poolConfigs[coin];

        var logSystem = 'Pool';
        var logComponent = coin;
        var logSubCat = 'Fork ' + forkId;


        var handlers = {
            auth: function(){},
            share: function(){},
            diff: function(){}
        };

        var internalEnabled = poolOptions.shareProcessing && poolOptions.shareProcessing.internal && poolOptions.shareProcessing.internal.enabled;
        var mposEnabled = poolOptions.shareProcesssing && poolOptions.shareProcessing.mpos && poolOptions.shareProcessing.mpos.enabled;

        if (!internalEnabled && !mposEnabled){
            logger.error('Master', coin, 'Share processing is not configured so a pool cannot be started for this coin.');
            return;
        }

        var shareProcessing = poolOptions.shareProcessing;

        //Functions required for MPOS compatibility
        if (shareProcessing && shareProcessing.mpos && shareProcessing.mpos.enabled){
            var mposCompat = new MposCompatibility(logger, poolOptions)

            handlers.auth = function(workerName, password, authCallback){
                mposCompat.handleAuth(workerName, password, authCallback);
            };

            handlers.share = function(isValidShare, isValidBlock, data){
                mposCompat.handleShare(isValidShare, isValidBlock, data);
            };

            handlers.diff = function(workerName, diff){
                mposCompat.handleDifficultyUpdate(workerName, diff);
            }
        }

        //Functions required for internal payment processing
        else if (shareProcessing && shareProcessing.internal && shareProcessing.internal.enabled){

            var shareProcessor = new ShareProcessor(logger, poolOptions)

            handlers.auth = function(workerName, password, authCallback){

                if(shareProcessing.internal.validateWorkerAddress) {
                    pool.daemon.cmd('validateaddress', [workerName], function(results){
                        var isValid = results.filter(function(r){return r.response.isvalid}).length > 0;
                        authCallback(isValid);
                    });
                }
                else {
                    authCallback(true);
                }
                
            };

            handlers.share = function(isValidShare, isValidBlock, data){
                shareProcessor.handleShare(isValidShare, isValidBlock, data);
            };
        }

        var authorizeFN = function (ip, workerName, password, callback) {
            handlers.auth(workerName, password, function(authorized){

                var authString = authorized ? 'Authorized' : 'Unauthorized ';

                logger.debug(logSystem, logComponent, logSubCat, authString + ' ' + workerName + ':' + password + ' [' + ip + ']');
                callback({
                    error: null,
                    authorized: authorized,
                    disconnect: false
                });
            });
        };


        var pool = Stratum.createPool(poolOptions, authorizeFN, logger);
        pool.on('share', function(isValidShare, isValidBlock, data){

            var shareData = JSON.stringify(data);

            if (data.solution && !isValidBlock)
                logger.debug(logSystem, logComponent, logSubCat, 'We thought a block solution was found but it was rejected by the daemon, share data: ' + shareData);

            else if (isValidBlock)
                logger.debug(logSystem, logComponent, logSubCat, 'Block solution found: ' + data.solution);


            if (isValidShare)
                logger.debug(logSystem, logComponent, logSubCat, 'Valid share of difficulty ' + data.difficulty + ' by ' + data.worker + ' [' + data.ip + ']' );

            else if (!isValidShare)
                logger.debug(logSystem, logComponent, logSubCat, 'Invalid share submitted, share data: ' + shareData);


            handlers.share(isValidShare, isValidBlock, data)


        }).on('difficultyUpdate', function(workerName, diff){
            handlers.diff(workerName, diff);
        }).on('log', function(severity, text) {
            logger[severity](logSystem, logComponent, logSubCat, text);
        });
        pool.start();
        pools[poolOptions.coin.name.toLowerCase()] = pool;
    });

    
    if (typeof(portalConfig.proxy) !== 'undefined' && portalConfig.proxy.enabled === true) {

        Object.keys(pools).forEach(function(coin) {
            var p = pools[coin];

            var internalEnabled = p.shareProcessing && p.shareProcessing.internal && p.shareProcessing.internal.enabled;
            var mposEnabled = p.shareProcesssing && p.shareProcessing.mpos && p.shareProcessing.mpos.enabled;

            if (!internalEnabled && !mposEnabled){
                logger.error('Master', coin, 'Share processing is not configured so a pool cannot be started for this coin.');
                console.log(coin, '____________________________________________ DISABLED');
                return;
            } else {
                proxyStuff.curActivePool = pools[coin];
                console.log(coin,' ____________________________________________ ACTIVE');
                return;
            }
        });

        proxyStuff.proxys = {};
        proxyStuff.varDiffs = {};
        Object.keys(portalConfig.proxy.ports).forEach(function(port) {
            proxyStuff.varDiffs[port] = new Vardiff(port, portalConfig.proxy.ports[port].varDiff);
        });
        Object.keys(pools).forEach(function (coinName) {
            var p = pools[coinName];
            Object.keys(proxyStuff.varDiffs).forEach(function(port) {
                p.setVarDiff(port, proxyStuff.varDiffs[port]);
            });
        });

        Object.keys(portalConfig.proxy.ports).forEach(function (port) {
            proxyStuff.proxys[port] = net .createServer({allowHalfOpen: true}, function(socket) {
                console.log(proxyStuff.curActivePool);
                pools[proxyStuff.curActivePool].getStratumServer().handleNewClient(socket);
            }).listen(parseInt(port), function(){
                console.log("Proxy listening on " + port);
            });
        });


        
    }
};