var mysql = require('mysql');
var cluster = require('cluster');
module.exports = function(logger, poolConfig){

    var mposConfig = poolConfig.shareProcessing.mpos;
    var coin = poolConfig.coin.name;

    var connection;

    var logIdentify = 'MPOS';

    function connect(){
        connection = mysql.createConnection({
            host: mposConfig.host,
            port: mposConfig.port,
            user: mposConfig.user,
            password: mposConfig.password,
            database: mposConfig.database
        });
        connection.connect(function(err){
            if (err)
                logger.error(logIdentify, 'mysql', 'Could not connect to mysql database: ' + JSON.stringify(err))
            else{
                logger.debug(logIdentify, 'mysql', 'Successful connection to MySQL database');
            }
        });
        connection.on('error', function(err){
            if(err.code === 'PROTOCOL_CONNECTION_LOST') {
                logger.warning(logIdentify, 'mysql', 'Lost connection to MySQL database, attempting reconnection...');
                connect();
            }
            else{
                logger.error(logIdentify, 'mysql', 'Database error: ' + JSON.stringify(err))
            }
        });
    }
    connect();

    this.handleAuth = function(workerName, password, authCallback){

        connection.query(
            'SELECT password FROM pool_worker WHERE username = LOWER(?)',
            [workerName],
            function(err, result){
                if (err){
                    logger.error(logIdentify, 'mysql', 'Database error when authenticating worker: ' +
                        JSON.stringify(err));
                    authCallback(false);
                }
                else if (!result[0])
                    authCallback(false);
                else if (mposConfig.stratumAuth === 'worker')
                    authCallback(true);
                else if (result[0].password === password)
                    authCallback(true)
                else
                    authCallback(false);
            }
        );

    };

    this.handleShare = function(isValidShare, isValidBlock, shareData){

        var dbData = [
            shareData.ip,
            shareData.worker,
            isValidShare ? 'Y' : 'N', 
            isValidBlock ? 'Y' : 'N',
            shareData.difficulty,
            typeof(shareData.error) === 'undefined' ? null : shareData.error,
            typeof(shareData.solution) === 'undefined' ? '' : shareData.solution
        ];
        connection.query(
            'INSERT INTO `shares` SET time = NOW(), rem_host = ?, username = ?, our_result = ?, upstream_result = ?, difficulty = ?, reason = ?, solution = ?',
            dbData,
            function(err, result) {
                if (err)
                    logger.error(logIdentify, 'mysql', 'Insert error when adding share: ' + JSON.stringify(err));
                else
                    logger.debug(logIdentify, 'mysql', 'Share inserted');
            }
        );
    };

    this.handleDifficultyUpdate = function(workerName, diff){

        connection.query(
            'UPDATE `pool_worker` SET `difficulty` = ' + diff + ' WHERE `username` = ' + connection.escape(workerName),
            function(err, result){
                if (err)
                    logger.error(logIdentify, 'mysql', 'Error when updating worker diff: ' +
                        JSON.stringify(err));
                else if (result.affectedRows === 0){
                    connection.query('INSERT INTO `pool_worker` SET ?', {username: workerName, difficulty: diff});
                }
                else
                    console.log('Updated difficulty successfully', result);
            }
        );
    };


};