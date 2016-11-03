_           = require 'lodash'
async       = require 'async'
MeshbluHttp = require 'meshblu-http'
Soldiers    = require './soldiers'
debug       = require('debug')('now-man-worker:worker')

class Worker
  constructor: (options)->
    { @meshbluConfig, @client, @queueName, @queueTimeout } = options
    { database, @disableSendTimestamp } = options
    throw new Error('Worker: requires meshbluConfig') unless @meshbluConfig?
    throw new Error('Worker: requires client') unless @client?
    throw new Error('Worker: requires queueName') unless @queueName?
    throw new Error('Worker: requires queueTimeout') unless @queueTimeout?
    throw new Error('Worker: requires database') unless database?
    @soldiers = new Soldiers { database }
    @shouldStop = false
    @isStopped = false

  doWithNextTick: (callback) =>
    # give some time for garbage collection
    process.nextTick =>
      @do (error) =>
        setTimeout =>
          callback error
        , 100

  do: (callback) =>
    @client.brpop @queueName, @queueTimeout, (error, result) =>
      return callback error if error?
      return callback() unless result?

      [ queue, recordId ] = result

      @soldiers.get { recordId }, (error, data) =>
        return callback error if error?
        return callback null unless data?
        @sendMessage data, (error) =>
          return callback error if error?
          return @soldiers.remove { recordId }, callback if data.fireOnce
          @soldiers.update { recordId }, callback

    return # avoid returning promise

  sendMessage: (data, callback) =>
    return callback() if _.isEmpty data
    { uuid, token, nodeId, sendTo, transactionId } = data

    config = _.defaults {uuid, token}, @meshbluConfig
    meshbluHttp = new MeshbluHttp config

    message =
      devices: [sendTo]
      payload:
        from: nodeId
        transactionId: transactionId ? nodeId

    message.payload.timestamp = _.now() unless @disableSendTimestamp

    debug 'messaging', message
    meshbluHttp.message message, callback

  run: (callback) =>
    async.doUntil @doWithNextTick, (=> @shouldStop), =>
      @isStopped = true
      callback null

  stop: (callback) =>
    @shouldStop = true

    timeout = setTimeout =>
      clearInterval interval
      callback new Error 'Stop Timeout Expired'
    , 5000

    interval = setInterval =>
      return unless @isStopped
      clearInterval interval
      clearTimeout timeout
      callback()
    , 250

module.exports = Worker
