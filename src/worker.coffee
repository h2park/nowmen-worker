async = require 'async'
_ = require 'lodash'

class Worker
  constructor: (options={})->
    { @MeshbluHttp, @meshbluConfig, @client, @queueName, @queueTimeout } = options
    throw new Error('Worker: requires Meshblu HTTP') unless @MeshbluHttp?
    throw new Error('Worker: requires Meshblu config') unless @meshbluConfig?
    throw new Error('Worker: requires client') unless @client?
    throw new Error('Worker: requires queueName') unless @queueName?
    throw new Error('Worker: requires queueTimeout') unless @queueTimeout?
    @shouldStop = false
    @isStopped = false

  doWithNextTick: (callback) =>
    # give some time for garbage collection
    process.nextTick =>
      @do (error) =>
        process.nextTick =>
          callback error

  do: (callback) =>
    @client.brpop @queueName, @queueTimeout, (error, result) =>
      return callback error if error?
      return callback() unless result?

      [ queue, data ] = result
      try
        data = JSON.parse data
      catch error
        return callback error

      @sendMessage data, callback

    return # avoid returning promise

  sendMessage: (data, callback) =>
    return callback() if _.isEmpty data
    { uuid, token, nodeId, sendTo, transactionId } = data

    config = _.defaults {uuid, token}, @meshbluConfig
    meshbluHttp = new @MeshbluHttp config

    message =
      devices: [sendTo]
      payload:
        from: nodeId
        transactionId: transactionId
        timestamp: _.now()

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
      return unless @isStopped?
      clearInterval interval
      clearTimeout timeout
      callback()
    , 250

module.exports = Worker
