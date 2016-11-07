_           = require 'lodash'
async       = require 'async'
MeshbluHttp = require 'meshblu-http'
Soldiers    = require './soldiers'
debug       = require('debug')('now-man-worker:worker')
overview    = require('debug')('now-man-worker:worker:overview')

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
    @_shouldStop = false
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
    debug 'meshblu config', config, { uuid, token }
    meshbluHttp = new MeshbluHttp config

    message =
      devices: [sendTo]
      payload:
        from: nodeId
        transactionId: transactionId ? nodeId

    message.payload.timestamp = _.now() unless @disableSendTimestamp

    overview 'messaging', message
    meshbluHttp.message message, (error) =>
      if error?.code == 403
        debug 'got a 403'
        return callback null
      callback error

  run: (callback) =>
    async.doUntil @doWithNextTick, @shouldStop, (error) =>
      console.error 'Worker Run Error', error if error?
      @isStopped = true
      callback null

  shouldStop: =>
    overview 'stopping' if @_shouldStop
    return @_shouldStop

  stop: (callback) =>
    @_shouldStop = true

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
