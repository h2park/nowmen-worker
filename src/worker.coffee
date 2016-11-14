_           = require 'lodash'
async       = require 'async'
MeshbluHttp = require 'meshblu-http'
Soldiers    = require './soldiers'
debug       = require('debug')('nowmen-worker:worker')
overview    = require('debug')('nowmen-worker:worker')

class Worker
  constructor: (options)->
    { @meshbluConfig, @client, @queueName, @queueTimeout } = options
    { database, @disableSendTimestamp, @sendUnixTimestamp } = options
    { @consoleError } = options
    throw new Error('Worker: requires meshbluConfig') unless @meshbluConfig?
    throw new Error('Worker: requires client') unless @client?
    throw new Error('Worker: requires queueName') unless @queueName?
    throw new Error('Worker: requires queueTimeout') unless @queueTimeout?
    throw new Error('Worker: requires database') unless database?
    @consoleError ?= console.error
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

      [ queue, rawData ] = result
      try
        {recordId, timestamp} = JSON.parse rawData
      catch error
        console.error 'Unable to parse', rawData
        return callback error

      @soldiers.get { recordId }, (error, data) =>
        return callback error if error?
        return callback null unless data?
        @sendMessage {data, timestamp}, (error) =>
          return @handleSendMessageError { data, error }, callback if error?
          return @soldiers.remove { recordId }, callback if data.fireOnce
          @soldiers.update { recordId }, callback

    return # avoid returning promise

  sendMessage: ({data, timestamp}, callback) =>
    { uuid, token, nodeId, sendTo, transactionId } = data

    config = _.defaults {uuid, token}, @meshbluConfig
    config.timeout = 3000
    debug 'meshblu config', config, { uuid, token }
    meshbluHttp = new MeshbluHttp config

    message =
      devices: [sendTo]
      payload:
        from: nodeId

    message.payload.transactionId = transactionId if transactionId

    message.payload.timestamp = _.now() unless @disableSendTimestamp
    message.payload.unixTimestamp = timestamp if @sendUnixTimestamp

    overview 'messaging', message
    meshbluHttp.message message, (error) =>
      callback error

  handleSendMessageError: ({ error, data }, callback) =>
    { sendTo, nodeId } = data
    if error?.code == 'ESOCKETTIMEDOUT'
      @consoleError 'Send message timeout', { sendTo, nodeId }
      return callback null
    if error?.code?
      @consoleError "Send message #{error?.code}", { sendTo, nodeId }
      return callback null
    return callback error

  run: (callback) =>
    async.doUntil @doWithNextTick, @shouldStop, (error) =>
      @consoleError 'Worker Run Error', error if error?
      @isStopped = true
      callback error

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
