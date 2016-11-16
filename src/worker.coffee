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
    { @consoleError, @timeout } = options
    { concurrency } = options
    throw new Error('Worker: requires meshbluConfig') unless @meshbluConfig?
    throw new Error('Worker: requires client') unless @client?
    throw new Error('Worker: requires queueName') unless @queueName?
    throw new Error('Worker: requires queueTimeout') unless @queueTimeout?
    throw new Error('Worker: requires database') unless database?
    @timeout ?= 3000
    @consoleError ?= console.error
    @soldiers = new Soldiers { database }
    @_shouldStop = false
    @isStopped = false
    concurrency ?= 1
    @queue = async.queue @doTask, concurrency

  doWithNextTick: (callback) =>
    # give some time for garbage collection
    process.nextTick =>
      @do (error) =>
        process.nextTick =>
          callback error

  do: (callback) =>
    debug 'process do'
    @client.brpop @queueName, @queueTimeout, (error, result) =>
      return callback error if error?
      @queue.push { recordId: '582cd44e274cc2d840d17554', timestamp: 'test' }
      return callback() unless result?
      [ _queue, rawData ] = result
      try
        { recordId, timestamp } = JSON.parse rawData
      catch error
        console.error 'Unable to parse', rawData
        @queue.drain = =>
          debug 'drained...'
          callback error
        return
      debug 'insert into queue'
      @queue.push { recordId, timestamp }
      callback null
    return # avoid returning promise

  doAndDrain: (callback) =>
    @do (error) =>
      return callback error if error?
      @queue.drain = callback

  doTask: ({ recordId, timestamp }, callback) =>
    debug 'process queue task'
    @soldiers.get { recordId }, (error, data) =>
      return callback error if error?
      return callback null unless data?
      @sendMessage {data, timestamp}, (error) =>
        return @handleSendMessageError { recordId, data, error }, callback if error?
        return @soldiers.remove { recordId }, callback if data.fireOnce
        @soldiers.update { recordId }, callback

  sendMessage: ({data, timestamp}, callback) =>
    { uuid, token, nodeId, sendTo, transactionId } = data

    config = _.defaults {uuid, token}, @meshbluConfig
    config.timeout = @timeout
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

  handleSendMessageError: ({ error, recordId, data }, callback) =>
    { sendTo, nodeId } = data
    if error?.code == 'ESOCKETTIMEDOUT'
      @consoleError 'Send message timeout', { sendTo, nodeId }
      return callback null
    if error?.code == 403
      @consoleError 'Send message forbidden (Removing record)', { sendTo, nodeId }
      return @soldiers.remove { recordId }, callback
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
    @queue.drain = callback

module.exports = Worker
