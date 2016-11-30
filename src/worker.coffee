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
    { concurrency, @requestTimeout } = options
    throw new Error('Worker: requires meshbluConfig') unless @meshbluConfig?
    throw new Error('Worker: requires client') unless @client?
    throw new Error('Worker: requires queueName') unless @queueName?
    throw new Error('Worker: requires queueTimeout') unless @queueTimeout?
    throw new Error('Worker: requires database') unless database?
    @requestTimeout ?= 5000
    @consoleError ?= @_consoleError
    @soldiers = new Soldiers { database }
    @_shouldStop = false
    @isStopped = false
    concurrency ?= 1
    @queue = async.queue @doTask, concurrency

  _consoleError: =>
    console.error new Date().toString(), arguments...

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
      return callback() unless result?
      [ _queue, rawData ] = result
      try
        { recordId, timestamp, uuid } = JSON.parse rawData
      catch error
        console.error 'Unable to parse', rawData
        @queue.drain = =>
          debug 'drained...'
          callback error
        return
      debug 'insert into queue'
      @queue.push { recordId, uuid, timestamp }
      callback null
    return # avoid returning promise

  doAndDrain: (callback) =>
    @do (error) =>
      return callback error if error?
      @queue.drain = callback

  doTask: ({ recordId, uuid, timestamp }, callback) =>
    debug 'process queue task'
    @soldiers.get { recordId, uuid }, (error, data) =>
      return callback error if error?
      return callback null unless data?
      @sendMessage {data, timestamp}, (error) =>
        return @handleSendMessageError { data, error }, callback if error?
        return @soldiers.remove { recordId, uuid }, callback if data.fireOnce
        @soldiers.update { recordId, uuid }, callback

  sendMessage: ({data, timestamp}, callback) =>
    { uuid, token, nodeId, sendTo, transactionId } = data

    config = _.defaults {uuid, token}, @meshbluConfig
    config.timeout = @requestTimeout
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
    @queue.drain = callback
    _.delay @queue.kill, 1000

module.exports = Worker
