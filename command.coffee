_              = require 'lodash'
chalk          = require 'chalk'
dashdash       = require 'dashdash'
Redis          = require 'ioredis'
RedisNS        = require '@octoblu/redis-ns'
Worker         = require './src/worker'
SigtermHandler = require 'sigterm-handler'

packageJSON    = require './package.json'

OPTIONS = [
  {
    names: ['redis-uri', 'r']
    type: 'string'
    env: 'REDIS_URI'
    help: 'Redis URI'
  },
  {
    names: ['redis-namespace', 'n']
    type: 'string'
    env: 'REDIS_NAMESPACE'
    help: 'Redis namespace for redis-ns'
  },
  {
    names: ['queue-name', 'q']
    type: 'string'
    env: 'QUEUE_NAME'
    help: 'Name of Redis work queue'
  },
  {
    names: ['queue-timeout', 't']
    type: 'positiveInteger'
    env: 'QUEUE_TIMEOUT'
    default: 30
    help: 'BRPOP timeout (in seconds)'
  },
  {
    names: ['help', 'h']
    type: 'bool'
    help: 'Print this help and exit.'
  },
  {
    names: ['version', 'v']
    type: 'bool'
    help: 'Print the version and exit.'
  }
]

class Command
  constructor: ->
    process.on 'uncaughtException', @die
    @parser = dashdash.createParser({options: OPTIONS})
    {@redis_uri, @redis_namespace, @queue_timeout, @queue_name} = @parseOptions()

  printHelp: =>
    options = { includeEnv: true, includeDefaults:true }
    console.log "usage: now-man-worker [OPTIONS]\noptions:\n#{@parser.help(options)}"

  parseOptions: =>
    options = @parser.parse(process.argv)

    if options.help
      @printHelp()
      process.exit 0

    if options.version
      console.log packageJSON.version
      process.exit 0

    unless options.redis_uri? && options.redis_namespace? && options.queue_name? && options.queue_timeout?
      @printHelp()
      console.error chalk.red 'Missing required parameter --redis-uri, -r, or env: REDIS_URI' unless options.redis_uri?
      console.error chalk.red 'Missing required parameter --redis-namespace, -n, or env: REDIS_NAMESPACE' unless options.redis_namespace?
      console.error chalk.red 'Missing required parameter --queue-timeout, -t, or env: QUEUE_TIMEOUT' unless options.queue_timeout?
      console.error chalk.red 'Missing required parameter --queue-name, -u, or env: QUEUE_NAME' unless options.queue_name?
      process.exit 1

    return options

  run: =>
    @getWorkerClient (error, client) =>
      return @die error if error?

      worker = new Worker { client, queueName: @queue_name, queueTimeout: @queue_timeout }
      worker.run @die

      sigtermHandler = new SigtermHandler { events: ['SIGINT', 'SIGTERM']}
      sigtermHandler.register worker.stop

  getWorkerClient: (callback) =>
    @getRedisClient @redis_uri, (error, client) =>
      return callback error if error?
      clientNS  = new RedisNS @redis_namespace, client
      callback null, clientNS

  getRedisClient: (redisUri, callback) =>
    callback = _.once callback
    client = new Redis redisUri, dropBufferSupport: true
    client.once 'ready', =>
      client.on 'error', @die
      callback null, client

    client.once 'error', callback

  die: (error) =>
    return process.exit(0) unless error?
    console.error 'ERROR'
    console.error error.stack
    process.exit 1

module.exports = Command
