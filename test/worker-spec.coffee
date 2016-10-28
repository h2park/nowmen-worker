Worker  = require '../src/worker'
Redis   = require 'ioredis'
RedisNS = require '@octoblu/redis-ns'
MeshbluConfig = require 'meshblu-config'
MeshbluHttp   = require 'meshblu-http'

describe 'Worker', ->
  beforeEach (done) ->
    client = new Redis 'localhost', dropBufferSupport: true
    client.on 'ready', =>
      @client = new RedisNS 'test-worker', client
      done()

  beforeEach ->
    queueName = 'work'
    queueTimeout = 1
    @sut = new Worker {
      MeshbluHttp,
      meshbluConfig: new MeshbluConfig().toJSON(),
      @client,
      queueName,
      queueTimeout
    }

  afterEach (done) ->
    @sut.stop done

  describe '->do', ->
    beforeEach (done) ->
      data = JSON.stringify foo: 'bar'
      @client.lpush 'work', data, done
      return # stupid promises

    beforeEach (done) ->
      @sut.do (error, @data) =>
        done error

    it 'should call the callback with data', ->
      expect(@data).to.deep.equal foo: 'bar'
