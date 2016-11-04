Worker        = require '../src/worker'
Redis         = require 'ioredis'
RedisNS       = require '@octoblu/redis-ns'
MeshbluConfig = require 'meshblu-config'
shmock        = require 'shmock'
mongojs       = require 'mongojs'
{ ObjectId }  = require 'mongojs'
enableDestroy = require 'server-destroy'

describe 'Worker', ->
  beforeEach (done) ->
    client = new Redis 'localhost', { dropBufferSupport: true }
    client.on 'ready', =>
      @client = new RedisNS 'test-now-man-worker', client
      @client.del 'work'
      done()

  beforeEach ->
    @meshblu = shmock 0xd00d
    enableDestroy @meshblu
    database = mongojs 'the-now-man-worker-test', ['soldiers']
    @collection = database.collection 'soldiers'

    queueName = 'work'
    queueTimeout = 1
    @sut = new Worker {
      disableSendTimestamp: true
      meshbluConfig:
        uuid: 'the-now-man-uuid'
        token: 'the-now-man-token'
        hostname: 'localhost'
        port: 0xd00d
        protocol: 'http'
      @client,
      database
      queueName,
      queueTimeout
    }

  beforeEach (done) ->
    @collection.drop (error) =>
      done()

  afterEach (done) ->
    @sut.stop done

  afterEach ->
    @meshblu.destroy()

  describe '->do', ->
    describe 'when a valid job queued', ->
      describe 'when a transactionId is passed', ->
        beforeEach (done) ->
          record =
            metadata:
              who: 'cares'
            data:
              nodeId: 'the-node-id'
              transactionId: 'the-transaction-id'
              uuid: 'the-interval-uuid'
              token: 'the-interval-token'
              sendTo: 'the-flow-uuid'
          @collection.insert record, (error, record) =>
            return done error if error?
            @recordId = record._id.toString()
            @client.lpush 'work', @recordId, done
            return # stupid promises

        beforeEach (done) ->
          intervalAuth = new Buffer('the-interval-uuid:the-interval-token').toString('base64')
          @sendMessage = @meshblu
            .post '/messages'
            .set 'Authorization', "Basic #{intervalAuth}"
            .send {
              devices: ['the-flow-uuid']
              payload:
                from: 'the-node-id'
                transactionId: 'the-transaction-id'
            }
            .reply 201

          @sut.do (error) =>
            done error

        it 'should send the message', ->
          @sendMessage.done()

        it 'should update the record', (done) ->
          @collection.findOne { _id: new ObjectId(@recordId) }, (error, record) =>
            return done error if error?
            expect(record.metadata.lastSent).to.exist
            expect(record.metadata.totalSent).to.equal 1
            done()

      describe 'when the record has been sent in the past', ->
        beforeEach (done) ->
          record =
            metadata:
              who: 'cares'
              lastSent: 'some-old-time'
              totalSent: 3
            data:
              nodeId: 'the-node-id'
              transactionId: 'the-transaction-id'
              uuid: 'the-interval-uuid'
              token: 'the-interval-token'
              sendTo: 'the-flow-uuid'
          @collection.insert record, (error, record) =>
            return done error if error?
            @recordId = record._id.toString()
            @client.lpush 'work', @recordId, done
            return # stupid promises

        beforeEach (done) ->
          intervalAuth = new Buffer('the-interval-uuid:the-interval-token').toString('base64')
          @sendMessage = @meshblu
            .post '/messages'
            .set 'Authorization', "Basic #{intervalAuth}"
            .send {
              devices: ['the-flow-uuid']
              payload:
                from: 'the-node-id'
                transactionId: 'the-transaction-id'
            }
            .reply 201

          @sut.do (error) =>
            done error

        it 'should send the message', ->
          @sendMessage.done()

        it 'should update the record', (done) ->
          @collection.findOne { _id: new ObjectId(@recordId) }, (error, record) =>
            return done error if error?
            expect(record.metadata.lastSent).to.not.equal 'some-old-time'
            expect(record.metadata.totalSent).to.equal 4
            done()

      describe 'when no transactionId is passed', ->
        beforeEach (done) ->
          record =
            metadata:
              who: 'cares'
            data:
              nodeId: 'the-node-id'
              uuid: 'the-interval-uuid'
              token: 'the-interval-token'
              sendTo: 'the-flow-uuid'
          @collection.insert record, (error, record) =>
            return done error if error?
            @recordId = record._id.toString()
            @client.lpush 'work', @recordId, done
            return # stupid promises

        beforeEach (done) ->
          intervalAuth = new Buffer('the-interval-uuid:the-interval-token').toString('base64')
          @sendMessage = @meshblu
            .post '/messages'
            .set 'Authorization', "Basic #{intervalAuth}"
            .send {
              devices: ['the-flow-uuid']
              payload:
                from: 'the-node-id'
                transactionId: 'the-node-id'
            }
            .reply 201

          @sut.do (error) =>
            done error

        it 'should send the message', ->
          @sendMessage.done()

        it 'should update the record', (done) ->
          @collection.findOne { _id: new ObjectId(@recordId) }, (error, record) =>
            return done error if error?
            expect(record.metadata.lastSent).to.exist
            expect(record.metadata.totalSent).to.equal 1
            done()

      describe 'when it is a fireOnce record', ->
        beforeEach (done) ->
          record =
            metadata:
              who: 'cares'
            data:
              nodeId: 'the-node-id'
              uuid: 'the-interval-uuid'
              token: 'the-interval-token'
              sendTo: 'the-flow-uuid'
              fireOnce: true
          @collection.insert record, (error, record) =>
            return done error if error?
            @recordId = record._id.toString()
            @client.lpush 'work', @recordId, done
            return # stupid promises

        beforeEach (done) ->
          intervalAuth = new Buffer('the-interval-uuid:the-interval-token').toString('base64')
          @sendMessage = @meshblu
            .post '/messages'
            .set 'Authorization', "Basic #{intervalAuth}"
            .send {
              devices: ['the-flow-uuid']
              payload:
                from: 'the-node-id'
                transactionId: 'the-node-id'
            }
            .reply 201

          @sut.do (error) =>
            done error

        it 'should send the message', ->
          @sendMessage.done()

        it 'should delete the record', (done) ->
          @collection.findOne { _id: new ObjectId(@recordId) }, (error, record) =>
            return done error if error?
            expect(record).to.not.exist
            done()

    describe 'when a deleted job queued', ->
      beforeEach (done) ->
        @client.lpush 'work', new ObjectId(), done
        return # stupid promises

      beforeEach (done) ->
        @sut.do (@error) => done()

      it 'should not blow up', ->
        expect(@error).to.not.exist
