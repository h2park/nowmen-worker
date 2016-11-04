{ ObjectId } = require 'mongojs'
moment       = require 'moment'
debug        = require('debug')('now-man-worker:soldiers')

class Soldiers
  constructor: ({ database }) ->
    @collection = database.collection 'soldiers'

  get: ({ recordId }, callback) =>
    unless recordId?
      debug 'missing recordId'
      return callback()
    debug 'recordId', { recordId }
    @collection.findOne { _id: new ObjectId(recordId) }, { data: true }, (error, record) =>
      return callback error if error?
      debug 'found record', record if record?
      debug 'no record found' unless record?
      callback null, record?.data

  update: ({ recordId }, callback) =>
    unless recordId?
      debug 'missing recordId'
      return callback()
    query  = { _id: new ObjectId(recordId) }
    update =
      $set:
        'metadata.lastSent': moment().unix()
      $inc:
        'metadata.totalSent': 1
    debug 'updating soldier', { query, update }
    @collection.update query, update, callback

  remove: ({ recordId }, callback) =>
    unless recordId?
      debug 'missing recordId'
      return callback()
    debug 'removing soldier', { recordId }
    @collection.remove { _id: new ObjectId(recordId) }, callback

module.exports = Soldiers
