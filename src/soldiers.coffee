{ ObjectId } = require 'mongojs'
moment       = require 'moment'
debug        = require('debug')('nowmen-worker:soldiers')
overview     = require('debug')('nowmen-worker:soldiers')

class Soldiers
  constructor: ({ database }) ->
    @collection = database.collection 'soldiers'

  get: ({ recordId, uuid }, callback) =>
    unless recordId? or uuid?
      overview 'missing id'
      return callback()
    debug 'recordId', { recordId }
    @collection.findOne @_getQuery({ uuid, recordId }), { data: true }, (error, record) =>
      return callback error if error?
      overview 'found record', record if record?
      overview 'no record found' unless record?
      callback null, record?.data

  update: ({ recordId, uuid }, callback) =>
    unless recordId? or uuid?
      overview 'missing id'
      return callback()
    query  = @_getQuery({ uuid, recordId })
    update =
      $set:
        'metadata.lastSent': moment().unix()
      $inc:
        'metadata.totalSent': 1
    overview 'updating soldier', { query, update }
    @collection.update query, update, callback

  remove: ({ recordId, uuid }, callback) =>
    unless recordId? or uuid?
      overview 'missing id'
      return callback()
    overview 'removing soldier', { recordId, uuid }
    @collection.remove @_getQuery({ uuid, recordId }), callback

  _getQuery: ({ uuid, recordId }) =>
    return { uuid } if uuid?
    return { _id: new ObjectId(recordId) }

module.exports = Soldiers
