var shallowClone = function(object) {
  var r = {};
  for(var name in object) r[name] = object[name];
  return r;
}

var createWriteConcern = function(self, options) {
  // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
  var writeConcern = options.writeConcern ? options.writeConcern : self.getWriteConcern();
  if(writeConcern instanceof WriteConcern)
    writeConcern = writeConcern.toJSON();

  if(options.w != null || options.wtimeout != null || options.j != null || options.fsync != null) {
    writeConcern = {};
    if(options.w != null) writeConcern.w = options.w;
    if(options.wtimeout != null) writeConcern.wtimeout = options.wtimeout;
    if(options.j != null) writeConcern.j = options.j;
    if(options.fsync != null) writeConcern.fsync = options.fsync;
  }

  return writeConcern;
}

/**
 * Perform a bulkWrite operation without a fluent API
 *
 * Legal operation types are
 *
 *  { insertOne: { document: { a: 1 } } }
 *
 *  { updateOne: { filter: {a:2}, update: {$set: {a:2}}, upsert:true } }
 *
 *  { updateMany: { filter: {a:2}, update: {$set: {a:2}}, upsert:true } }
 *
 *  { deleteOne: { filter: {c:1} } }
 *
 *  { deleteMany: { filter: {c:1} } }
 *
 *  { replaceOne: { filter: {c:3}, replacement: {c:4}, upsert:true}}
 *
 * @method
 * @param {object[]} operations Bulk operations to perform.
 * @param {object} [options=null] Optional settings.
 * @param {(number|string)} [options.w=null] The write concern.
 * @param {number} [options.wtimeout=null] The write concern timeout.
 * @param {boolean} [options.j=false] Specify a journal write concern.
 * @return {object}
 */
DBCollection.prototype.bulkWrite = function(operations, options) {
  options = options || {};
  options = shallowClone(options);
  options.ordered = typeof options.ordered == 'boolean' ? options.ordered : true;

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};

  // Use bulk operation API already in the shell
  var bulk = options.ordered ? this.initializeOrderedBulkOp() : this.initializeUnorderedBulkOp();

  // Contains all inserted _ids
  var insertedIds = {};

  // Index of the operation
  var index = 0;

  // For each of the operations we need to add the op to the bulk
  operations.forEach(function(op) {
    if(op.insertOne) {
      if(!op.insertOne.document) throw new Error('insertOne bulkWrite operation expects the document field');
      if(op.insertOne.document._id == null) op.insertOne.document._id = new ObjectId();
      // Save the total insertedIds
      insertedIds[index] = op.insertOne.document._id;
      // Translate operation to bulk operation
      bulk.insert(op.insertOne.document);
    } else if(op.updateOne) {
      if(!op.updateOne.filter) throw new Error('updateOne bulkWrite operation expects the filter field');
      if(!op.updateOne.update) throw new Error('updateOne bulkWrite operation expects the update field');
      // Translate operation to bulk operation
      var operation = bulk.find(op.updateOne.filter);
      if(op.updateOne.upsert) operation = operation.upsert();
      operation.updateOne(op.updateOne.update)
    } else if(op.updateMany) {
      if(!op.updateMany.filter) throw new Error('updateMany bulkWrite operation expects the filter field');
      if(!op.updateMany.update) throw new Error('updateMany bulkWrite operation expects the update field');
      // Translate operation to bulk operation
      var operation = bulk.find(op.updateMany.filter);
      if(op.updateMany.upsert) operation = operation.upsert();
      operation.update(op.updateMany.update)
    } else if(op.replaceOne) {
      if(!op.replaceOne.filter) throw new Error('replaceOne bulkWrite operation expects the filter field');
      if(!op.replaceOne.replacement) throw new Error('replaceOne bulkWrite operation expects the replacement field');
      // Translate operation to bulk operation
      var operation = bulk.find(op.replaceOne.filter);
      if(op.replaceOne.upsert) operation = operation.upsert();
      operation.replaceOne(op.replaceOne.replacement)
    } else if(op.deleteOne) {
      if(!op.deleteOne.filter) throw new Error('deleteOne bulkWrite operation expects the filter field');
      // Translate operation to bulk operation
      bulk.find(op.deleteOne.filter).removeOne();
    } else if(op.deleteMany) {
      if(!op.deleteMany.filter) throw new Error('deleteMany bulkWrite operation expects the filter field');
      // Translate operation to bulk operation
      bulk.find(op.deleteMany.filter).remove();
    }

    index = index + 1;
  });

  try {
    // Execute bulk operation
    var r = bulk.execute(writeConcern);
    if(!result.acknowledged) return result;
    result.deletedCount = r.nRemoved;
    result.insertedCount = r.nInserted;
    result.matchedCount = r.nMatched;
    result.upsertedCount = r.nUpserted;
    result.insertedIds = insertedIds;
    result.upsertedIds = {};

    // Iterate over all the upserts
    var upserts = r.getUpsertedIds();
    upserts.forEach(function(x) {
      result.upsertedIds[x.index] = x._id;
    });

    // // Set all the created inserts
    // result.insertedIds = documents.map(function(x) { return x._id; });
  } catch(err) {
    throw err;
  }
 
  // Return the result
  return result;
}

/**
 * Inserts a single document into MongoDB.
 * @method
 * @param {object} doc Document to insert.
 * @param {object} [options=null] Optional settings.
 * @param {(number|string)} [options.w=null] The write concern.
 * @param {number} [options.wtimeout=null] The write concern timeout.
 * @param {boolean} [options.j=false] Specify a journal write concern.
 * @return {object}
 */
DBCollection.prototype.insertOne = function(document, options) {
  options = options || {};
  options = shallowClone(options);
  if(document._id == null) document._id = new ObjectId();
  
  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};

  // Use bulk operation API already in the shell
  var bulk = this.initializeOrderedBulkOp();
  bulk.insert(document);
  
  // Execute insert
  var r = bulk.execute(writeConcern);
  if(!result.acknowledged) return result;
  result.insertedId = document._id;
  
  // Return the result
  return result;
}

/**
 * Inserts an array of documents into MongoDB.
 * @method
 * @param {object[]} docs Documents to insert.
 * @param {object} [options=null] Optional settings.
 * @param {(number|string)} [options.w=null] The write concern.
 * @param {number} [options.wtimeout=null] The write concern timeout.
 * @param {boolean} [options.j=false] Specify a journal write concern.
 * @param {boolean} [options.ordered=true] Execute inserts in ordered or unordered fashion.
 * @return {object}
 */
DBCollection.prototype.insertMany = function(documents, options) {
  options = options || {};
  options = shallowClone(options);
  options.ordered = typeof options.ordered == 'boolean' ? options.ordered : true;

  // Ensure all documents have an _id
  documents = documents.map(function(x) { 
    if(x._id == null) x._id = new ObjectId();
    return x;
  });

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};
  
  // Use bulk operation API already in the shell
  var bulk = options.ordered ? this.initializeOrderedBulkOp() : this.initializeUnorderedBulkOp();
  
  // Add all operations to the bulk operation
  documents.forEach(function(doc) {
    bulk.insert(doc);
  });

  try {
    // Execute bulk operation
    var r = bulk.execute(writeConcern);
    if(!result.acknowledged) return result;
    // Set all the created inserts
    result.insertedIds = documents.map(function(x) { return x._id; });
  } catch(err) {
    throw err;
  }

  // Return the result
  return result;
}

/**
 * Delete a document on MongoDB
 * @method
 * @param {object} filter The Filter used to select the document to remove
 * @param {object} [options=null] Optional settings.
 * @param {(number|string)} [options.w=null] The write concern.
 * @param {number} [options.wtimeout=null] The write concern timeout.
 * @param {boolean} [options.j=false] Specify a journal write concern.
 * @return {object}
 */
DBCollection.prototype.deleteOne = function(filter, options) {
  options = options || {};
  options = shallowClone(options);

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};

  // Use bulk operation API already in the shell
  var bulk = this.initializeOrderedBulkOp();

  // Add the deleteOne operation
  bulk.find(filter).removeOne();
  
  // Remove the documents
  var r = bulk.execute(writeConcern);
  if(!result.acknowledged) return result;
  result.deletedCount = r.nRemoved;
  return result;
}

/**
 * Delete multiple documents on MongoDB
 * @method
 * @param {object} filter The Filter used to select the documents to remove
 * @param {object} [options=null] Optional settings.
 * @param {(number|string)} [options.w=null] The write concern.
 * @param {number} [options.wtimeout=null] The write concern timeout.
 * @param {boolean} [options.j=false] Specify a journal write concern.
 * @return {object}
 */
DBCollection.prototype.deleteMany = function(filter, options) {  
  options = options || {};
  options = shallowClone(options);

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};

  // Use bulk operation API already in the shell
  var bulk = this.initializeOrderedBulkOp();

  // Add the deleteOne operation
  bulk.find(filter).remove();
  // Remove the documents
  var r = bulk.execute(writeConcern);
  if(!result.acknowledged) return result;
  result.deletedCount = r.nRemoved;
  return result;
}

/**
 * Replace a document on MongoDB
 * @method
 * @param {object} filter The Filter used to select the document to update
 * @param {object} doc The Document that replaces the matching document
 * @param {object} [options=null] Optional settings.
 * @param {boolean} [options.upsert=false] Update operation is an upsert.
 * @param {(number|string)} [options.w=null] The write concern.
 * @param {number} [options.wtimeout=null] The write concern timeout.
 * @param {boolean} [options.j=false] Specify a journal write concern.
 * @return {object}
 */
DBCollection.prototype.replaceOne = function(filter, replacement, options) {
  options = options || {};
  options = shallowClone(options);

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};

  // Use bulk operation API already in the shell
  var bulk = this.initializeOrderedBulkOp();

  // Add the deleteOne operation
  var op = bulk.find(filter);
  if(options.upsert) op = op.upsert();
  op.replaceOne(replacement);

  // Remove the documents
  var r = bulk.execute(writeConcern);
  if(!result.acknowledged) return result;
  result.matchedCount = r.nMatched;
  result.modifiedCount = r.nModified != null ? r.nModified : r.n;
  if(r.getUpsertedIdAt(0) != null) result.upsertedId = r.getUpsertedIdAt(0)._id;
  return result;
}

/**
 * Update a single document on MongoDB
 * @method
 * @param {object} filter The Filter used to select the document to update
 * @param {object} update The update operations to be applied to the document
 * @param {object} [options=null] Optional settings.
 * @param {boolean} [options.upsert=false] Update operation is an upsert.
 * @param {(number|string)} [options.w=null] The write concern.
 * @param {number} [options.wtimeout=null] The write concern timeout.
 * @param {boolean} [options.j=false] Specify a journal write concern.
 * @return {object}
 */
DBCollection.prototype.updateOne = function(filter, update, options) {
  options = options || {};
  options = shallowClone(options);

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};

  // Use bulk operation API already in the shell
  var bulk = this.initializeOrderedBulkOp();
  
  // Add the updateOne operation
  var op = bulk.find(filter);
  if(options.upsert) op = op.upsert();
  op.updateOne(update);

  // Remove the documents
  var r = bulk.execute(writeConcern);
  if(!result.acknowledged) return result;
  result.matchedCount = r.nMatched;
  result.modifiedCount = r.nModified != null ? r.nModified : r.n;
  if(r.getUpsertedIdAt(0) != null) result.upsertedId = r.getUpsertedIdAt(0)._id;
  return result;
}

/**
 * Update multiple documents on MongoDB
 * @method
 * @param {object} filter The Filter used to select the document to update
 * @param {object} update The update operations to be applied to the document
 * @param {object} [options=null] Optional settings.
 * @param {boolean} [options.upsert=false] Update operation is an upsert.
 * @param {(number|string)} [options.w=null] The write concern.
 * @param {number} [options.wtimeout=null] The write concern timeout.
 * @param {boolean} [options.j=false] Specify a journal write concern.
 * @return {object}
 */
DBCollection.prototype.updateMany = function(filter, update, options) {
  options = options || {};
  options = shallowClone(options);

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};

  // Use bulk operation API already in the shell
  var bulk = this.initializeOrderedBulkOp();

  // Add the updateOne operation
  var op = bulk.find(filter);
  if(options.upsert) op = op.upsert();
  op.update(update);

  // Remove the documents
  var r = bulk.execute(writeConcern);
  if(!result.acknowledged) return result;
  result.matchedCount = r.nMatched;
  result.modifiedCount = r.nModified != null ? r.nModified : r.n;
  if(r.getUpsertedIdAt(0) != null) result.upsertedId = r.getUpsertedIdAt(0)._id;
  return result;
}

/**
 * Find a document and delete it in one atomic operation, requires a write lock for the duration of the operation.
 *
 * @method
 * @param {object} filter Document selection filter.
 * @param {object} [options=null] Optional settings.
 * @param {object} [options.projection=null] Limits the fields to return for all matching documents.
 * @param {object} [options.sort=null] Determines which document the operation modifies if the query selects multiple documents.
 * @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
 * @return {object}
 */
DBCollection.prototype.findOneAndDelete = function(filter, options) {
  options = options || {};
  options = shallowClone(options);
  // Set up the command
  var cmd = {query: filter, remove: true};
  if(options.sort) cmd.sort = options.sort;
  if(options.projection) cmd.fields = options.projection;
  if(options.maxTimeMS) cmd.maxTimeMS = options.maxTimeMS;

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Setup the write concern
  if(writeConcern) cmd.writeConcern = writeConcern;

  // Execute findAndModify
  return this.findAndModify(cmd);
}

/**
 * Find a document and replace it in one atomic operation, requires a write lock for the duration of the operation.
 *
 * @method
 * @param {object} filter Document selection filter.
 * @param {object} replacement Document replacing the matching document.
 * @param {object} [options=null] Optional settings.
 * @param {object} [options.projection=null] Limits the fields to return for all matching documents.
 * @param {object} [options.sort=null] Determines which document the operation modifies if the query selects multiple documents.
 * @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
 * @param {boolean} [options.upsert=false] Upsert the document if it does not exist.
 * @param {boolean} [options.returnDocument=false] When true, returns the updated document rather than the original. The default is false.
 * @return {object}
 */
DBCollection.prototype.findOneAndReplace = function(filter, replacement, options) {
  options = options || {};
  options = shallowClone(options);
  // Set up the command
  var cmd = {query: filter, update: replacement};
  if(options.sort) cmd.sort = options.sort;
  if(options.projection) cmd.fields = options.projection;
  if(options.maxTimeMS) cmd.maxTimeMS = options.maxTimeMS;
  
  // Set flags
  cmd.upsert = typeof options.upsert == 'boolean' ? options.upsert : false;
  cmd.new = typeof options.returnDocument == 'boolean' ? options.returnDocument : false;

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Setup the write concern
  if(writeConcern) cmd.writeConcern = writeConcern;

  // Execute findAndModify
  return this.findAndModify(cmd);
}

/**
 * Find a document and update it in one atomic operation, requires a write lock for the duration of the operation.
 *
 * @method
 * @param {object} filter Document selection filter.
 * @param {object} update Update operations to be performed on the document
 * @param {object} [options=null] Optional settings.
 * @param {object} [options.projection=null] Limits the fields to return for all matching documents.
 * @param {object} [options.sort=null] Determines which document the operation modifies if the query selects multiple documents.
 * @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
 * @param {boolean} [options.upsert=false] Upsert the document if it does not exist.
 * @param {boolean} [options.returnDocument=false] When true, returns the updated document rather than the original. The default is false.
 * @return {object}
 */
DBCollection.prototype.findOneAndUpdate = function(filter, update, options) {
  options = options || {};
  options = shallowClone(options);
  // Set up the command
  var cmd = {query: filter, update: update};
  if(options.sort) cmd.sort = options.sort;
  if(options.projection) cmd.fields = options.projection;
  if(options.maxTimeMS) cmd.maxTimeMS = options.maxTimeMS;
  
  // Set flags
  cmd.upsert = typeof options.upsert == 'boolean' ? options.upsert : false;
  cmd.new = typeof options.returnDocument == 'boolean' ? options.returnDocument : false;

  // Get the write concern
  var writeConcern = createWriteConcern(this, options);

  // Setup the write concern
  if(writeConcern) cmd.writeConcern = writeConcern;

  // Execute findAndModify
  return this.findAndModify(cmd);
}

//
// CRUD specification read methods
//

/**
 * Count number of matching documents in the db to a query.
 * @method
 * @param {object} query The query for the count.
 * @param {object} [options=null] Optional settings.
 * @param {boolean} [options.limit=null] The limit of documents to count.
 * @param {boolean} [options.skip=null] The number of documents to skip for the count.
 * @param {string} [options.hint=null] An index name hint for the query.
 * @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
 * @return {number}
 */
DBCollection.prototype.count = function(query, options) {
  options = options || {};
  options = shallowClone(options);

  // Set parameters
  var skip = typeof options.skip == 'number' ? options.skip : null;
  var limit = typeof options.limit == 'number' ? options.limit : null;
  var hint = options.hint;

  // Execute using command if we have passed in skip/limit or hint
  if(skip != null || limit != null || hint != null) {
    // Final query
    var cmd = {
        'count': this.getName(), 'query': query
      , 'fields': null
    };

    // Add limit and skip if defined
    if(typeof skip == 'number') cmd.skip = skip;
    if(typeof limit == 'number') cmd.limit = limit;
    if(hint) options.hint = hint;
    if(options.maxTimeMS) cmd.maxTimeMS = options.maxTimeMS;

    // Run the command and return the result
    return this.runCommand(cmd).n;
  }

  // Return the result of the find
  return this.find(query).count();
}





