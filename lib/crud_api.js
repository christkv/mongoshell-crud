/*
 * CRUD Specification Write Operations
 * https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst
 */
DBCollection.prototype.bulkWrite = function() {
  print("bulkWrite called")
}

DBCollection.prototype.insertOne = function(document, options) {
  options = options || {};
  if(document._id == null) document._id = new ObjectId();
  
  // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
  var writeConcern = options.writeConcern ? options.writeConcern : this.getWriteConcern();
  if(writeConcern instanceof WriteConcern)
    writeConcern = writeConcern.toJSON();

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};
  // Execute insert
  var r = this.insert(document, options);
  result.insertedId = document._id;
  // Return the result
  return result;
}

DBCollection.prototype.insertMany = function(documents, options) {
  options = options || {};
  options.ordered = typeof options.ordered == 'booolean' ? options.ordered : true;

  // Ensure all documents have an _id
  documents = documents.map(function(x) { 
    if(x._id == null) x._id = new ObjectId();
    return x;
  });

  // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
  var writeConcern = options.writeConcern ? options.writeConcern : this.getWriteConcern();
  if(writeConcern instanceof WriteConcern)
    writeConcern = writeConcern.toJSON();

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
    // Set all the created inserts
    result.insertedIds = documents.map(function(x) { return x._id; });
  } catch(err) {
    throw err;
  }

  // Return the result
  return result;
}

DBCollection.prototype.deleteOne = function(filter, options) {
  options = options || {};
  options.justOne = true;
  var result = this.remove(filter, options);
  return {deletedCount: result.nRemoved};
}

DBCollection.prototype.deleteMany = function(filter, options) {  
  options = options || {};
  options.justOne = false;
  var result = this.remove(filter, options);
  return {deletedCount: result.nRemoved};
}

DBCollection.prototype.replaceOne = function(filter, replacement, options) {
  options = options || {};
  options.multi = false;

  // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
  var writeConcern = options.writeConcern ? options.writeConcern : this.getWriteConcern();
  if(writeConcern instanceof WriteConcern)
    writeConcern = writeConcern.toJSON();

  // Setup the write concern
  if(writeConcern) options.writeConcern = writeConcern;

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};

  // Execute the upsert
  var r = this.update(filter, replacement, options)
  // print("######################################################################## 0")
  // printjson(r)
  result.matchedCount = r.nMatched;
  result.modifiedCount = r.nModified != null ? r.nModified : r.n;
  if(r.getUpsertedId() != null) result.upsertedId = r.getUpsertedId()._id;
  // print("######################################################################## 1")
  // printjson(result)
  return result;
}

DBCollection.prototype.updateOne = function() {
  print("updateOne called")
}

DBCollection.prototype.updateMany = function(filter, update, options) {
  options = options || {};
  options.multi = true;

  // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
  var writeConcern = options.writeConcern ? options.writeConcern : this.getWriteConcern();
  if(writeConcern instanceof WriteConcern)
    writeConcern = writeConcern.toJSON();

  // Setup the write concern
  if(writeConcern) options.writeConcern = writeConcern;

  // Result
  var result = {acknowledged: writeConcern && writeConcern.w == 0 ? false: true};

  // Execute the upsert
  var r = this.update(filter, update, options)
  // print("######################################################################## 0")
  // printjson(r)
  result.matchedCount = r.nMatched;
  result.modifiedCount = r.nModified != null ? r.nModified : r.n;
  if(r.getUpsertedId() != null) result.upsertedId = r.getUpsertedId()._id;
  // print("######################################################################## 1")
  // printjson(result)
  return result;
}

/*
 * CRUD Specification FindAndModify Operations
 * https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst
 */
DBCollection.prototype.findOneAndDelete = function(filter, options) {
  options = options || {};
  // Set up the command
  var cmd = {query: filter, remove: true};
  if(options.sort) cmd.sort = options.sort;
  if(options.projection) cmd.fields = options.projection;
  if(options.maxTimeMS) cmd.maxTimeMS = options.maxTimeMS;

  // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
  var writeConcern = options.writeConcern ? options.writeConcern : this.getWriteConcern();
  if(writeConcern instanceof WriteConcern)
    writeConcern = writeConcern.toJSON();

  // Setup the write concern
  if(writeConcern) cmd.writeConcern = writeConcern;

  // Execute findAndModify
  return this.findAndModify(cmd);
}

DBCollection.prototype.findOneAndReplace = function(filter, replacement, options) {
  options = options || {};
  // print("--------------------------------------------------------- 1")
  // printjson(options)
  // Set up the command
  var cmd = {query: filter, update: replacement};
  if(options.sort) cmd.sort = options.sort;
  if(options.projection) cmd.fields = options.projection;
  if(options.maxTimeMS) cmd.maxTimeMS = options.maxTimeMS;
  
  // Set flags
  cmd.upsert = typeof options.upsert == 'boolean' ? options.upsert : false;
  cmd.new = typeof options.returnDocument == 'boolean' ? options.returnDocument : false;

  // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
  var writeConcern = options.writeConcern ? options.writeConcern : this.getWriteConcern();
  if(writeConcern instanceof WriteConcern)
    writeConcern = writeConcern.toJSON();

  // Setup the write concern
  if(writeConcern) cmd.writeConcern = writeConcern;

  // print("--------------------------------------------------------- 1")
  // printjson(cmd)

  // Execute findAndModify
  return this.findAndModify(cmd);
}

DBCollection.prototype.findOneAndUpdate = function(filter, update, options) {
  options = options || {};
  // print("--------------------------------------------------------- 1")
  // printjson(options)
  // Set up the command
  var cmd = {query: filter, update: update};
  if(options.sort) cmd.sort = options.sort;
  if(options.projection) cmd.fields = options.projection;
  if(options.maxTimeMS) cmd.maxTimeMS = options.maxTimeMS;
  
  // Set flags
  cmd.upsert = typeof options.upsert == 'boolean' ? options.upsert : false;
  cmd.new = typeof options.returnDocument == 'boolean' ? options.returnDocument : false;

  // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
  var writeConcern = options.writeConcern ? options.writeConcern : this.getWriteConcern();
  if(writeConcern instanceof WriteConcern)
    writeConcern = writeConcern.toJSON();

  // Setup the write concern
  if(writeConcern) cmd.writeConcern = writeConcern;

  // print("--------------------------------------------------------- 1")
  // printjson(cmd)

  // Execute findAndModify
  return this.findAndModify(cmd);
}
