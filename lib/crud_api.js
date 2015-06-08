/*
 * CRUD Specification Write Operations
 * https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst
 */
DBCollection.prototype.bulkWrite = function() {
  print("bulkWrite called")
}

DBCollection.prototype.insertOne = function() {
  print("insertOne called")
}

DBCollection.prototype.insertMany = function(documents, options) {
  options = options || {};
  options.ordered = typeof options.ordered == 'booolean' ? options.ordered : true;

  // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
  var writeConcern = options.writeConcern ? options.writeConcern : this.getWriteConcern();
  if(writeConcern instanceof WriteConcern)
    writeConcern = writeConcern.toJSON();

  // Result
  var result = null;
  // Use bulk operation API already in the shell
  var bulk = options.ordered ? this.initializeOrderedBulkOp() : this.initializeUnorderedBulkOp();
  // Add all operations to the bulk operation
  documents.forEach(function(doc) {
    bulk.insert(doc);
  });

  try {
    // Execute bulk operation
    result = bulk.execute(writeConcern);
  } catch(err) {
    throw err;
  }

  // Return the result
  return result;
}

DBCollection.prototype.deleteOne = function() {
  print("deleteOne called")
}

DBCollection.prototype.deleteMany = function(filter, options) {  
  options = options || {};
  options.justOne = false;
  var result = this.remove(filter, options);
  return {deletedCount: result.nRemoved};
}

DBCollection.prototype.replaceOne = function() {
  print("replaceOne called")
}

DBCollection.prototype.updateOne = function() {
  print("updateOne called")
}

DBCollection.prototype.updateMany = function() {
  print("updateMany called")
}

/*
 * CRUD Specification FindAndModify Operations
 * https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst
 */
DBCollection.prototype.findOneAndDelete = function() {
  print("findOneAndDelete called")
}

DBCollection.prototype.findOneAndReplace = function() {
  print("findOneAndReplace called")
}

DBCollection.prototype.findOneAndUpdate = function() {
  print("findOneAndUpdate called")
}
