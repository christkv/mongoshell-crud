DBCollection.prototype._createWriteConcern = function(options) {
    // If writeConcern set, use it, else get from collection (which will inherit from db/mongo)
    var writeConcerns = options.writeConcern || this.getWriteConcern();
    var writeConcernOptions = ['w', 'wtimeout', 'j', 'fsync'];

    if (writeConcerns instanceof WriteConcern) {
        writeConcerns = writeConcerns.toJSON();
    }

    // Only merge in write concern options if at least one is specified in options
    if (options.w != null
        || options.wtimeout != null
        || options.j != null
        || options.fsync != null) {
        writeConcerns = {};

        writeConcernOptions.forEach(function(wc) {
            if (options[wc] != null) {
                writeConcerns[wc] = options[wc];
            }
        });
    }

    return writeConcerns;
}

/**
 * @return {Object} a new document with an _id: ObjectId if _id is not present.
 *     Otherwise, returns the same object passed.
 */
DBCollection.prototype.addIdIfNeeded = function(obj) {
    if ( typeof( obj._id ) == "undefined" && ! Array.isArray( obj ) ){
        var tmp = obj; // don't want to modify input
        obj = {_id: new ObjectId()};

        for (var key in tmp){
            obj[key] = tmp[key];
        }
    }

    return obj;
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
    var opts = {};
    Object.extend(opts, options);
    opts.ordered = (typeof opts.ordered == 'boolean') ? opts.ordered : true;

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {acknowledged: (writeConcern && writeConcern.w == 0) ? false: true};

    // Use bulk operation API already in the shell
    var bulkOp = opts.ordered
        ? this.initializeOrderedBulkOp()
        : this.initializeUnorderedBulkOp();

    // Contains all inserted _ids
    var insertedIds = {};

    // For each of the operations we need to add the op to the bulk
    operations.forEach(function(op, index) {
        if(op.insertOne) {
            if(!op.insertOne.document) {
                throw new Error('insertOne bulkWrite operation expects the document field');
            }

            // Add _id ObjectId if needed
            op.insertOne.document = this.addIdIfNeeded(op.insertOne.document);
            // InsertedIds is a map of [originalInsertOrderIndex] = document._id
            insertedIds[index] = op.insertOne.document._id;
            // Translate operation to bulk operation
            bulkOp.insert(op.insertOne.document);
        } else if(op.updateOne) {
            if(!op.updateOne.filter) {
                throw new Error('updateOne bulkWrite operation expects the filter field');
            }

            if(!op.updateOne.update) {
                throw new Error('updateOne bulkWrite operation expects the update field');
            }

            // Translate operation to bulk operation
            var operation = bulkOp.find(op.updateOne.filter);
            if(op.updateOne.upsert) {
                operation = operation.upsert();
            }

            operation.updateOne(op.updateOne.update)
        } else if(op.updateMany) {
            if(!op.updateMany.filter) {
                throw new Error('updateMany bulkWrite operation expects the filter field');
            }

            if(!op.updateMany.update) {
                throw new Error('updateMany bulkWrite operation expects the update field');
            }

            // Translate operation to bulk operation
            var operation = bulkOp.find(op.updateMany.filter);
            if(op.updateMany.upsert) {
                operation = operation.upsert();
            }

            operation.update(op.updateMany.update)
        } else if(op.replaceOne) {
            if(!op.replaceOne.filter) {
                throw new Error('replaceOne bulkWrite operation expects the filter field');
            }

            if(!op.replaceOne.replacement) {
                throw new Error('replaceOne bulkWrite operation expects the replacement field');
            }

            // Translate operation to bulkOp operation
            var operation = bulkOp.find(op.replaceOne.filter);
            if(op.replaceOne.upsert) {
                operation = operation.upsert();
            }

            operation.replaceOne(op.replaceOne.replacement)
        } else if(op.deleteOne) {
            if(!op.deleteOne.filter) {
                throw new Error('deleteOne bulkWrite operation expects the filter field');
            }

            // Translate operation to bulkOp operation
            bulkOp.find(op.deleteOne.filter).removeOne();
        } else if(op.deleteMany) {
            if(!op.deleteMany.filter) {
                throw new Error('deleteMany bulkWrite operation expects the filter field');
            }

            // Translate operation to bulkOp operation
            bulkOp.find(op.deleteMany.filter).remove();
        }
    }, this);

    // Execute bulkOp operation
    var response = bulkOp.execute(writeConcern);
    if(!result.acknowledged) {
        return result;
    }

    result.deletedCount = response.nRemoved;
    result.insertedCount = response.nInserted;
    result.matchedCount = response.nMatched;
    result.upsertedCount = response.nUpserted;
    result.insertedIds = insertedIds;
    result.upsertedIds = {};

    // Iterate over all the upserts
    var upserts = response.getUpsertedIds();
    upserts.forEach(function(x) {
        result.upsertedIds[x.index] = x._id;
    });

    // Return the result
    return result;
}

/**
* Inserts a single document into MongoDB.
*
* @method
* @param {object} doc Document to insert.
* @param {object} [options=null] Optional settings.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.insertOne = function(document, options) {
    var opts = Object.extend({}, options || {});

    // Add _id ObjectId if needed
    document = this.addIdIfNeeded(document);

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {acknowledged: (writeConcern && writeConcern.w == 0) ? false: true};

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();
    bulk.insert(document);

    try {
        // Execute insert
        bulk.execute(writeConcern);
    } catch (err) {
        if(err.hasWriteErrors && err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }
    }

    if (!result.acknowledged) {
        return result;
    }

    // Set the inserted id
    result.insertedId = document._id;

    // Return the result
    return result;
}

/**
* Inserts an array of documents into MongoDB.
*
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
    var self = this;
    var opts = Object.extend({}, options || {});
    opts.ordered = (typeof opts.ordered == 'boolean') ? opts.ordered : true;

    // Ensure all documents have an _id
    documents = documents.map(function(x) {
        return this.addIdIfNeeded(x);
    }, this);

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {acknowledged: (writeConcern && writeConcern.w == 0) ? false: true};

    // Use bulk operation API already in the shell
    var bulk = opts.ordered
        ? this.initializeOrderedBulkOp()
        : this.initializeUnorderedBulkOp();

    // Add all operations to the bulk operation
    documents.forEach(function(doc) {
        bulk.insert(doc);
    });

    // Execute bulk operation
    bulk.execute(writeConcern);
    if (!result.acknowledged) {
        return result;
    }

    // Set all the created inserts
    result.insertedIds = documents.map(function(x) {
      return x._id;
    });

    // Return the result
    return result;
}

/**
* Delete a document on MongoDB
*
* @method
* @param {object} filter The filter used to select the document to remove
* @param {object} [options=null] Optional settings.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.deleteOne = function(filter, options) {
    var opts = Object.extend({}, options || {});

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {acknowledged: (writeConcern && writeConcern.w == 0) ? false: true};

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();

    // Add the deleteOne operation
    bulk.find(filter).removeOne();

    try {
        // Remove the first document that matches the selector
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if(err.hasWriteErrors && err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }
    }

    if (!result.acknowledged) {
        return result;
    }

    result.deletedCount = r.nRemoved;
    return result;
}

/**
* Delete multiple documents on MongoDB
*
* @method
* @param {object} filter The Filter used to select the documents to remove
* @param {object} [options=null] Optional settings.
* @param {(number|string)} [options.w=null] The write concern.
* @param {number} [options.wtimeout=null] The write concern timeout.
* @param {boolean} [options.j=false] Specify a journal write concern.
* @return {object}
*/
DBCollection.prototype.deleteMany = function(filter, options) {
    var opts = Object.extend({}, options || {});

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {acknowledged: (writeConcern && writeConcern.w == 0) ? false: true};

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();

    // Add the deleteOne operation
    bulk.find(filter).remove();

    try {
        // Remove all documents that matche the selector
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if(err.hasWriteErrors && err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }
    }

    if (!result.acknowledged) {
        return result;
    }

    result.deletedCount = r.nRemoved;
    return result;
}

/**
* Replace a document on MongoDB
*
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
    var opts = Object.extend({}, options || {});

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {acknowledged: (writeConcern && writeConcern.w == 0) ? false: true };

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();

    // Add the deleteOne operation
    var op = bulk.find(filter);
    if (opts.upsert) {
        op = op.upsert();
    }

    op.replaceOne(replacement);

    try {
        // Replace the document
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if(err.hasWriteErrors && err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }
    }

    if (!result.acknowledged) {
        return result;
    }

    result.matchedCount = r.nMatched;
    result.modifiedCount = (r.nModified != null) ? r.nModified : r.n;

    if (r.getUpsertedIds().length > 0) {
        result.upsertedId = r.getUpsertedIdAt(0)._id;
    }

    return result;
}

/**
* Update a single document on MongoDB
*
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
    var opts = Object.extend({}, options || {});

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {acknowledged: (writeConcern && writeConcern.w == 0) ? false: true};

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();

    // Add the updateOne operation
    var op = bulk.find(filter);
    if (opts.upsert) {
        op = op.upsert();
    }

    op.updateOne(update);

    try {
        // Update the first document that matches the selector
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if(err.hasWriteErrors && err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }
    }

    if (!result.acknowledged) {
        return result;
    }

    result.matchedCount = r.nMatched;
    result.modifiedCount = (r.nModified != null) ? r.nModified : r.n;

    if (r.getUpsertedIds().length > 0) {
        result.upsertedId = r.getUpsertedIdAt(0)._id
    }

    return result;
}

/**
* Update multiple documents on MongoDB
*
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
    var opts = Object.extend({}, options || {});

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Result
    var result = {acknowledged: (writeConcern && writeConcern.w == 0) ? false: true};

    // Use bulk operation API already in the shell
    var bulk = this.initializeOrderedBulkOp();

    // Add the updateMany operation
    var op = bulk.find(filter);
    if (opts.upsert) {
        op = op.upsert();
    }

    op.update(update);

    try {
        // Update all documents that match the selector
        var r = bulk.execute(writeConcern);
    } catch (err) {
        if(err.hasWriteErrors && err.hasWriteErrors()) {
            throw err.getWriteErrorAt(0);
        }
    }

    if (!result.acknowledged) {
        return result;
    }

    result.matchedCount = r.nMatched;
    result.modifiedCount = (r.nModified != null) ? r.nModified : r.n;

    if (r.getUpsertedIds().length > 0) {
        result.upsertedId = r.getUpsertedIdAt(0)._id
    }

    return result;
}

/**
* Find a document and delete it in one atomic operation,
* requires a write lock for the duration of the operation.
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
    var opts = Object.extend({}, options || {});
    // Set up the command
    var cmd = {query: filter, remove: true};

    if (opts.sort) {
        cmd.sort = opts.sort;
    }

    if (opts.projection) {
        cmd.fields = opts.projection;
    }

    if (opts.maxTimeMS) {
        cmd.maxTimeMS = opts.maxTimeMS;
    }

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Setup the write concern
    if (writeConcern) {
        cmd.writeConcern = writeConcern;
    }

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
    var opts = Object.extend({}, options || {});
    // Set up the command
    var cmd = {query: filter, update: replacement};
    if (opts.sort) {
        cmd.sort = opts.sort;
    }

    if (opts.projection) {
        cmd.fields = opts.projection;
    }

    if (opts.maxTimeMS) {
        cmd.maxTimeMS = opts.maxTimeMS;
    }

    // Set flags
    cmd.upsert = (typeof opts.upsert == 'boolean') ? opts.upsert : false;
    cmd.new = (typeof opts.returnDocument == 'boolean') ? opts.returnDocument : false;

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Setup the write concern
    if (writeConcern) {
        cmd.writeConcern = writeConcern;
    }

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
    var opts = Object.extend({}, options || {});

    // Set up the command
    var cmd = {query: filter, update: update};
    if (opts.sort) {
        cmd.sort = opts.sort;
    }

    if (opts.projection) {
        cmd.fields = opts.projection;
    }

    if (opts.maxTimeMS) {
        cmd.maxTimeMS = opts.maxTimeMS;
    }

    // Set flags
    cmd.upsert = (typeof opts.upsert == 'boolean') ? opts.upsert : false;
    cmd.new = (typeof opts.returnDocument == 'boolean') ? opts.returnDocument : false;

    // Get the write concern
    var writeConcern = this._createWriteConcern(opts);

    // Setup the write concern
    if (writeConcern) {
        cmd.writeConcern = writeConcern;
    }

    // Execute findAndModify
    return this.findAndModify(cmd);
}

//
// CRUD specification read methods
//

/**
* Count number of matching documents in the db to a query.
*
* @method
* @param {object} query The query for the count.
* @param {object} [options=null] Optional settings.
* @param {number} [options.limit=null] The limit of documents to count.
* @param {number} [options.skip=null] The number of documents to skip for the count.
* @param {string|object} [options.hint=null] An index name hint or specification for the query.
* @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
* @return {number}
*/
DBCollection.prototype.count = function(query, options) {
    var opts = Object.extend({}, options || {});

    // Set parameters
    var skip = (typeof opts.skip == 'number') ? opts.skip : null;
    var limit = (typeof opts.limit == 'number') ? opts.limit : null;
    var hint = opts.hint;

    // Execute using command if we have passed in skip/limit or hint
    if (skip != null || limit != null || hint != null) {
        // Final query
        var cmd = {
            'count': this.getName(),
            'query': query
        };

        // Add limit and skip if defined
        if (typeof skip == 'number' && skip >= 0) {
            cmd.skip = skip;
        }

        if (typeof limit == 'number' && limit >= 0) {
            cmd.limit = limit;
        }

        if (hint) {
            opts.hint = hint;
        }

        if (opts.maxTimeMS) {
            cmd.maxTimeMS = opts.maxTimeMS;
        }

        // Run the command and return the result
        var response = this.runReadCommand(cmd);
        if (response.ok == 0) {
            throw new Error("count failed: " + tojson(response));
        }

        return response.n;
    }

    // Return the result of the find
    return this.find(query).count();
}

/**
* The distinct command returns returns a list of distinct values for the given key across a collection.
*
* @method
* @param {string} key Field of the document to find distinct values for.
* @param {object} query The query for filtering the set of documents to which we apply the distinct filter.
* @param {object} [options=null] Optional settings.
* @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
* @return {object}
*/
DBCollection.prototype.distinct = function(keyString, query, options){
    var opts = Object.extend({}, options || {});
    var keyStringType = typeof keyString;
    var queryType = typeof query;

    if (keyStringType != "string") {
        throw new Error("The first argument to the distinct command must be a string but was a " + keyStringType);
    }

    if (query != null && queryType != "object") {
        throw new Error("The query argument to the distinct command must be a document but was a " + queryType);
    }

    // Distinct command
    var cmd = {
        distinct : this.getName(),
        key : keyString,
        query : query || {}
    };

    // Set maxTimeMS if provided
    if (opts.maxTimeMS) {
        cmd.maxTimeMS = opts.maxTimeMS;
    }

    // Execute distinct command
    var res = this.runReadCommand(cmd);
    if (!res.ok) {
        throw new Error("distinct failed: " + tojson(res));
    }

    return res.values;
}

//
// CRUD specification find cursor extension
//

/**
* Get partial results from a mongos if some shards are down (instead of throwing an error).
*
* @method
* @see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query
* @return {DBQuery}
*/
DBQuery.prototype.allowPartialResults = function() {
    this._checkModify();
    this.addOption(DBQuery.Option.partial);
    return this;
}

/**
* The server normally times out idle cursors after an inactivity period (10 minutes)
* to prevent excess memory use. Set this option to prevent that.
*
* @method
* @see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query
* @return {DBQuery}
*/
DBQuery.prototype.noCursorTimeout = function() {
    this._checkModify();
    this.addOption(DBQuery.Option.noTimeout);
    return this;
}

/**
* Internal replication use only - driver should not set
*
* @method
* @see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query
* @return {DBQuery}
*/
DBQuery.prototype.oplogReplay = function() {
    this._checkModify();
    this.addOption(DBQuery.Option.oplogReplay);
    return this;
}

/**
* Limits the fields to return for all matching documents.
*
* @method
* @see http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/
* @param {object} document Document specifying the projection of the resulting documents.
* @return {DBQuery}
*/
DBQuery.prototype.projection = function(document) {
    this._checkModify();
    this._fields = document;
    return this;
}

/**
* Specify cursor as a tailable cursor, allowing to specify if it will use awaitData
*
* @method
* @see http://docs.mongodb.org/manual/tutorial/create-tailable-cursor/
* @param {boolean} [awaitData=true] cursor blocks for a few seconds to wait for data if no documents found.
* @return {DBQuery}
*/
DBQuery.prototype.tailable = function(awaitData) {
    this._checkModify();
    this.addOption(DBQuery.Option.tailable);

    // Set await data if either specifically set or not specified
    if(awaitData || awaitData == null) {
        this.addOption(DBQuery.Option.awaitData);
    }

    return this;
}

/**
* Specify a document containing modifiers for the query.
*
* @method
* @see http://docs.mongodb.org/manual/reference/operator/query-modifier/
* @param {object} document A document containng modifers to apply to the cursor.
* @return {DBQuery}
*/
DBQuery.prototype.modifiers = function(document) {
    this._checkModify();

    for(var name in document) {
        if(name[0] != '$') {
            throw Error('All modifiers must start with a $ such as $maxScan or $returnKey');
        }
    }

    for(var name in document) {
        this._addSpecial(name, document[name]);
    }

    return this;
}

DBCollection.prototype._distinct = function( keyString , query ){
    return this._dbReadCommand( { distinct : this._shortName , key : keyString , query : query || {} } );
}

//
// CRUD specification aggregation cursor extension
//

DBCollection.prototype.aggregate = function(pipeline, aggregateOptions) {
    if (!(pipeline instanceof Array)) {
        // support legacy varargs form. (Also handles db.foo.aggregate())
        pipeline = argumentsToArray(arguments)
        aggregateOptions = {}
    } else if (aggregateOptions === undefined) {
        aggregateOptions = {};
    }

    // Copy the aggregateOptions
    var copy = Object.extend({}, aggregateOptions);

    // Ensure handle crud API aggregateOptions
    var keys = Object.keys(copy);

    for (var i = 0; i < keys.length; i++) {
        var name = keys[i];

        if (name == 'batchSize') {
            if (copy.cursor == null) {
                copy.cursor = {};
            }

            copy.cursor.batchSize = copy['batchSize'];
            delete copy['batchSize'];
        } else if (name == 'useCursor') {
            if (copy.cursor == null) {
                copy.cursor = {};
            }

            delete copy['useCursor'];
        }
    }

    // Assign the cleaned up options
    aggregateOptions = copy;
    // Create the initial command document
    var cmd = {pipeline: pipeline};
    Object.extend(cmd, aggregateOptions);

    if (!('cursor' in cmd)) {
        // implicitly use cursors
        cmd.cursor = {};
    }

    // in a well formed pipeline, $out must be the last stage. If it isn't then the server
    // will reject the pipeline anyway.
    var hasOutStage = pipeline.length >= 1 && pipeline[pipeline.length - 1].hasOwnProperty("$out");

    var doAgg = function(cmd) {
        // if we don't have an out stage, we could run on a secondary
        // so we need to attach readPreference
        return hasOutStage ?
            this.runCommand("aggregate", cmd) : this.runReadCommand("aggregate", cmd);
    }.bind(this);

    var res = doAgg(cmd);

    if (!res.ok
        && (res.code == 17020 || res.errmsg == "unrecognized field \"cursor")
        && !("cursor" in aggregateOptions)) {
            // If the command failed because cursors aren't supported and the user didn't explicitly
            // request a cursor, try again without requesting a cursor.
            delete cmd.cursor;

            res = doAgg(cmd);

            if ('result' in res && !("cursor" in res)) {
                // convert old-style output to cursor-style output
                res.cursor = {ns: '', id: NumberLong(0)};
                res.cursor.firstBatch = res.result;
                delete res.result;
            }
        }

    assert.commandWorked(res, "aggregate failed");

    if ("cursor" in res) {
        return new DBCommandCursor(this._mongo, res);
    }

    return res;
}
