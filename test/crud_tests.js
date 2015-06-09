print("crud_tests")

// Get the collection
var col = db.crud_tests;

// Setup
var createTestExecutor = function(instance, method, data, setup, teardown) {
  return function(args, result, state) {
    // Execute setup
    if(setup) setup(instance, method, data);

    // Execute the method with arguments
    var r = instance[method].apply(instance, args);
    // Assert equality
    assert.docEq(result, r);

    // Get all the results
    var results = instance.find({}).toArray();
    // Assert equality
    assert.docEq(state, results);

    // Execute teardown
    if(teardown) teardown(instance, method, data);
  }
}

var data = [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }];
var findOneData = [{ _id:1, x:11 }];
var replaceOneData = [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }];
var updateManyData = [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }];
var updateOneData = [{ _id: 1, x: 11 }, { _id: 2, x: 22 }, { _id:3, x:33 }];
var bulkWriteOrderedData = [{ _id: 1, c: 1 }, { _id: 2, c: 2 }, { _id: 3, c: 3 }];
var bulkWriteUnOrderedData = [{ _id: 1, c: 1 }, { _id: 2, c: 2 }, { _id: 3, c: 3 }];

// Setup method
var setup = function(col, method, data) {
  col.remove({});
  col.insertMany(data);  
}

// Setup executors
var deleteManyExecutor = createTestExecutor(col, 'deleteMany', data, setup);
var deleteOneExecutor = createTestExecutor(col, 'deleteOne', data, setup);
var findOneAndDeleteExecutor = createTestExecutor(col, 'findOneAndDelete', data, setup);
var findOneAndReplaceExecutor = createTestExecutor(col, 'findOneAndReplace', data, setup);
var findOneAndUpdateExecutor = createTestExecutor(col, 'findOneAndUpdate', data, setup);
var insertManyExecutor = createTestExecutor(col, 'insertMany', findOneData, setup);
var insertOneExecutor = createTestExecutor(col, 'insertOne', findOneData, setup);
var replaceOneExecutor = createTestExecutor(col, 'replaceOne', replaceOneData, setup);
var updateManyExecutor = createTestExecutor(col, 'updateMany', updateManyData, setup)
var updateOneExecutor = createTestExecutor(col, 'updateOne', updateOneData, setup);
var bulkOrderedWriteExecutor = createTestExecutor(col, 'bulkWrite', bulkWriteOrderedData, setup);
var bulkUnOrderedWriteExecutor = createTestExecutor(col, 'bulkWrite', bulkWriteUnOrderedData, setup);

//                                                                                                     888               
//  .d8888b.                        888      888       888         d8b 888                  .d88888b.                    
// d88P  Y88b                       888      888   o   888         Y8P 888                 d88P" "Y88b                   
// 888    888                       888      888  d8b  888             888                 888     888                   
// 888        888d888 888  888  .d88888      888 d888b 888 888d888 888 888888 .d88b.       888     888 88888b.  .d8888b  
// 888        888P"   888  888 d88" 888      888d88888b888 888P"   888 888   d8P  Y8b      888     888 888 "88b 88K      
// 888    888 888     888  888 888  888      88888P Y88888 888     888 888   88888888      888     888 888  888 "Y8888b. 
// Y88b  d88P 888     Y88b 888 Y88b 888      8888P   Y8888 888     888 Y88b. Y8b.          Y88b. .d88P 888 d88P      X88 
//  "Y8888P"  888      "Y88888  "Y88888      888P     Y888 888     888  "Y888 "Y8888        "Y88888P"  88888P"   88888P' 
//                                                                                                     888               

//
// BulkWrite
//

bulkOrderedWriteExecutor([[
      { insertOne: { document: {_id: 4, a: 1 } } }
    , { updateOne: { filter: {_id: 5, a:2}, update: {$set: {a:2}}, upsert:true } }
    , { updateMany: { filter: {_id: 6,a:3}, update: {$set: {a:3}}, upsert:true } }
    , { deleteOne: { filter: {c:1} } }
    , { deleteMany: { filter: {c:2} } }
    , { replaceOne: { filter: {c:3}, replacement: {c:4}, upsert:true } }]], {
      acknowledged: true, insertedCount:1, matchedCount:1, deletedCount:2, upsertedCount:2, insertedIds : {'0' : 4 }, upsertedIds : { '1' : 5, '2' : 6 }
    }, [{ "_id" : 3, "c" : 4 }, { "_id" : 4, "a" : 1 }, { "_id" : 5, "a" : 2 }, { "_id" : 6, "a" : 3 }]);

bulkUnOrderedWriteExecutor([[
      { insertOne: { document: { _id: 4, a: 1 } } }
    , { updateOne: { filter: {_id: 5, a:2}, update: {$set: {a:2}}, upsert:true } }
    , { updateMany: { filter: {_id: 6, a:3}, update: {$set: {a:3}}, upsert:true } }
    , { deleteOne: { filter: {c:1} } }
    , { deleteMany: { filter: {c:2} } }
    , { replaceOne: { filter: {c:3}, replacement: {c:4}, upsert:true } }], { ordered: false }], {
      acknowledged: true, insertedCount:1, matchedCount:1, deletedCount:2, upsertedCount:2, insertedIds : {'0' : 4 }, upsertedIds : { '1' : 5, '2' : 6 }
    }, [{ "_id" : 3, "c" : 4 }, { "_id" : 4, "a" : 1 }, { "_id" : 5, "a" : 2 }, { "_id" : 6, "a" : 3 }]);

//
// DeleteMany
//

// DeleteMany when many documents match
deleteManyExecutor([{ _id: { $gt: 1 } }], {acknowledged: true, deletedCount:2}, [{_id:1, x: 11}]);
// DeleteMany when no document matches
deleteManyExecutor([{ _id: 4 }], {acknowledged: true, deletedCount:0}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// DeleteMany when many documents match, no write concern
deleteManyExecutor([{ _id: { $gt: 1 } }, { w : 0 }], {acknowledged: false}, [{_id:1, x: 11}]);

//
// DeleteOne
//

// DeleteOne when many documents match
deleteOneExecutor([{ _id: { $gt: 1 } }], {acknowledged: true, deletedCount:1}, [{_id:1, x: 11}, {_id:3, x: 33}]);
// DeleteOne when one document matches
deleteOneExecutor([{ _id: 2 }], {acknowledged: true, deletedCount:1}, [{_id:1, x: 11}, {_id:3, x: 33}]);
// DeleteOne when no documents match
deleteOneExecutor([{ _id: 4 }], {acknowledged: true, deletedCount:0}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// DeleteOne when many documents match, no write concern
deleteOneExecutor([{ _id: { $gt: 1 } }, {w:0}], {acknowledged: false}, [{_id:1, x: 11}, {_id:3, x: 33}]);

//
// FindOneAndDelete
//

// FindOneAndDelete when one document matches
findOneAndDeleteExecutor([{ _id: { $gt: 1 } }, { projection: { x: 1, _id: 0 }, sort: { x: 1 } }], {x:22}, [{_id:1, x: 11}, {_id:3, x: 33}]);
// FindOneAndDelete when one document matches
findOneAndDeleteExecutor([{ _id: 2 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 } }], {x:22}, [{_id:1, x: 11}, {_id:3, x: 33}]);
// FindOneAndDelete when no documents match
findOneAndDeleteExecutor([{ _id: 4 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 } }], null, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);

//
// FindOneAndReplace
//

// FindOneAndReplace when many documents match returning the document before modification
findOneAndReplaceExecutor([{ _id: { $gt: 1 } }, { x: 32 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 } }], {x:22}, [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]);
// FindOneAndReplace when many documents match returning the document after modification
findOneAndReplaceExecutor([{ _id: { $gt: 1 } }, { x: 32 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }], {x:32}, [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]);
// FindOneAndReplace when one document matches returning the document before modification
findOneAndReplaceExecutor([{ _id: 2 }, { x: 32 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 } }], {x:22}, [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]);
// FindOneAndReplace when one document matches returning the document after modification
findOneAndReplaceExecutor([{ _id: 2 }, { x: 32 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }], {x:32}, [{_id:1, x: 11}, {_id:2, x: 32}, {_id:3, x: 33}]);
// FindOneAndReplace when no documents match returning the document before modification
findOneAndReplaceExecutor([{ _id: 4 }, { x: 44 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 } }], null, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// FindOneAndReplace when no documents match with upsert returning the document before modification
findOneAndReplaceExecutor([{ _id: 4 }, { x: 44 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, upsert:true }], null, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x:44}]);
// FindOneAndReplace when no documents match returning the document after modification
findOneAndReplaceExecutor([{ _id: 4 }, { x: 44 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }], null, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// FindOneAndReplace when no documents match with upsert returning the document after modification
findOneAndReplaceExecutor([{ _id: 4 }, { x: 44 }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true, upsert:true }], {x:44}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 44}]);

//
// FindOneAndUpdate
//

// FindOneAndUpdate when many documents match returning the document before modification
findOneAndUpdateExecutor([{ _id: { $gt: 1 } }, { $inc: { x: 1 } }, { projection: { x: 1, _id: 0 }, sort: { x: 1 } }], {x:22}, [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]);
// FindOneAndUpdate when many documents match returning the document after modification
findOneAndUpdateExecutor([{ _id: { $gt: 1 } }, { $inc: { x: 1 } }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument: true }], {x:23}, [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]);
// FindOneAndUpdate when one document matches returning the document before modification
findOneAndUpdateExecutor([{ _id: 2 }, { $inc: { x: 1 } }, { projection: { x: 1, _id: 0 }, sort: { x: 1 } }], {x:22}, [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]);
// FindOneAndUpdate when one document matches returning the document after modification
findOneAndUpdateExecutor([{ _id: 2 }, { $inc: { x: 1 } }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument: true }], {x:23}, [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]);
// FindOneAndUpdate when no documents match returning the document before modification
findOneAndUpdateExecutor([{ _id: 4 }, { $inc: { x: 1 } }, { projection: { x: 1, _id: 0 }, sort: { x: 1 } }], null, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// FindOneAndUpdate when no documents match with upsert returning the document before modification
findOneAndUpdateExecutor([{ _id: 4 }, { $inc: { x: 1 } }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, upsert:true }], null, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]);
// FindOneAndUpdate when no documents match returning the document after modification
findOneAndUpdateExecutor([{ _id: 4 }, { $inc: { x: 1 } }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true }], null, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// FindOneAndUpdate when no documents match with upsert returning the document after modification
findOneAndUpdateExecutor([{ _id: 4 }, { $inc: { x: 1 } }, { projection: { x: 1, _id: 0 }, sort: { x: 1 }, returnDocument:true, upsert:true }], {x:1}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]);

//
// InsertMany
//

// InsertMany with non-existing documents
insertManyExecutor([[{_id: 2, x: 22}, {_id:3, x:33}]], {acknowledged: true, insertedIds: [2, 3]}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// InsertMany with non-existing documents, no write concern
insertManyExecutor([[{_id: 2, x: 22}, {_id:3, x:33}], {w:0}], {acknowledged: false}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);

//
// InsertOne
//

// InsertMany with non-existing documents
insertOneExecutor([{_id: 2, x: 22}], {acknowledged: true, insertedId: 2}, [{_id:1, x: 11}, {_id:2, x: 22}]);
// InsertMany with non-existing documents, no write concern
insertOneExecutor([{_id: 2, x: 22}, {w:0}], {acknowledged: false}, [{_id:1, x: 11}, {_id:2, x: 22}]);

//
// ReplaceOne
//

// ReplaceOne when many documents match
replaceOneExecutor([{ _id: { $gt: 1 } }, { x: 111 }], {acknowledged:true, matchedCount:1, modifiedCount:1}, [{_id:1, x: 11}, {_id:2, x: 111}, {_id:3, x: 33}]);
// ReplaceOne when one document matches
replaceOneExecutor([{ _id: 1 }, { _id: 1, x: 111 }], {acknowledged:true, matchedCount:1, modifiedCount:1}, [{_id:1, x: 111}, {_id:2, x: 22}, {_id:3, x: 33}]);
// ReplaceOne when no documents match
replaceOneExecutor([{ _id: 4 }, { _id: 4, x: 1 }], {acknowledged:true, matchedCount:0, modifiedCount:0}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// ReplaceOne with upsert when no documents match without an id specified
replaceOneExecutor([{ _id: 4 }, { x: 1 }, {upsert:true}], {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]);
// ReplaceOne with upsert when no documents match with an id specified
replaceOneExecutor([{ _id: 4 }, { _id: 4, x: 1 }, {upsert:true}], {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]);
// ReplaceOne with upsert when no documents match with an id specified, no write concern
replaceOneExecutor([{ _id: 4 }, { _id: 4, x: 1 }, {upsert:true, w:0}], {acknowledged:false}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]);
// ReplaceOne with upsert when no documents match with an id specified, no write concern
replaceOneExecutor([{ _id: 4 }, { _id: 4, x: 1 }, {upsert:true, writeConcern:{w:0}}], {acknowledged:false}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]);

//
// UpdateMany
//

// UpdateMany when many documents match
updateManyExecutor([{ _id: { $gt: 1 } }, { $inc: { x: 1 } }], {acknowledged:true, matchedCount:2, modifiedCount:2}, [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 34}]);
// UpdateMany when one document matches
updateManyExecutor([{ _id: 1 }, { $inc: { x: 1 } }], {acknowledged:true, matchedCount:1, modifiedCount:1}, [{_id:1, x: 12}, {_id:2, x: 22}, {_id:3, x: 33}]);
// UpdateMany when no documents match
updateManyExecutor([{ _id: 4 }, { $inc: { x: 1 } }], {acknowledged:true, matchedCount:0, modifiedCount:0}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// UpdateMany with upsert when no documents match
updateManyExecutor([{ _id: 4 }, { $inc: { x: 1 } }, { upsert: true }], {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]);
// UpdateMany with upsert when no documents match, no write concern
updateManyExecutor([{ _id: 4 }, { $inc: { x: 1 } }, { upsert: true, w: 0 }], {acknowledged:false}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id:4, x: 1}]);

//
// UpdateOne
//

// UpdateOne when many documents match
updateOneExecutor([{ _id: { $gt: 1 } }, { $inc: { x: 1 } }], {acknowledged:true, matchedCount:1, modifiedCount:1}, [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]);
// UpdateOne when one document matches
updateOneExecutor([{ _id: 1 }, { $inc: { x: 1 } }], {acknowledged:true, matchedCount:1, modifiedCount:1}, [{_id:1, x: 12}, {_id:2, x: 22}, {_id:3, x: 33}]);
// UpdateOne when no documents match
updateOneExecutor([{ _id: 4 }, { $inc: { x: 1 } }], {acknowledged:true, matchedCount:0, modifiedCount:0}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);
// UpdateOne with upsert when no documents match
updateOneExecutor([{ _id: 4 }, { $inc: { x: 1 } }, {upsert:true}], {acknowledged:true, matchedCount:0, modifiedCount:0, upsertedId: 4}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}, {_id: 4, x: 1}]);
// UpdateOne when many documents match, no write concern
updateOneExecutor([{ _id: { $gt: 1 } }, { $inc: { x: 1 } }, {w:0}], {acknowledged:false}, [{_id:1, x: 11}, {_id:2, x: 23}, {_id:3, x: 33}]);

//                                                                                                     888               
//  .d8888b.                        888      8888888b.                        888       .d88888b.                    
// d88P  Y88b                       888      888   Y88b                       888      d88P" "Y88b                   
// 888    888                       888      888    888                       888      888     888                   
// 888        888d888 888  888  .d88888      888   d88P .d88b.   8888b.   .d88888      888     888 88888b.  .d8888b  
// 888        888P"   888  888 d88" 888      8888888P" d8P  Y8b     "88b d88" 888      888     888 888 "88b 88K      
// 888    888 888     888  888 888  888      888 T88b  88888888 .d888888 888  888      888     888 888  888 "Y8888b. 
// Y88b  d88P 888     Y88b 888 Y88b 888      888  T88b Y8b.     888  888 Y88b 888      Y88b. .d88P 888 d88P      X88 
//  "Y8888P"  888      "Y88888  "Y88888      888   T88b "Y8888  "Y888888  "Y88888       "Y88888P"  88888P"   88888P' 
//                                                                                                     888               

var data = [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }];
var distinctData = [{ _id: 1, x:11 }, { _id: 2, x:22 }, { _id: 3, x:33 }];

// Setup method
var setup = function(col, method, data) {
  col.remove({});
  col.insertMany(data);  
}

// Setup executors
var countExecutor = createTestExecutor(col, 'count', data, setup);
var distinctExecutor = createTestExecutor(col, 'distinct', distinctData, setup);

//
// Count
//

// Simple count of all elements
countExecutor([{}], 3, data);
// Simple count no arguments
countExecutor([], 3, data);
// Simple count filtered
countExecutor([{_id: {$gt: 1}}], 2, data);
// Simple count of all elements, applying limit
countExecutor([{}, {limit:1}], 1, data);
// Simple count of all elements, applying skip
countExecutor([{}, {skip:1}], 2, data);
// Simple count no arguments, applying hint
countExecutor([{}, {hint: "_id"}], 3, data);

//
// Distinct
//

// Simple distinct of field x no filter
distinctExecutor(['x'], [11, 22, 33], data);
// Simple distinct of field x
distinctExecutor(['x', {}], [11, 22, 33], data);
// Simple distinct of field x filtered
distinctExecutor(['x', {x: { $gt: 11 }}], [22, 33], data);
// Simple distinct of field x filtered with maxTimeMS
distinctExecutor(['x', {x: { $gt: 11 }}, {maxTimeMS:100}], [22, 33], data);


