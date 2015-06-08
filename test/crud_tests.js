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

var data = [{_id: 1, x:11}, {_id: 2, x:22}, {_id: 3, x:33}];
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

//
// DeleteMany
//

// DeleteMany when many documents match
deleteManyExecutor([{ _id: { $gt: 1 } }], {deletedCount:2}, [{_id:1, x: 11}]);
// DeleteMany when no document matches
deleteManyExecutor([{ _id: 4 }], {deletedCount:0}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);

//
// DeleteOne
//

// DeleteOne when many documents match
deleteOneExecutor([{ _id: { $gt: 1 } }], {deletedCount:1}, [{_id:1, x: 11}, {_id:3, x: 33}]);
// DeleteOne when one document matches
deleteOneExecutor([{ _id: 2 }], {deletedCount:1}, [{_id:1, x: 11}, {_id:3, x: 33}]);
// DeleteOne when no documents match
deleteOneExecutor([{ _id: 4 }], {deletedCount:0}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);

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







