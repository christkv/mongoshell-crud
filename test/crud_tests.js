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

//
// deleteMany tests
deleteManyExecutor([{ _id: { $gt: 1 } }], {deletedCount:2}, [{_id:1, x: 11}]);
deleteManyExecutor([{ _id: 4 }], {deletedCount:0}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);

//
// deleteOne tests
deleteOneExecutor([{ _id: { $gt: 1 } }], {deletedCount:1}, [{_id:1, x: 11}, {_id:3, x: 33}]);
deleteOneExecutor([{ _id: 2 }], {deletedCount:1}, [{_id:1, x: 11}, {_id:3, x: 33}]);
deleteOneExecutor([{ _id: 4 }], {deletedCount:0}, [{_id:1, x: 11}, {_id:2, x: 22}, {_id:3, x: 33}]);






