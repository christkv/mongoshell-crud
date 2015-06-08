print("crud_tests")

//
// deleteMany tests
var data = [{_id: 1, x:11}, {_id: 2, x:22}, {_id: 3, x:33}];
var col = db.crud_tests;

// Drop the collection
col.drop();

// Setup of data
col.insertMany(data);

// DeleteMany when many documents match
var result = col.deleteMany({_id: { $gt: 1 }});
assert(result.deletedCount == 2);
var endState = col.find().toArray();
assert(endState.length == 1);
assert(endState[0]._id == 1);
assert(endState[0].x == 11);


// var db = db.getSiblingDB('test');
// print(Object.keys(db))
// db.t.test();