var MongoClient = require('mongodb').MongoClient;
const { padEnd } = require('lodash');
const lodash = require("lodash");
var url = "mongodb://localhost:27017/";

MongoClient.connect(url, async function (err, db) {
  if (err) throw err;
  var dbo = db.db("mydb");
  console.log("db connection successful");
  // let result = await dbo.collection("client_extracted_feed").find({},{ projection: { description: 0 } }).limit(5).toArray();

  var query = { "status": { $ne: 2 }, "employerId": "emp-az-appcast", "company": /.*amazon.*/i }
  var totalData = await dbo.collection("data").find(query).toArray();
  let parts = totalData.length / 10
  for (i = 0; i < parts; i++) {
    var cursor = await dbo.collection("data").find(query).project({ "status": 0 }).limit(10).toArray();
    let ids = lodash.map(cursor, "_id")

    let data = await dbo.collection("data").updateMany({ "_id": { $in: ids } }, { $set: { status: 2 } })
    let cursor1 = await dbo.collection("client_extracted_feed_backup").insertMany(cursor)
  }
  partsrem = totalData.length % 10
  if (partsrem != 0) {
    var cursor = await dbo.collection("data").find(query).project({ "status": 0 }).limit(partsrem).toArray();
    let ids = lodash.map(cursor, "_id")

    let data = await dbo.collection("data").updateMany({ "_id": { $in: ids } }, { $set: { status: 2 } })
    let cursor1 = await dbo.collection("client_extracted_feed_backup").insertMany(cursor)
  }
});



