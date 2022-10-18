const MongoClient = require('mongodb').MongoClient;
const lodash = require("lodash");
var url = "mongodb://localhost:27017/";

MongoClient.connect(url, async function (err, db) {
    if (err) throw err;
    var dbo = db.db("mydb");
    console.log("db connection successful");
    // let result = await dbo.collection("client_extracted_feed").find({},{ projection: { description: 0 } }).limit(5).toArray();
    var query = { "status":{$ne:2},"employerId": "emp-az-appcast", "company": /.*amazon.*/i }
    var cursor = await dbo.collection("client_extract_feed_backup").find(query).project({"status":0}).limit(100000).toArray();
    let ids = lodash.map(cursor, "_id")
  // console.log(ids)
    let data = await dbo.collection("client_extract_feed_backup").updateMany({ "_id": { $in: ids } }, { $set: { "status": 2 } })
   // console.log(data);

    let cursor1 = await dbo.collection("client").insertMany(cursor)


});



