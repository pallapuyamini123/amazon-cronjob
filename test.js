const { MongoClient } = require('mongodb');
const lodash = require("lodash");
var url2 = "mongodb://localhost:27017/";
let options = {
    socketTimeoutMS: 1000000,
    connectTimeoutMS: 1000000,
    useNewUrlParser: true,
    useUnifiedTopology: true,
    serverSelectionTimeoutMS: 1000000,
    keepAlive: 1,
    auto_reconnect: true,
};

var client1 = new MongoClient(url2, options);
client1.connect();
var dbo1 = client1.db("mydb");
console.log("db2 connection successful");
myjob(dbo1)

async function myjob(dbo1) {

    var query = { "employerId": "emp-az-appcast", "company": /.*amazon.*/i }
    // let cursor = await dbo.collection("client_extracted_feed").find(query).limit(100000).toArray();
    const cursor = await dbo1.collection("client_feed").find(query).batchSize(10).toArray();
    console.log("length", cursor.length);
    let cursor1 = await dbo1.collection("test_feed").find().toArray();
    console.log("length", cursor1.length);
    const data1 = lodash.map(cursor, "createdDt");

    // console.log(data1)
    const data2 = lodash.map(cursor1, "createdDt");
    const dif = lodash.differenceWith(data1, data2, lodash.isEqual);
    console.log(dif);
    let new_record = dif.length;

    if (new_record > 0) {
        let records = [];
        for (let i = 0; i < new_record; i++) {
            let record = await dbo1.collection("client_feed").find({ createdDt: new_record[i] }).project({ "_id": 0 }).toArray();
            records.push(record)
        }
        let arr = lodash.flattenDeep(records);
        console.log(arr)
        let diff_data = await dbo1.collection("test_feed").insertMany(arr);
        console.log("aud happen");
    }

    else {
        console.log("aud did not happen");
    }

}





