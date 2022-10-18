const { MongoClient } = require('mongodb');
var url = "mongodb://mirza_ashraf:M%21rz%40%23A%24hraf@54.161.254.222:27017/admin";
var url2 = "mongodb://localhost:27017/";
var client = new MongoClient(url, { 'useUnifiedTopology': true, 'useNewUrlParser': true });
client.connect();
var dbo = client.db("alljobs_prod");
console.log("db connection successful");

var client1 = new MongoClient(url2, { 'useUnifiedTopology': true, 'useNewUrlParser': true });
client1.connect();
var dbo1 = client1.db("mydb");
console.log("db2 connection successful");
myjob(dbo, dbo1)

async function myjob(dbo, dbo1) {

    var query = { "employerId": "emp-az-appcast", "company": /.*amazon.*/i }
    // const compareDate = await dbo.collection("client_extracted_feed").findOne(query)
    //  let recordDate = compareDate.createdDt
    // console.log(recordDate)



    // let cursor = await dbo.collection("client_extracted_feed").find(query).limit(100000).toArray();
    const cursor = await dbo.collection("client_extracted_feed").find(query).batchSize(10000).toArray();
    console.log("length", cursor.length);
    cursor.forEach(async (element) => {
        const recordDate = element.createdDt;
        let date = new Date();
        if (recordDate.getTime() === date.getTime()) {
            console.log("aud did not happen")
        }
        else {
            let cursor1 = await dbo1.collection("test_backup").insertOne(element)
            console.log("aud happen")
        }
    });

}




