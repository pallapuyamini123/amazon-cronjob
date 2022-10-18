const { MongoClient } = require('mongodb');
const lodash = require("lodash");
let options = {
    socketTimeoutMS: 1000000,
    connectTimeoutMS: 1000000,
    useNewUrlParser: true,
    useUnifiedTopology: true,
    serverSelectionTimeoutMS: 1000000,
    keepAlive: 1,
    auto_reconnect: true,
};




//connections establisthed between prod and local

var url = "mongodb://mirza_ashraf:M%21rz%40%23A%24hraf@54.161.254.222:27017/admin";
var url2 = "mongodb://localhost:27017/";
var client = new MongoClient(url, options);
client.connect();
var dbo = client.db("alljobs_prod");
var collection_prod = dbo.collection("client_extracted_feed");
console.log("db connection successful");

var client1 = new MongoClient(url2, options);
client1.connect();
var dbo1 = client1.db("mydb");

var collection_local = dbo1.collection("test_backup");
console.log("db2 connection successful");
let Update_cycle;



(async () => {
    var pipeline = [
        {
            "$match": {
                "employerId": "emp-az-appcast",
                "company": /.*amazon.*/i
            }
        },
        {
            "$group": {
                "_id": "$createdDt"
            }
        }
    ];

    const cursor_prod = await collection_prod.aggregate(pipeline, options).toArray();
    console.log("cursor_prod",cursor_prod.length);
    let data = lodash.map(cursor_prod, "_id");
    console.log(data)
    let dates = data.map(e => {
        let val = new Date(e).toString();
        return val
    })
    console.log(dates);
    var pipeline1 = [
        {
            "$match": {
                "employerId": "emp-az-appcast",
                "company": /.*amazon.*/i,
            }
        },
        {
            "$group": {
                "_id": "$createdDt"
            }
        }
    ];
    const cursor_local = await collection_local.aggregate(pipeline1, options).toArray();
    console.log("cursor_local",cursor_local.length);
    let data1 = lodash.map(cursor_local, "_id");
    console.log(data1)
    let dates1 = data1.map(e => {
        let val1 = new Date(e).toString();
        return val1
    })
    console.log(dates1);
    let diffData = lodash.difference(dates, dates1)
    console.log("diffdata", diffData, diffData.length)
    if (diffData.length > 0) {
        var pipeline2 = [
            {
                "$group": {
                    "_id": "$cycle",
                    "count": {
                        "$sum": 1.0
                    }
                }
            },
            {
                "$sort": {
                    "_id": -1.0
                }
            },
            {
                "$limit": 1.0
            }
        ];
        let data2 = await collection_local.aggregate(pipeline2).toArray();
        // console.log(data.length)
        if(data2.length>0){

        let Find_data = await collection_local.find({ "cycle": data2[0]["_id"] }).project({ "cycle": 1 }).toArray();
        let Update_cycle = Find_data[0]["cycle"] + 1;
        console.log(Update_cycle)

        }
        else{
            Update_cycle=parseInt(1)
            console.log(Update_cycle);
        }

        let skip_data = 0
        let flat = true
        while (flat) {
            console.log("hii")
            let n = 15000
            var pipeline3 = [
                {
                    "$match": {
                        "employerId": "emp-az-appcast",
                        "company": /.*amazon.*/i
                    }
                },
                {
                    "$addFields": {
                        "cycle": Update_cycle
                    }
                },
                {
                    "$skip": skip_data
                },
                {
                    "$limit": 10000
                },
                {
                    "$project": {
                        "_id": 0
                    }
                }
            ];
            let myobj = await collection_prod.aggregate(pipeline3).toArray();
            skip_data += myobj.length
            console.log(myobj.length)
            // process.exit()
            if (myobj.length > 0) {
                //console.log("hiii")
                let chunk = lodash.chunk(myobj, parseInt(n))
                for (let eachchunk of chunk) {
                    console.log("length of each chunk=", eachchunk.length)
                    console.log("-------main data inserted to backup------")
                    let ops = await collection_local.insertMany(eachchunk);
                    console.log(typeof (ops), "data", ops)
                    console.log("inserted")
                }
            } else {
                flat = false;
            }
        }
        console.log("--completed the data to inserting into backup---")
    } else {
        console.log("no data")
    }
})();