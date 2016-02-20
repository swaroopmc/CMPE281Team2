var express = require('express');
var app = express();
var bodyParser = require('body-parser');
app.use(express.static('public'));
var redis = require('redis');
var client = redis.createClient(6379, "52.35.55.113"); //creates a new client
var amqp = require('amqplib/callback_api');
var MongoClient = require('mongodb').MongoClient;

  
var expandedUrl = undefined;
var urlencodedParser = bodyParser.urlencoded({ extended: false })
var mysql      = require('mysql');
client.on('connect', function() {
    console.log('Connected to REDIS!');
});


amqp.connect('amqp://localhost', function(err, conn) {
  conn.createChannel(function(err, ch) {
  var q = 'shortner';

  ch.assertQueue(q, {durable: false});
  console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q);
  ch.consume(q, function(msg) {
  var recvd = msg.content.toString();
    client.set(recvd.split(",")[0],recvd.split(",")[1]);
  console.log(" [x] Received %s", msg.content.toString()+" "+recvd.split(",")[0]+" "+recvd.split(",")[1]);
  }, {noAck: true});
  });
});

var connection = mysql.createConnection({
  host     : 'aasz0fm1nj6uya.csbd8ml2tbxl.us-west-2.rds.amazonaws.com',
  user     : 'team2rdsadmin',
  password : '********',
  database : 'urlshortener'
});

connection.connect();

function expand(shortUrl){
console.log("lets check"+shortUrl);
client.get(shortUrl, function(err, reply) {
    console.log("The expanded url is "+reply);
    expandedUrl = reply;

    if(reply==null){
      connection.query("SELECT originalurl from urlinfo where shorturl='"+shortUrl+"'", function(err, rows, fields) {
      if (!err && rows.length!=0){
        console.log('The solution is: ', rows[0]);
        expandedUrl = rows[0].originalurl;
        client.set(shortUrl,rows[0].originalurl, function(err, reply) {
        console.log("The Shorturl "+shortUrl+"is updated in the Redis Cache!!");
      });
      }
      else{
        console.log('Error while performing Query.'+err);
      }
      });
    }
 });
  
  //update the stats
  //simulating the sources,for demo purpose.
  MongoClient.connect("mongodb://admin:admin@ds063124.mongolab.com:63124/stats", function(err, db) {
  if(err) { return console.dir(err); }
  console.log("Updating STATS!!")
  var id = Date.now();
  var source,location;
  if(id%3==0){
    source = 'www.facebook.com';
    location ='Germany';
  }
  else if(id%3==1){
    source = 'www.twitter.com' ;
    location ='USA';
  }
  else{
    source = 'www.quora.com';
    location ='Canada';
  }
  var collection_hits = db.collection('webstats_hits');
  var collection_source = db.collection('webstats_source');
  var colection_location = db.collection('webstats_location');

  collection_hits.update({'key':shortUrl}, {'$inc':{hits:1}},{'upsert':true});
  collection_source.insertOne({'source':source,'key':shortUrl});
  colection_location.insertOne({'location':location,'key':shortUrl});

});

}


function converToBase(val){    
  return baseString.charAt(val);
}

function convertBaseToDecimal(baseStr){
var len = baseStr.length;
    var num=0;
    var ch;
for(var i=len-1;i>=0;i--){
    ch = findIndexOfChar(baseStr.charAt(i))
    num = num + ch * Math.pow(62,i);
 }
    return num;
}
 function findIndexOfChar(char){
 for(var i=0;i<62;i++)
     if(char==baseString.charAt(i)){
      return i;
     }
     return -1;
 }

 function validateUrl(url){
    var ind =url.lastIndexOf("/");
    if(ind >-1 && url.substring(ind+1,url.length)!=""){
      return (url.substring(ind+1,url.length));
    }
    else{
      console.log("Please input the URL in the format:\"https:\/\/IpAddress\/<code>\"");
      return -1;
    }
 }
var baseString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
var IdMap = {};
var urlMap ={};

//syntax : http://127.0.0.1/hits/IsriPe
app.get('/hits/:keys', function(req, res) {
  console.log("Get hits"+req.params.keys);
  MongoClient.connect("mongodb://admin:admin@ds063124.mongolab.com:63124/stats", function(err, db) {
    var collection = db.collection('webstats_hits');
    collection.findOne({key:req.params.keys},{},function(e,docs){
           res.send(docs);
      });
  });
})

//synatax : http://127.0.0.1/sources?keys=IsriPe&source=www.facebook.com
app.get('/sources', function(req, res) {
  var cnt;
  var sources = req.query.source;
  var keys = req.query.keys;
    console.log("Get source hits:"+keys+" "+sources);

  MongoClient.connect("mongodb://admin:admin@ds063124.mongolab.com:63124/stats", function(err, db) {
    if(err){
      console.log("error is"+err);
    }
    var collection = db.collection('webstats_source');
     collection.count({key:keys,source:sources},function(err, c) {
        cnt = c;
               console.log("Countis"+c);
               response = {
               count:cnt
               };
        res.send(JSON.stringify(response));

     });
  });
       
})

//syntax : http://127.0.0.1/locations?keys=UXstue&location=Canada
app.get('/locations', function(req, res) {
  var cnt;
  var location = req.query.location;
  var keys = req.query.keys;
    console.log("Get source hits:"+keys+" "+location);

  MongoClient.connect("mongodb://admin:admin@ds063124.mongolab.com:63124/stats", function(err, db) {
    if(err){
      console.log("error is"+err);
    }
    var collection = db.collection('webstats_location');
     collection.count({key:keys,location:location},function(err, c) {
        cnt = c;
               console.log("Count is"+c);
               response = {
               count:cnt
               };
        res.send(JSON.stringify(response));

     });
  });      
})

app.get('/:code', function (req, res) {

  var shorturl = req.params.code;
    expand(shorturl);
    setTimeout(function(){
    response = {
       expandedUrl:expandedUrl
   };
   console.log(response);

   res.writeHead(302, {
    'Location':expandedUrl
    });

  res.end();
  }, 400);
  
})

  var server = app.listen(8082, function () {

  var host = server.address().address
  var port = server.address().port

  console.log("App listening at http://%s:%s", host, port)

  
})