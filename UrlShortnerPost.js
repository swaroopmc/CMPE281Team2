var crc = require('crc');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
app.use(express.static('public'));
var crypto = require('crypto');
var shasum = crypto.createHash('md5');
var amqp = require('amqplib/callback_api');


var urlencodedParser = bodyParser.urlencoded({ extended: false })
var mysql      = require('mysql');
var connection = mysql.createConnection({
  host     : 'aasz0fm1nj6uya.csbd8ml2tbxl.us-west-2.rds.amazonaws.com',
  user     : 'team2rdsadmin',
  password : '***********',
  database : 'urlshortener'
});

connection.connect();

function shortner(url){
  if(url in urlMap)
    return urlMap[url];
  var id = crc.crc32(url)

IdMap[id] = url;
var tmpId = id;

var tmp ="";
while(tmpId >0){
  tmp = tmp + converToBase(tmpId%62);
  tmpId = tmpId/62;
  if(tmpId <1)
    tmpId =0;
}

urlMap[url] = tmp;
//TODO :Websitename
var dataToPersisit = {websitename:'Default',originalurl:url,shorturl:tmp};
var query = connection.query('INSERT IGNORE INTO urlinfo SET ?',dataToPersisit, function(err, result) {

  if (!err)
    console.log("The data is stored to DB"+result);
  else{
    console.log("Error while performing Query."+err);
    throw err;
  }
});
console.log(query.sql);
amqp.connect('amqp://localhost', function(err, conn) {
  conn.createChannel(function(err, ch) {
    var q = 'shortner';

    ch.assertQueue(q, {durable: false});
    ch.sendToQueue(q, new Buffer(tmp+","+url));
    console.log(" [x] Sent "+tmp+","+url);
  });
});
return tmp
}


function convertHexToDecimal(hexNumber){
	var decNumber=0;
	console.log("len is"+hexNumber.length);
	for(var i=0;i<hexNumber.length;i++){
		console.log("going..."+i);
		if(hexNumber.charAt(i)=='a'){
     console.log("going...a"+decNumber);
     decNumber = decNumber +10*Math.pow(16,i);
   }
   else if(hexNumber.charAt(i)=='b'){
     decNumber = decNumber +11*Math.pow(16,i);
   }
   else if(hexNumber.charAt(i)=='c'){
     decNumber = decNumber +12*Math.pow(16,i);
   }
   else if(hexNumber.charAt(i)=='d'){
     decNumber = decNumber +13*Math.pow(16,i);
   }
   else if(hexNumber.charAt(i)=='e'){
     decNumber = decNumber +14*Math.pow(16,i);
   }
   else if(hexNumber.charAt(i)=='f'){
     decNumber = decNumber +15*Math.pow(16,i);
   }
 }

 return decNumber;

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
    //console.log("Converetd back"+num);
    return num;
  }
  function findIndexOfChar(char){
   for(var i=0;i<62;i++)
     if(char==baseString.charAt(i)){
     	return i;
     }
     return -1;
   }
   var baseString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
//Map of id to url
var IdMap = {};
//map of url to id
var urlMap ={};

app.post('/shorten', urlencodedParser, function (req, res) {

   // Prepare output in JSON format
   response = {
     shortUrl:"IPADDRESS/"+shortner(req.body.url)
   };
   console.log(response);
   
   res.send(JSON.stringify(response));
   res.writeHeader(200, {"Content-Type": "text/html"});
  res.end();
  
})

var server = app.listen(8081 , function (){

  var host = server.address().address
  var port = server.address().port

  console.log("App listening at http://%s:%s", host, port)

})