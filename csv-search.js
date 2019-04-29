var fs = require('fs');
var csv = require('csv');
var redis = require("redis");
var async = require('async');
var client = redis.createClient();

var express = require('express');
let app = express();

var line = 0;
client.flushdb( function (err, succeeded) {
    console.log(succeeded); // will be true if successfull
});


    csv().from.stream(fs.createReadStream(__dirname + '/data.csv'), {
        columns: ['name', 'age', 'latitude', 'longitude', 'monthly_income',
                              'experienced', ]
                })
        .on('record', function (data) {
            if (line > 1) {
                client.hmset(data.longitude + '_' + data.latitude,  {'name' :  data.name, 
                            'age': data.age,
                            'latitude': data.latitude,
                            'longitude' : data.longitude,
                            'monthly_income': data.monthly_income,
                            'experienced': data.experienced,
                            'score': randomNumberGenerator(0,1 ) });
                }
            line = line + 1
        })
        .on('end', function () {
                    //callback(null, i);
                    console.log('csv file read has been completed.')
        })
        .on('error', function (err) {
                    console.log(err.message);
                    callback(err, i);
        });

const randomNumberGenerator = (min, max) => {
   return Math.random() * (max - min) + min;
};

const scoreCompare = (a,b) => { 
    if (a.score > b.score) {
    return 1;
    }
    else if (a.score < b.score) {
        return -1;
    } else if (a.score == b.score) {
        return 0;
    }
 }

app.get('/people-like-you', function (req, res) {

    let name = req.query.name;
    let age = req.query.age;
    let latitude = req.query.latitue;
    let longitude = req.query.longitude;
    let monthly_income = req.query.monthly_income;
    let experienced = req.query.experienced;
    console.log(latitude);
    console.log(longitude);

    let results = [];
        client.keys('*', function (err, keys) {
            //let results = [];
            if (err) return console.log(err);
            if(keys){
                async.map(keys, function(key, cb) {
                   client.hgetall(key, function (error, value) {
                        if (error) return cb(error);
                        console.log(key);
                        var job = null;
                        if ( (experienced == value.experienced && (age > value.age -1 && age < value.age +2 ) )|| ( Math.floor(latitude) <= value.lalitude &&  Math.ceil(latitude) >= value.latitude ) 
                        && ( Math.ceil(longitude) >= value.longitude &&  Math.floor(longitude) <= value.longitude)) {
                        console.log('match found');
                        job = {};
                        job['jobId']=key;
                        job['data']=value;
                        //results.push(job);
                        console.log(value);
                        }
                        //console.log(results.length);
                        cb(null, job);
                        
                    }); 
                }, function (error, results) {
                   if (error) return console.log(error);
                   results = results.slice(10);
                   console.log(results);
                   console.log('map function finished');
                   //Array.keys(results)
                   arr = [];
                   for (var i=0; i< results.length; i++) {
                       if (results[i] ) {
                            arr.push(results[i].data);
                       }
                   }
                   arr = arr.slice(0,10);
                   arr= arr.sort(scoreCompare);
                   console.log('Generating 10 elements of return arr object');
                   res.json(JSON.stringify(arr));

                });
            }
        });

    });

    let server = app.listen(8000, function() {  
        console.log('Server is listening on port 8080')
    });