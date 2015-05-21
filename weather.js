var _ = require('underscore')
var Q = require("q");
var fs = require('fs')
var ForecastIo = require('forecastio');
var mysql = require('mysql');
var setting = JSON.parse(fs.readFileSync('setting.json').toString());

var connection = mysql.createConnection({
    host: setting.host,
    user: setting.user,
    password: setting.password,
    database: setting.database
});
var forecastIo = new ForecastIo(setting.forecastKey);

connection.connect();
connection.query('TRUNCATE TABLE farmland_forecast');

var queryFarmlandBorder = function () {
    var deferred = Q.defer();
    connection.query('SELECT * from farmland_border', function(err, rows, fields) {
        if (err) {
            deferred.reject(err);
        };
        var results = _.map(_.values(_.groupBy(rows, function(row){ return row.farmland_id; })), function(items) {
            var avgLat = _.reduce(items, function(memo, num){ return memo + num.latitude; }, 0)/items.length;
            var avgLng = _.reduce(items, function(memo, num){ return memo + num.longitude; }, 0)/items.length;
            var farmland_id = items[0].farmland_id;
            return {lat: avgLat, lng: avgLng, id: farmland_id};
        });
        deferred.resolve(results);
    });
    return deferred.promise;    
};

var queryForecastIo = function (avgLat, avgLng, id, options) {
    var deferred = Q.defer();
    forecastIo.forecast(avgLat, avgLng, options, function(err, data) {
        if (err) {
            deferred.reject(err);
        };
        var makeid = function () {
            var text = "";
            var possible = "abcdefghijklmnopqrstuvwxyz0123456789";
            for( var i=0; i < 32; i++ )
                text += possible.charAt(Math.floor(Math.random() * possible.length));
            return text;
        };
        var convertWindBearing = function (windBearing) {
        	return Math.floor((windBearing + 22.5*0.5)/22.5) % 16 + 1;
        };
        var results = _.map(data.hourly.data, function(h) {
            var date = new Date(h.time*1000);
            var year = date.getFullYear();
            var month = (date.getMonth() + 1);
            month = (month < 10) ? ('0' + month) : month;
            var day = date.getDate();
            day = (day < 10) ? ('0' + day) : day;
            var hh = date.getHours();
            hh = (hh < 10) ? ('0' + hh) : hh;
            var m = date.getMinutes();
            m = (m < 10) ? ('0' + m) : m;
            var s = date.getSeconds();
            s = (s < 10) ? ('0' + s) : s;
            var d = year + "-" + month + "-" + day  + " " + hh + ":" + m + ":" + s;
            return [makeid(), id, d, h.temperature, h.humidity*100, h.pressure*100, h.windSpeed, convertWindBearing(h.windBearing), h.precipIntensity , h.cloudCover];
        });
        deferred.resolve(results);
    });
    return deferred.promise;    
};

var insertFarmlandForecast = function (row) {
    var deferred = Q.defer();
    var sql = "insert into farmland_forecast (id, farmland_id, forecast_time, airtemp, airhumidity, atmosphericpressure, windspeed, winddirection, rainfall, cloudiness) values (?,?,?,?,?,?,?,?,?,?)";
    connection.query(sql, row, function(err, results) {
        if (err) {
            deferred.reject(err);
        };
        deferred.resolve(results);                  
    });
    return deferred.promise;    
};

var p = queryFarmlandBorder();
p.then(function (farmlands) {
    var promises = _.map(farmlands, function(farmland) {
        return queryForecastIo(farmland.lat, farmland.lng, farmland.id, {units: 'si'});
    });
    return Q.all(promises);
}).then(function (results) {
    var rows = _.reduce(results, function(memo, num){ return memo.concat(num); }, []);
    var promises = _.map(rows, function(row) {
        return insertFarmlandForecast(row);
    });
    return Q.all(promises);    
}).then(function (results) {
    console.log(results);
    connection.end();    
});
