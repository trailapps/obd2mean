var express = require('express');
var router = express.Router();

const elasticsearch = require('elasticsearch');
const esClient = new elasticsearch.Client({
    host: 'https://6g27omgy:i0ullpiru43nd7kh@rowan-3016959.us-east-1.bonsaisearch.net',
    log: 'error'
});

esClient.ping({
    requestTimeout: 30000,
}, function (error) {
    if (error) {
        console.error('elasticsearch cluster is down!');
    } else {
        console.log('Connected to ElasticSearch');
    }
});

const search = function search(index, body) {
    return esClient.search({ index: index, body: body });
};

// Get All Tasks
router.get('/tasks', function (req, res, next) {
    let body = { size: 2000, from: 0, query: { match_all: {} }};

    console.log(`retrieving all documents (displaying ${body.size} at a time)...`);
    search('logstash-vehicledata', body)
        .then(results => {
            console.log(`found ${results.hits.total} items in ${results.took}ms`);
            console.log(`returned article titles:`);
            results.hits.hits.forEach((hit, index) => {
            console.log(`\t${body.from + ++index} - ${hit._source.title}`)
            res.json(results);
            }
        );
        })
        .catch(console.error);


});

//Save Task
router.post('/task', function (req, res, next) {
    var obddata = req.body;
    bulkIndex('logstash-vehicledata', 'obd2', obddata);
});

const bulkIndex = function bulkIndex(index, type, data) {
    let bulkBody = [];

    data.forEach(item => {
        bulkBody.push({
            index: {
                _index: index,
                _type: type,
                _id: item.id
            }
        });

        bulkBody.push(item);
    });

    esClient.bulk({ body: bulkBody })
        .then(response => {
            let errorCount = 0;
            response.items.forEach(item => {
                if (item.index && item.index.error) {
                    console.log(++errorCount, item.index.error);
                }
            });
            console.log(`Successfully indexed ${data.length - errorCount} out of ${data.length} items`);
        })
        .catch(console.err);
};



module.exports = router;