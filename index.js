/**
 * Created by Pieter Heyvaert, Data Science Lab (Ghent University - iMinds) on 6/10/16.
 */

var fs = require('fs');
var ldf = require('ldf-client');
var N3 = require('n3');
var config = require('./config.json');
var fragmentsClient = new ldf.FragmentsClient(config.server);
var exec = require('child_process').exec;

function mapJSON(data) {

  fs.writeFile(data.module + '.json', data, function (err) {
    if (err) {
      return console.log(err);
    }

    generateRDF(data.module);
  });
}

function generateRDF(module) {
  var dir = __dirname; //'/home/pieter/Developer/demo-thermometer-continuous-datasource/mapper/';
  var outputFile = dir + '/triples.ttl';
  var format = 'turtle';
  var originalMappingFile = dir + '/tessel.rml.ttl';
  var mappingFile = dir + '/custom-tessel.rml.ttl';
  var logFile = dir + '/tessel.rml.log';
  var rmwd = dir;

  //console.log('sed -e \'s/\\*\\*\\*INPUT\\*\\*\\*/' + dir.replace(/\//g, '\\/') + '\\/' + module + '\\.json/g\' ' + originalMappingFile );

  function cb(triples) {
    fs.writeFile(originalMappingFile, triples, function () {
      exec('cd ' + dir + '; rm -f ' + mappingFile + '; sed -e \'s/\\*\\*\\*INPUT\\*\\*\\*/' + dir.replace(/\//g, '\\/') + '\\/' + module + '\\.json/g\' ' + originalMappingFile + ' > ' + mappingFile, function (error, stdout, stderr) {

        exec('cd ' + rmwd + '; java -jar RML-Mapper.jar -m ' + mappingFile + ' -f ' + format + ' -o ' + outputFile + ' > ' + logFile + '; sed -i \'/^s*$/d\' ' + outputFile, function (error, stdout, stderr) {
          //var readStream = fs.createReadStream(outputFile);

          //readStream.pipe(process.stdout);
          console.log('done');
        });
      });
    });
  }

  fetchMapping(module, cb);
}

function fetchMapping(module, cb) {
  var writer = N3.Writer();

  var query = 'PREFIX ex: <http://www.example.com/>' +
    'PREFIX rml: <http://semweb.mmlab.be/ns/rml#>' +
    'PREFIX rr: <http://www.w3.org/ns/r2rml#>' +

    'CONSTRUCT {' +
    '?tm rml:logicalSource [' +
    '  rml:source ?source;' +
    'rml:referenceFormulation ?refForm;' +
    'rml:iterator ?iterator' +
    '];' +

    'rr:subjectMap [' +
    '  rr:template ?template;' +
    'rr:class ?class' +
    ']' +
    '}' +
    'WHERE {' +
    ' ?tm ex:useWithTesselModule \'' + module + '\' .' +
    '  ?tm rml:logicalSource ?logicalSource .' +
    ' ?tm rr:subjectMap ?sm .' +

    '?logicalSource rml:source ?source .' +
    '?logicalSource rml:referenceFormulation ?refForm .' +
    '?logicalSource rml:iterator ?iterator .' +

    '?sm rr:template ?template .' +
    '?sm rr:class ?class .' +
    '}';

  //var query = 'select * where {?s ?p ?o.}';

  results = new ldf.SparqlIterator(query, {fragmentsClient: fragmentsClient});
  results.on('data', function (d) {
    console.log(d);
    writer.addTriple(d.subject, d.predicate, d.object);
  });

  results.on('end', function () {
    writer.end(function (error, result) {cb(result);});
  });
}

module.exports = {
  mapJSON: mapJSON
};