const config = require('./config.json');
const customers = require('./customers.json');
const util = require('util')
const extend = require('util')._extend;
const http = require('http');
const url = require('url');
const uuid = require('uuid');
const datastore = require('nedb');
var path = require('path');

//for storing context created and context fetched data into NEdb file system.
//from NEdb file system it is periodically pushed into elasticsearch and same is removed from filesystem
var dbcontextsstore = new datastore({ filename: path.join(__dirname, 'contextsstore.db'), autoload: true });

//console.log(path.join(__dirname, 'contextsstore.db'));

var contexts = {};
var resources = new Array();
var occupied_resources = new Array();
console.log('===============================');
console.log(config);
console.log('===============================');
console.log(customers);
console.log('===============================');



// Initialize Contexts and Resources
Object.entries(customers).forEach(([cust, infos]) =>
{
    resources[cust] = Array();
    occupied_resources[cust] = Array();
    contexts[cust] = {};
    Object.values(infos['resources']).forEach((entry) =>
    {
        for (i = 0; i < customers[cust].resource_concurrent_limit; i++) {
            resources[cust].push(entry);
        }        

    });
});


copy_all_contexts_from_other_nodes();


function get_same_resource_avlbl_count(cust, resource) {
    var count = 0;
    for (var i = 0; i < resources[cust].length; ++i) {
        if (resources[cust][i] === resource)
            count++;
    }
    return count;
}

function get_same_resource_occupied_count(cust, resource) {
    var count = 0;
    for (var i = 0; i < occupied_resources[cust].length; ++i) {
        if (occupied_resources[cust][i] === resource)
            count++;
    }
    return count;
}


function contexts_cleanup() {
    let limit = Math.floor(Date.now() / 1000) - config.context_timeout;
    for (let [customer, cust_contexts] of Object.entries(contexts))
        for (let [_uuid, context] of Object.entries(cust_contexts)) {
            if (context['_timestamp'] < limit) {
                //var resOrUuid = (resource.toString().length === 36) ? 'uuid' : 'resource'; 
                console.log(logtime() + 'Automatic release of Resource [' + context['resource'] + '; uuid (' + _uuid + ')] for customer [' + customer + '] and associated context clean up');
                free_context(customer, context['resource'], _uuid);
            }
        }
    print_statistics();
}
setInterval(contexts_cleanup, config.cleanup_interval * 1000);

function delete_context_nedb(docId) {
    dbcontextsstore.remove({ _id: docId }, function (err, numDeleted) {
        if (err) {
            console.log('delete_context_nedb() error [_id:', docId, '] - ', err);
            return false;
        }
        else {
            console.log('Deleted doc from contextsstore.db file system [_id:', docId, ']');
            return true;
        }
    });
}


function elasticsearch_add_data(indxname, nedbJsonDoc) {  
        var docId = nedbJsonDoc['_id']
        delete nedbJsonDoc['_id'];    
        const post_data = JSON.stringify(nedbJsonDoc); 
    
        const _URL = url.parse(config.elasticsearch_url);
        const indxPath = indxname + '/contextmanager';
        const options = {
            hostname: _URL.hostname,
            port: _URL.port,
            path: indxPath,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': post_data.length,
            }
        }

        const request = http.request(options, (response) => {
            let chunks_of_data = [];
            response.on('data', (fragments) => {
                chunks_of_data.push(fragments);
            });

        response.on('end', () => {
            let response_body = Buffer.concat(chunks_of_data);
            console.log(logtime() + "elasticsearch_add_data (" + indxPath + ") response: " + response.statusCode + " - " + response_body.toString());
            if (response.statusCode === 201) {
                //console.log(docId);
                delete_context_nedb(docId);
            }
        });

        response.on('error', (error) => {
            console.log(logtime() + "Error getting response for elasticsearch_add_data (" + indxPath + ")" + error.message);
           
        });
    });

    request.on('error', (error) => {        
        console.log(logtime() + "Error sending request for elasticsearch_add_data (" + indxPath + ")" + error.message);        
    });

    request.write(post_data);
    request.end();
    
}


//pull data from nedb file system and periodically push into elasticsearch
function periodic_pull_nedb() {

    if (config.elasticsearch_push_enable == false)
        return;

    try {


        dbcontextsstore.persistence.compactDatafile();
        var rowCnt = 0;
        dbcontextsstore.find({}, function (err, docs) {
            if (err) console.log('periodic_pull_nedb() error - ' + err);
            else {
                docs.forEach(function (doc) {
                    rowCnt++;
                    if (rowCnt > config.elasticsearch_push_bunch_size) return;
                    console.log('Trying to push context (uuid:', doc.uuid, '; action:', doc.action, ') into elasticsearch db');
                    var indexName = 'contextsstore_' + ((new Date()).getMonth() + 1) + '_' + (new Date()).getFullYear();
                    //console.log(indexName);
                    elasticsearch_add_data(indexName, doc)
                });
            }
        });
    } catch (err) {
        console.log('periodic_pull_nedb() - ' + err);
    }
   
}
setInterval(periodic_pull_nedb, config.elasticsearch_push_interval * 1000);


var server = http.createServer(function(req, res) {
    var page = url.parse(req.url).pathname;
	if ((req.method === 'POST') && (page == '/contextmanager'))		
	{
		let body = '';
		req.on('data', chunk => { body += chunk.toString(); });
		req.on('end', () => {
            try {
               
                let reqIPAddress = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
                //x-real-ip //x-forwarded-for
                let body_json = JSON.parse(body);
                //console.log(">>>>>> " + JSON.stringify(body_json));
                if (body_json.customer !== undefined && body_json.token !== undefined)
                {
                    if (customers.hasOwnProperty(body_json.customer) && customers[body_json.customer]['token'] === body_json.token) {
                        console.log(new Date().toJSON() + " - action '" + body_json.action + "' called by " + reqIPAddress);
                        switch (body_json.action) {
                            case 'createcontext':
                                //console.log('entered cretecontext');
                                let resource = get_resource(body_json.customer);
                                if (resource !== undefined) {

                                    var _uuid = get_uuid();
                                    let temp_context = { '_timestamp': Math.floor(Date.now() / 1000), 'resource': resource.toString(), '_ani': body_json.ani, '_data': JSON.stringify(body_json), 'uuid': _uuid };

                                    //http mechanism to check if same resource is occupied or free on other nodes
                                    check_resource_occupancy_on_other_nodes(body_json.customer, resource.toString());

                                    setTimeout(function () {
                                        //if same resource already occupied by other node, then retry to get another free resource
                                        if (is_resource_occupied(body_json.customer, resource) === false) {
                                            //console.log("resource not occupied by other node");
                                            sync_context_on_other_nodes(body_json.customer, resource.toString(), temp_context, _uuid);
                                            contexts[body_json.customer][_uuid] = temp_context;//contexts[body_json.customer][resource] = temp_context;
                                            response_200(res, '{ "resource" : "' + resource.toString() + '", "uuid" : "' + _uuid + '"}');
                                            console.log(logtime() + "Resource [" + body_json.customer +
                                                " (" + resource.toString() + ") (uuid:" + _uuid + ")] occupied. Stored Context -- > \n"
                                                + (contexts[body_json.customer][_uuid]['_data']));
                                            save_context_nedb(temp_context, body_json.action);
                                        }
                                        else {
                                            //console.log("Resource [" + resource.toString() + "] already occupied by other node........trying another");   

                                            var prev_occupied_resource = resource.toString();

                                            resource = undefined;
                                            temp_context = undefined;
                                            resource = get_resource(body_json.customer);
                                            temp_context = { '_timestamp': Math.floor(Date.now() / 1000), 'resource': resource.toString(), '_ani': body_json.ani, '_data': JSON.stringify(body_json), 'uuid': _uuid };

                                            release_resource(body_json.customer, prev_occupied_resource);

                                            if (resource !== undefined) {
                                                //use http mechanism to check if same resource is occupied or free on other nodes
                                                check_resource_occupancy_on_other_nodes(body_json.customer, resource.toString());

                                                setTimeout(function () {
                                                    if (is_resource_occupied(body_json.customer, resource) === false) {
                                                        contexts[body_json.customer][_uuid] = temp_context;//contexts[body_json.customer][resource] = temp_context;
                                                        sync_context_on_other_nodes(body_json.customer, resource.toString(), temp_context, _uuid);
                                                        response_200(res, '{ "resource" : "' + resource.toString() + '", "uuid" : "' + _uuid + '"}');
                                                        console.log(logtime() + "Resource [" + body_json.customer +
                                                            " (" + resource.toString() + ") (uuid:" + _uuid + ")] occupied.Stored Context -- > \n"
                                                            + (contexts[body_json.customer][_uuid]['_data']));
                                                        save_context_nedb(temp_context, body_json.action);
                                                    }
                                                    else {
                                                        release_resource(body_json.customer, resource.toString());
                                                        response_503(res, 'NO_RESOURCE_AVAILABLE');
                                                        console.log(logtime() + '503 NO_RESOURCE_AVAILABLE ->\n' + body);
                                                    }
                                                }, config.other_nodes_sync_timeout);
                                            }
                                            else {
                                                response_503(res, 'NO_RESOURCE_AVAILABLE');
                                                console.log(logtime() + '503 NO_RESOURCE_AVAILABLE ->\n' + body);
                                            }
                                        }
                                    }, config.other_nodes_sync_timeout);
                                }
                                else {
                                    response_503(res, 'NO_RESOURCE_AVAILABLE');
                                    console.log(logtime() + '503 NO_RESOURCE_AVAILABLE ->\n' + body);
                                }
                                break;
                         

                            case 'getcontext':
                                if (body_json.resource !== undefined && body_json.uuid !== undefined && resources.hasOwnProperty(body_json.customer) ) {
                                    let context = retreive_context(body_json.customer, body_json.resource, body_json.ani, body_json.uuid);
                                    if (context !== undefined) {
                                        clear_context_on_other_nodes(body_json.customer, body_json.resource, body_json.uuid);
                                        response_200(res, context);
                                        console.log(logtime() + "Resource [" + body_json.customer +
                                            " (" + body_json.resource + ") (uuid:" + body_json.uuid + ") ] released. Context cleared -- > \n" + context);

                                        let temp_context = {
                                            '_timestamp': Math.floor(Date.now() / 1000), '_ani': body_json.ani, 'resource': body_json.resource,
                                            '_data': JSON.stringify(body_json), 'uuid': body_json.uuid
                                        };

                                        save_context_nedb(temp_context, body_json.action);
                                    }
                                    else {
                                        copy_context_from_other_nodes(body_json.customer, body_json.resource);
                                        setTimeout(function () {
                                            context = retreive_context(body_json.customer, body_json.resource, body_json.ani);
                                            if (context !== undefined) {
                                                clear_context_on_other_nodes(body_json.customer, body_json.resource);
                                                response_200(res, context);
                                                console.log(logtime() + "Resource [" + body_json.customer +
                                                    " (" + body_json.resource + ")] free. Context cleared -->\n " + context);

                                                let temp_context = {
                                                    '_timestamp': Math.floor(Date.now() / 1000), '_ani': body_json.ani, 'resource': body_json.resource,
                                                    '_data': JSON.stringify(body_json), 'uuid': body_json.uuid
                                                };

                                                save_context_nedb(temp_context, body_json.action);
                                            }
                                            else {
                                                response_404(res, 'No matching context found');
                                                console.log(logtime() + '404 No matching context found ->\n' + body);
                                            }
                                        }, config.other_nodes_sync_timeout);
                                    }

                                } else {
                                    response_404(res, 'BAD_REQUEST');
                                    console.log(logtime() + '404 BAD_REQUEST ->\n' + body);
                                }
                                break;

                            case 'createcontext2': //instead of getting free resource no., uuid is generated and considered as resource
                                //console.log('entered cretecontext');
                                var uuid = get_uuid();
                                if (uuid !== undefined) {
                                    let temp_context = { '_timestamp': Math.floor(Date.now() / 1000), '_ani': body_json.ani, '_data': JSON.stringify(body_json), 'uuid': uuid };

                                    //no need to check resource (i.e. uuid) occupancy on other nodes, because each uuid generated is unique, so 
                                    //it can't be duplicated
                                    sync_context_on_other_nodes(body_json.customer, uuid, temp_context);
                                    contexts[body_json.customer][uuid] = temp_context;
                                    response_200(res, '{ "uuid" : "' + uuid + '" }');
                                    console.log(logtime() + "uuid [" + body_json.customer +
                                        " (" + uuid + ")] generated. Stored Context --> \n" + (contexts[body_json.customer][uuid]['_data']));

                                    save_context_nedb(temp_context, body_json.action);
                                }
                                else {
                                    response_503(res, 'NO_UUID_AVAILABLE');
                                    console.log(logtime() + '503 NO_UUID_AVAILABLE ->\n' + body);
                                }
                                break;

                            case 'getcontext2':
                                if (body_json.uuid !== undefined && resources.hasOwnProperty(body_json.customer)) {      
                                    //let full_context = get_full_context(body_json.customer, body_json.uuid);
                                    let context = retreive_context(body_json.customer, body_json.uuid, body_json.ani);
                                    if (context !== undefined) {                                        
                                        let temp_context = { '_timestamp': Math.floor(Date.now() / 1000), '_ani': body_json.ani, '_data': JSON.stringify(body_json), 'uuid': body_json.uuid };
                                        
                                        clear_context_on_other_nodes(body_json.customer, body_json.uuid);
                                        response_200(res, context);
                                        console.log(logtime() + "uuid [" + body_json.customer +
                                            " (" + body_json.uuid + ")] released. Context cleared --> \n" + context);

                                        save_context_nedb(temp_context, body_json.action);
                                    }
                                    else {
                                        copy_context_from_other_nodes(body_json.customer, body_json.uuid);
                                        setTimeout(function () {                    
                                            //full_context = get_full_context(body_json.customer, body_json.uuid);
                                            context = retreive_context(body_json.customer, body_json.uuid, body_json.ani);
                                            if (context !== undefined) {          
                                                let temp_context = { '_timestamp': Math.floor(Date.now() / 1000), '_ani': body_json.ani, '_data': JSON.stringify(body_json), 'uuid': body_json.uuid };

                                                clear_context_on_other_nodes(body_json.customer, body_json.uuid);
                                                response_200(res, context);
                                                console.log(logtime() + "uuid [" + body_json.customer +
                                                    " (" + body_json.resource + ")] released. Context cleared -->\n " + context);

                                                save_context_nedb(temp_context, body_json.action);
                                            }
                                            else {
                                                response_404(res, 'No matching context found');
                                                console.log(logtime() + '404 No matching context found ->\n' + body);
                                            }
                                        }, config.other_nodes_sync_timeout);
                                    }

                                } else {
                                    response_404(res, 'BAD_REQUEST');
                                    console.log(logtime() + '404 BAD_REQUEST ->\n' + body);
                                }
                                break;

                            default:                                
                                response_404(res, 'BAD_REQUEST');
                                console.log(logtime() + '404 BAD_REQUEST ->\n' + body);
                        }
                    }
                    else {
                        response_401(res, 'UNAUTHORIZED');
                        console.log(logtime() + '401 UNAUTHORIZED ->\n' + body);
                    }
                }
                else if (body_json.hasOwnProperty('sync_token') && body_json.action !== undefined && body_json.sync_token === config.sync_token)
                {
                    console.log(logtime() + "action '" + body_json.action + "' called by " + reqIPAddress);  
                    //methods for syncing resource and contexts through internal http communication
                    //methods called by other nodes                   
                    switch (body_json.action) {
                        case '_synccontext':                           
                            if (body_json.resource !== undefined && body_json.context_json !== undefined &&
                                body_json.customer !== undefined && resources.hasOwnProperty(body_json.customer) && body_json.uuid !== undefined) {                           
                                var sync_result = sync_context(body_json.customer, body_json.resource, body_json.context_json, body_json.uuid);
                                var msg = '';
                                if (sync_result === "OK") {
                                    msg = "Resource and context synced up [" + body_json.customer + " (" + body_json.resource + ") (uuid:" + body_json.uuid +")]";
                                    response_200(res, msg);
                                    console.log(logtime() + msg);
                                }
                                else if (sync_result === "NOK") //resource already occupied by this node //
                                {
                                    //console.log("NOK");
                                    msg = "Resource [" + body_json.customer + " (" + body_json.resource + ")] limit already exhausted. Sync not possible.";
                                    response_201(res, msg);
                                    console.log(logtime() + msg);
                                }
                                else {
                                    msg = "Resource and context not synced up [" + body_json.customer + " (" + body_json.resource + ")]";
                                    response_404(res, msg);
                                    console.log(logtime() + msg);
                                }
                            } else response_404(res, 'BAD_REQUEST _synccontext');
                            break;

                        case '_checkresourceoccupancy':
                            if (body_json.resource !== undefined && body_json.customer !== undefined && resources.hasOwnProperty(body_json.customer)) {
                                //var sync_result = check_resource_occupancy(body_json.customer, body_json.resource);
                                var msg = '';
                                if (check_resource_occupancy(body_json.customer, body_json.resource)) {
                                     //resource already occupied by this node 
                                    msg = "Resource [" + body_json.customer + " (" + body_json.resource + ")] limit already exhausted.";
                                    response_201(res, msg);
                                    console.log(logtime() + msg);
                                }
                                else 
                                {
                                    //resource already occupied by this node 
                                    msg = "Resource [" + body_json.customer + " (" + body_json.resource + ")] not exhausted till now.";
                                    response_200(res, msg);
                                    console.log(logtime() + msg);
                                }                               
                            } else response_404(res, 'BAD_REQUEST _checkresourceoccupancy');
                            break;

                        case '_clearcontext':                            
                            if (body_json.uuid !== undefined && body_json.resource !== undefined && body_json.customer !== undefined && resources.hasOwnProperty(body_json.customer)) {
                                var msg = "Resource and context cleared [" + body_json.customer + " (" + body_json.resource + ") (uuid:" + body_json.uuid + ")]";
                                free_context(body_json.customer, body_json.resource, body_json.uuid);
                                console.log(logtime() + msg);
                                response_200(res, msg);
                            } else response_404(res, 'BAD_REQUEST _clearcontext');
                            break;

                        case '_copycontext':
                            if (body_json.uuid !== undefined && body_json.resource !== undefined && body_json.customer !== undefined && resources.hasOwnProperty(body_json.customer)) {
                                if (contexts[body_json.customer][body_json.uuid] === undefined) {//if (contexts[body_json.customer][body_json.resource] === undefined) {
                                    response_200(res, "Context [" + body_json.customer + " (" + body_json.resource + ") (uuid:" + body_json.uuid + ")] already cleared");
                                }
                                else {
                                    //response_202(res, JSON.stringify(contexts[body_json.customer][body_json.resource]));
                                    response_202(res, JSON.stringify(contexts[body_json.customer][body_json.uuid]));
                                }                                
                            } else response_404(res, 'BAD_REQUEST _copycontext');
                            break;

                        case '_copyallcontexts':
                            if (is_contexts_empty()) {
                                response_202(res, 'Contexts is empty');
                            }
                            else {
                                response_200(res, JSON.stringify(contexts));
                            }                            
                            break;

                        default:
                            response_404(res, 'BAD_REQUEST_SYNC');
                    }
                }
                else response_404(res, 'BAD_REQUEST');
			}	
            catch (error) {
                console.log(logtime() + 'Inconsistency body received - ' + body + "\n" + error);
                if (res.writableEnded === false) response_404(res, 'BAD_REQUEST');
			}				
		});
	} else response_404(res);
});

setTimeout(function () {

    var empty = is_contexts_empty();   
    if (empty) {
        console.log(logtime() + "No contexts to copy from any other node");
    }

    sync_resources_upon_start();
    print_statistics();
    server.listen(config.listening_port);
    console.log(logtime() + "http listening at port " + config.listening_port + " started");
}, 5000);


function response_404(res,msg='Not Found')
{
    try {
        res.writeHead(404, { "Content-Type": "text/plain" })
        res.write(msg);
        res.end();
    } catch (err) {
        console.log(logtime(), err);
    }	
}

function response_401(res,msg='Unauthorized')
{
    try {
        res.writeHead(401, { "Content-Type": "text/plain" })
        res.write(msg);
        res.end();

    } catch (err) {
        console.log(logtime(), err);
    }
	
}

function response_503(res,msg='Service Unavailable')
{
	res.writeHead(503, {"Content-Type": "text/plain"})
	res.write(msg);
	res.end();
}

function response_200(res,msg='OK')
{
	res.writeHead(200, {"Content-Type": "application/json"})
	res.write(msg);
	res.end();
}

//to be used when resource is already occupied by this node
function response_201(res, msg = 'OK') {
    res.writeHead(201, { "Content-Type": "application/json" })
    res.write(msg);
    res.end();
}

//to be used when context exist on other nodes
function response_202(res, msg = 'OK') {
    res.writeHead(202, { "Content-Type": "application/json" })
    res.write(msg);
    res.end();
}

function get_resource_old(customer)
{
	return (resources[customer] !== undefined) ? resources[customer].shift() : undefined;
}

//one of free resource will be fetched randomly and will get removed from list
function get_resource(customer) {
    if (resources[customer] !== undefined && resources[customer].length > 0) {
        var randomIndex = Math.floor(Math.random() * resources[customer].length);       
        return resources[customer].splice(randomIndex, 1);        
    }
    else return undefined;
    
}

//function to check if given resource occupied by another node or not;
//true: resource occupied by other node.
//false: resource not occupied and can be used for createcontext api response msg.
function is_resource_occupied(customer, resource) {
    //if (occupied_resources[customer] !== undefined && occupied_resources[customer].indexOf(resource.toString()) !== -1)
    if (occupied_resources[customer] !== undefined && get_same_resource_occupied_count(customer, resource.toString()) >= customers[customer].resource_concurrent_limit)
        return true;

    return false;
}

//function to be used when resource is occupied by another node;
//so same resource needs to be in syncup on this node also
function sync_context(customer, resource, context_json, _uuid) {
    if (resources[customer] !== undefined) {
        var indxResource = resources[customer].indexOf(resource.toString());
        if (indxResource !== -1) { //resource exist in array
            resources[customer].splice(indxResource, 1);
            contexts[customer][_uuid] = context_json;//contexts[customer][resource] = context_json;
            //console.log("Context synced - [" + customer + ": " + resource + "]");
            //console.log("Free resources now => [" + customer + ": (" + resources[customer] + ")]");
            return "OK";
        }
        else
        {
            return "NOK"
        }
    }
   
    return undefined;           
}

//false = resource not occupied;
//true = resource occupied;
function check_resource_occupancy(customer, resource) {
    if (resources[customer] !== undefined) {
        var indxResource = resources[customer].indexOf(resource.toString());
        if (indxResource === -1) { 
            //not part of free resources indicates that resource is already occupied
            return true;
        }
    }
    return false;
}

function logtime() {
    return (new Date()).toJSON() + " - ";
}

//returns current utc time in format 'YYYY-MM-DDTHH:mm:ss.fffZ' (for e.g. '2019-02-07T18:58:34.304Z')
function getISOTimestamp() {
    return new Date().toISOString();   
}


function release_resource(customer,resource)
{
    //if (resources[customer] !== undefined && resources[customer].indexOf(resource.toString()) === -1) resources[customer].push(resource); 
    if (resources[customer] !== undefined && get_same_resource_occupied_count(customer, resource) < customers[customer].resource_concurrent_limit)
        resources[customer].push(resource); 
    if (occupied_resources[customer] !== undefined) {
        var indxOccupiedResource = occupied_resources[customer].indexOf(resource.toString())
        if(indxOccupiedResource !== -1) occupied_resources[customer].splice(indxOccupiedResource, 1);
    }
}

//function free_context(customer,resource)
//{
//	release_resource(customer,resource);	
//	delete contexts[customer][resource];
//}

function free_context(customer, resource, _uuid) {
    release_resource(customer, resource);
    delete contexts[customer][_uuid];
}

function get_full_context(customer, resource) {
    let context = '';
    if (contexts.hasOwnProperty(customer) && contexts[customer].hasOwnProperty(resource)) {
        context = contexts[customer][resource];
    }
    else context = undefined;

    return context;
}

//function retreive_context(customer,resource,ani)
//{
//	let context='';
//	if (contexts.hasOwnProperty(customer) && contexts[customer].hasOwnProperty(resource) && (ani === undefined || contexts[customer][resource]['_ani']===ani)) 
//	{
//		context=contexts[customer][resource]['_data'];
//		free_context(customer,resource);
//	}
//    else context = undefined;

//	return context;
//}

function retreive_context(customer, resource, ani, _uuid) {
    let context = '';

    console.log('>>>>>>>>>' + JSON.stringify(contexts[customer][_uuid]));
    if (contexts.hasOwnProperty(customer) && contexts[customer].hasOwnProperty(_uuid)
        //&& (ani === undefined || contexts[customer][_uuid]['_ani'] === ani)
        //&& (resource === undefined || contexts[customer][_uuid]['resource'] === resource)
    )
    {
        context = contexts[customer][_uuid]['_data'];
        free_context(customer,resource,_uuid);
    }
    else context = undefined;

    return context;
}


function print_statistics()
{
    let stats_c = logtime() + 'Contexts => ';
    let stats_r = logtime() + 'Free resources => ';
	Object.entries(contexts).forEach(([cust, res]) => { stats_c+='['+ cust + ': ' + Object.keys(res).length + '] '; });
	Object.entries(resources).forEach(([cust, res]) => { stats_r+='['+ cust + ': ' + res.length+ '] '; });
    //console.log(stats_c+' | '+stats_r);
    console.log(stats_c);
    //console.log(stats_r);
	//console.log(contexts);    

}

//save contexts created or fetched into nedb file system
function save_context_nedb(contextJson, action) {

    //let tempContext = Object.assign({}, contextJson);
    //Object.assign(tempContext, contextJson);

    //let tempContext = JSON.parse(JSON.stringify(contextJson));
    let tempContext = extend({}, contextJson);

    //console.log('>>>>>>>>' + JSON.stringify(contextJson));
    //console.log('>>>>>>>>' + JSON.stringify(tempContext));

    delete tempContext['_timestamp'];
    
    tempContext['@timestamp'] = getISOTimestamp();
    let _dataField = JSON.parse(tempContext['_data']);
    _dataField['action'] = action;
    _dataField['token'] = 'xxxxxxxxxxxxxx';

    tempContext['data_input'] = JSON.stringify(_dataField);    
    tempContext['customer'] = _dataField.customer;
    tempContext['action'] = action;
    delete tempContext['_data'];

    dbcontextsstore.insert(tempContext, function (err, doc) {
        if (err) console.log(logtime() + 'Error saving context to NeDB file system (action:' + action + ') - ' + err);
        else console.log(logtime(), 'Context [uuid:', doc.uuid, '; _id:', doc._id, '] saved to NeDB file system successfully (action:' + action + ')');
    });
}

function get_uuid() {
    return uuid.v4();
}

//console.log(util.inspect(resources, {showHidden: false, depth: null}))

//to be called at application start
//based on contexts copied from other active node.
//occupied resources will be released from free list.
function sync_resources_upon_start()
{
    Object.entries(contexts).forEach(([customer, cust_contexts]) =>
    {       
        Object.entries(cust_contexts).forEach(([resource, context]) => {
            //console.log(customer + ":" + resource);
            var indxResource = resources[customer].indexOf(resource.toString());
            if (resources[customer] !== undefined && indxResource !== -1) {
                resources[customer].splice(indxResource, 1);

                console.log(logtime() + 'Copied [' + customer + ' (' + resource + ')] context -->\n' + context['_data']);
            }
        });
    });    
}

//copy contexts received from other node
//into this node contexts
function copy_into_contexts(contextsOtherNode) {
    Object.entries(contextsOtherNode).forEach(([customer, cust_contexts]) => {
        Object.entries(cust_contexts).forEach(([resource, contextOtherNode]) => {

            if (contexts[customer] !== undefined) {
                contexts[customer][resource] = contextOtherNode;
            }
        });
    });
}


//true: contexts is empty
//false: contexts not empty
function is_contexts_empty()
{
    var empty = true;
    Object.entries(contexts).forEach(([customer, cust_contexts]) => {
        //console.log('>>>>>>>>>>>>>' + customer);
        if (contexts[customer] !== undefined) {
            //console.log('>>>>>' + customer + ' - ' + Object.keys(contexts[customer]).length);
            if (Object.keys(contexts[customer]).length > 0) {
                //console.log('context copied ');
                empty = false;
            }                
        }      
    }); 

    return empty;
}

//if same resource already occupied on other node then response code 201 is sent
function check_resource_occupancy_on_other_nodes(_customer, _resource) {
    //console.log('entered checkresourceoccupancy');
    if (occupied_resources.hasOwnProperty(_customer) === false)
        return;

    const _action = "_checkresourceoccupancy";
    const post_data = JSON.stringify({"customer": _customer, "action": _action,
                                 "resource": _resource, "sync_token": config.sync_token
    });

    for (var other_node in config.other_nodes) {

        const other_node_url = config.other_nodes[other_node];
        const myURL = url.parse(other_node_url);
        const options = {
            hostname: myURL.hostname,
            port: myURL.port,
            path: myURL.pathname,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': post_data.length,
            }
        }

        const request = http.request(options, (response) => {
            let chunks_of_data = [];
            response.on('data', (fragments) => {
                chunks_of_data.push(fragments);
            });

            response.on('end', () => {
                let response_body = Buffer.concat(chunks_of_data);
                console.log(logtime() +  "action '" + _action + "' response  from '" + other_node_url + "': " + response_body.toString());

                if (response.statusCode === 201) {
                    //console.log(" 201 ");
                    //same resource already occupied by other node which sent this response
                    //hence do not send this resource again for use;
                    //find another free resource
                    //if (occupied_resources[_customer].indexOf(_resource.toString()) === -1) {
                    if (get_same_resource_occupied_count(_customer, _resource.toString()) < customers[_customer].resource_concurrent_limit) {
                        occupied_resources[_customer].push(_resource);
                        console.log(logtime() + "Resource [" + _customer + " (" + _resource + ")] already occupied by '" + other_node_url + "'");
                        return;
                    }
                }
            });

            response.on('error', (error) => {
                console.log(logtime() + "Error getting response for action '" + _action + "' from other node - " + error.message);
            });
        });

        request.on('error', (error) => {
            console.log(logtime() + "Error sending request for action '" + _action + "' to other node - " + error.message);
        });

        request.write(post_data);
        request.end();
    }
}

//synchronization of context on other nodes;
//inform other nodes about resource occupied and context stored for this resource
function sync_context_on_other_nodes(_customer, _resource, _context_json, _uuid) {

    const _action = "_synccontext";
    const post_data = JSON.stringify({
        "context_json": _context_json, "customer": _customer, "action": _action,
        "resource": _resource, "sync_token": config.sync_token, "uuid": _uuid
    });
    
    for (var other_node in config.other_nodes) {
        
            const other_node_url = config.other_nodes[other_node];
            const myURL = url.parse(other_node_url);
            const options = {
                hostname: myURL.hostname,
                port: myURL.port,
                path: myURL.pathname,
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': post_data.length,
                }
            }

            const request = http.request(options, (response) => {
                let chunks_of_data = [];
                response.on('data', (fragments) => {
                    chunks_of_data.push(fragments);
                });

                response.on('end', ()=> {
                    let response_body = Buffer.concat(chunks_of_data);
                    console.log(logtime() + "action '" + _action + "' response from node [" + other_node_url + "]: " + response_body.toString());
                             
                });

                response.on('error', (error) => {
                    console.log(logtime() + "Error getting response for action '" + _action + "' from node [" + other_node_url + "] - " +  error.message);
                });
            });

            request.on('error', (error) => {
                console.log(logtime() + "Error sending request for action '" + _action + "' to node [" + other_node_url + "] - " +  error.message);
            });
                        
            request.write(post_data);
            request.end();          
        }        
}

//clear context from other nodes also once it is retrieved
function clear_context_on_other_nodes(_customer, _resource, _uuid) {
    const _action = "_clearcontext";
    const post_data = JSON.stringify({ "customer": _customer, "action": _action, "resource": _resource, "sync_token": config.sync_token, "uuid": _uuid });
  
    for (var other_node in config.other_nodes) {
        const other_node_url = config.other_nodes[other_node];
        const myURL = url.parse(other_node_url);
        const options = {
            hostname: myURL.hostname,
            port: myURL.port,
            path: myURL.pathname,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': post_data.length,
            }
        }

        const request = http.request(options, (response) => {
            let chunks_of_data = [];
            response.on('data', (fragments) => {
                chunks_of_data.push(fragments);
            });

            response.on('end', () => {
                let response_body = Buffer.concat(chunks_of_data);
                console.log(logtime() + "action '" + _action + "' response from node [" + other_node_url + "]: " + response_body.toString());
            });

            response.on('error', (error) => {
                console.log(logtime() + "Error getting response for action '" + _action + "' from node [" + other_node_url + "] - " + error.message);
            });
        });

        request.on('error', (error) => {
            console.log(logtime() + "Error sending request for action '" + _action + "' to node [" + other_node_url + "] - " + error.message);
        });

        request.write(post_data);
        request.end();
    }
}

//when same _resource is already hold by other nodes, then response 202 is sent 
//and context json is sent in response msg.
function copy_context_from_other_nodes(_customer, _resource, _uuid) {
    const _action = "_copycontext"
    const post_data = JSON.stringify({ "customer": _customer, "action": _action, "resource": _resource, "sync_token": config.sync_token, "uuid": _uuid });
   
    for (var other_node in config.other_nodes) {
       
        var indxResource = resources[_customer].indexOf(_resource.toString());
        if (indxResource === -1)
            return;

        const other_node_url = config.other_nodes[other_node];
        const myURL = url.parse(other_node_url);
        const options = {
            hostname: myURL.hostname,
            port: myURL.port,
            path: myURL.pathname,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': post_data.length,
            }
        }

        const request = http.request(options, (response) => {
            let chunks_of_data = [];
            response.on('data', (fragments) => {
                chunks_of_data.push(fragments);
            });

            response.on('end', () => {
                let response_body = Buffer.concat(chunks_of_data);
                console.log(logtime() + "action '" + _action + "' response from node [" + other_node_url + "]: " + response_body.toString());

                if (response.statusCode === 202) {
                    //console.log(" 202 ");
                    //resource context already occupied by node which sent this response
                    //context json sent in response msg        
                    var indxResource = resources[_customer].indexOf(_resource.toString());
                    if (indxResource !== -1) {
                        resources[_customer].splice(indxResource, 1);
                        
                        let contextSentByNode = JSON.parse(response_body);
                        contexts[_customer][_uuid] = contextSentByNode;//contexts[_customer][_resource] = contextSentByNode;
                        return;
                        //console.log("context copied from other node ==> " + JSON.stringify(contexts[_customer][_resource]));
                    }                    
                }
            });

            response.on('error', (error) => {
                console.log(logtime() + "Error getting response for action '" + _action + "' from node [" + other_node_url + "] - " + error.message);
            });
        });

        request.on('error', (error) => {
            console.log(logtime() + "Error sending request for action '" + _action + "' to node [" + other_node_url + "] - " + error.message);
        });

        request.write(post_data);
        request.end();
    }
}

//to be used upon application startup before server listening
function copy_all_contexts_from_other_nodes() {
    
    console.log(logtime() + 'Checking and copying all contexts from other nodes');

    const _action = "_copyallcontexts";
    const post_data = JSON.stringify({ "action": _action, "sync_token": config.sync_token }); 
        
    for (var other_node in config.other_nodes) {      

        const other_node_url = config.other_nodes[other_node];
        const myURL = url.parse(other_node_url);
        const options = {
            hostname: myURL.hostname,
            port: myURL.port,
            path: myURL.pathname,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': post_data.length,
            }
        }

        const request = http.request(options, (response) => {
            let chunks_of_data = [];
            response.on('data', (fragments) => {
                chunks_of_data.push(fragments);
            });

            response.on('end', () => {
                let response_body = Buffer.concat(chunks_of_data);
                console.log(logtime() + "action '" + _action + "' response from node [" + other_node_url + "]: " + response_body.toString());

                if (response.statusCode === 200) {
                    var contextsOtherNode = JSON.parse(response_body.toString());
                    //contexts = contextToSync;
                    copy_into_contexts(contextsOtherNode);
                }         
            });

            response.on('error', (error) => {
                console.log(logtime() + "Error getting response for action '" + _action + "' from node [" + other_node_url + "] - " + error.message);
            });
        });

        request.on('error', (error) => {
            console.log(logtime() + "Error sending request for action '" + _action + "' to node [" + other_node_url + "] - " + error.message);
        });

        request.write(post_data);
        request.end();
    }
}


//subscribe to listening socket
// subber.js
//var zmq = require("zeromq"),
//  listen_sock = zmq.socket("sub");

//listen_sock.connect("tcp://127.0.0.1:3000");
//listen_sock.subscribe("cats");
//console.log("Subscriber connected to port 3000");

//sock.on("message", function(topic, message) {
//  console.log(
//    "received a message related to:",
//    topic.toString(),
//    "containing message:",
//    message.toString()
//  );
//});
