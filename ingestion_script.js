const request = require('request');
const config = require('./config');
const elasticsearch = require('elasticsearch');
//const fs = require('fs');
const taskListArr = require('./tasklist.js');
const contentIdMap = require('./contentIdMap.js');
// const catalogSeriesMap = require('./catalog.series.map');
const seriesIdMap = require('./collections/series-map');
const applicationIdMap = require('./collections/application-map');
// const catalogSeriesMap = require('./collections/catalog-map');
let catalogSeriesMap = null;
let counter = 0;

const esClient = new elasticsearch.Client({
	host: config.es.url,
	log: 'error',
	requestTimeout: '90000'
});

/* const taskList = fs.readFileSync('taskList.txt', 'utf-8');
const taskListArr = taskList.split("\r\n"); 
const contentIdText = fs.readFileSync('contentIdMap.json', 'utf-8');
const contentIdMap = JSON.parse(contentIdText);
*/
const batchSize = config.batchSize || 200;
const maxRetryCount = config.retryCount || 3;

let lastRequestIndex = -1;
let bulkBody = [];
let bulkTaskCounter = 0;
let callbackCounter = 0;
let errorTaskListArr = [];
let errorHandleCount = 0;
let batchCounter = 0;
let balooAuthToken = "";


// 2. Pick a task id from task list and send request to baloo to get its data.
function syncTask() {
	callbackCounter = 0;
	bulkBody = [];
	bulkTaskCounter = 0;
	batchCounter++;
	for (let i = lastRequestIndex + 1; i < taskListArr.length && bulkTaskCounter < batchSize; i++) {
		bulkTaskCounter++;
		lastRequestIndex = i;
		sendBalooRequest(taskListArr[i], i);
	}
};

/** 3. Call the Baloo API with authentication token and task id.
 * 		a) When we get a success response then modifying data as our requrement.
 * 		b) Creating bulk update body using the formatted data.
 * 		c) Ingecting the data in the server.
 */
function sendBalooRequest(taskId, index) {
	let options = {
		url: `${config.baloo.url}/scenarios/${taskId}?includeActions=false`,
		headers: {
			'Content-Type': 'application/json',
			'Authorization': balooAuthToken
		}
	};
	request(options, function (error, response, body) {
		if (error) {
			callbackCounter++;
			console.log(`Error in baloo get request for taskId -> ${taskId}`);
			if (errorTaskListArr.indexOf(taskId) === -1) {
				errorTaskListArr.push(taskId);
			}
		} else {
			console.log(`Baloo request succeeded for task number: ${++counter} of task: ${taskId}`);
			let response = JSON.parse(body);
			let updatedResponseObj = modifyBalooResponse(response, taskId);

			let options1 = {
				url: `${config.baloo.url}/element/${response.taskId}/ancestors`,
				headers: {
					'Content-Type': 'application/json',
					'Authorization': balooAuthToken
				}
			};
			request(options1, (err, res, body1) => {
				callbackCounter++;
				if (err) {
					console.log(`Error in baloo get request for metadata of taskId: -> ${response.taskId}`);
					if (errorTaskListArr.indexOf(taskId) === -1) {
						errorTaskListArr.push(taskId);
					}
				} else {
					let response1 = JSON.parse(body1);
					modifyUpdatedResponseObj(response1, updatedResponseObj);
				}

				if (callbackCounter === bulkTaskCounter) {
					updateElasticSearchServer();
				}
			});

			addBulkUpdateBodyForElasticServer(taskId, updatedResponseObj);
		}
	})
}

// 4. Modify baloo API response.
function modifyBalooResponse(response, taskId) {
	var updatedResponseObj = {};
	updatedResponseObj.title = response.title;
	updatedResponseObj.taskId = response.friendlyId;
	updatedResponseObj.steps = response.steps;
	updatedResponseObj.scenario = response.type.code;
	updatedResponseObj.isActive = response.isActive;
	updatedResponseObj.skillAccessible = response.skillAccessible;
	updatedResponseObj.simulationAccessible = response.simulationAccessible;
	updatedResponseObj.visualClickStreamEnabled = response.visualClickStreamEnabled;
	updatedResponseObj.contentId = contentIdMap[taskId];
	updatedResponseObj.uid = response.friendlyId.split('.').join('').toLocaleLowerCase();

	// Removing _id, threads, methods from steps
	if (updatedResponseObj.steps && updatedResponseObj.steps.length > 0) {
		for (let i = 0; i < updatedResponseObj.steps.length; i++) {
			delete(updatedResponseObj.steps[i]._id);
			delete(updatedResponseObj.steps[i].threads);
			delete(updatedResponseObj.steps[i].methods);
		}
	}

	return updatedResponseObj
}

// 5. Now we have the data as required, here creating bulk update body for ingestion in server.
function addBulkUpdateBodyForElasticServer(taskId, data) {
	bulkBody.push({
		index: {
			_index: config.es.index,
			_type: config.es.type,
			_id: taskId
		}
	});
	bulkBody.push(data);
}

// Finally ingesting data in Bonsai server.
function updateElasticSearchServer() {
	esClient.bulk({
			body: bulkBody
		})
		.then((response) => {
			let errorCount = 0;
			response.items.forEach(item => {
				if (item.index && item.index.error) {
					++errorCount;
					console.log(`Error in Elastic Search Indexing for taskId -> ${item._id}`);
					if (errorTaskListArr.indexOf(item._id) === -1) {
						errorTaskListArr.push(taskId);
					}
				} else {
					if (errorTaskListArr.length > 0 && errorTaskListArr.indexOf(item.index._id) !== -1) {
						errorTaskListArr.splice(errorTaskListArr.indexOf(item.index._id), 1);
					}
				}
			});

			console.log(`Successfully indexed ${bulkBody.length / 2 - errorCount} out of ${bulkTaskCounter} items in Batch No. ${batchCounter}`);

			if (lastRequestIndex + 1 !== taskListArr.length) {
				syncTask();
			} else {
				if (errorTaskListArr.length > 0 && errorHandleCount < maxRetryCount) {
					console.log("Now handling failures of previous batches");
					errorHandleCount++;
					syncErrorTask();
				}

				if (errorTaskListArr.length === 0) {
					console.log("Ingestion completed with no issues.")
				}

				if (errorTaskListArr.length > 0 && errorHandleCount >= maxRetryCount) {
					console.log(`Ingestion completed with following issues ${errorTaskListArr.toString()}`);
				}
			}
		})
		.catch((error) => {
			console.log(`Error while indexing Batch No. ${batchCounter}`);
			if (lastRequestIndex + 1 !== taskListArr.length) {
				errorTaskListArr = errorTaskListArr.concat(taskListArr.slice(lastRequestIndex + 1 - batchSize, lastRequestIndex + 1));
				syncTask();
			} else {
				if (errorTaskListArr.length > 0 && errorHandleCount < maxRetryCount) {
					errorHandleCount++;
					syncErrorTask();
				}

				if (errorTaskListArr.length === 0) {
					console.log("Ingestion completed with no issues.")
				}

				if (errorTaskListArr.length > 0 && errorHandleCount >= maxRetryCount) {
					console.log(`Ingestion completed with following issues ${errorTaskListArr.toString()}`);
				}
			}
		});
}

function syncErrorTask() {
	callbackCounter = 0;
	bulkBody = [];
	bulkTaskCounter = 0;
	batchCounter++;
	for (let i = 0; i < errorTaskListArr.length && bulkTaskCounter < batchSize; i++) {
		bulkTaskCounter++;
		sendBalooRequest(errorTaskListArr[i], i);
	}
};


/** 1. Authentication with Baloo
 * 		a) Call syncTask() function to send request(s) to baloo.
 */
request.post({
	url: config.baloo.url + '/auth/signin',
	form: {
		"username": config.baloo.username,
		"password": config.baloo.password
	}
}, function (err, httpResponse, body) {
	if (err) {
		console.log("Authentication Error for Baloo Server");
	} else {
		if (httpResponse.statusCode === 200) {
			balooAuthToken = "JWT " + httpResponse["headers"]["authorization-token"];
			/* Promise.all([
					getProductSeriesMap(),
					getSeriesIdMap(),
					getApplicationIdMap(),
					getChapterIdMap(),
					getProjectIdMap()
				])
				.then(([catalogSeriesMap, seriesIdMap, applicationIdMap, chapterIdMap, projectIdMap]) => {
					catalogSeriesMap = catalogSeriesMap;
					seriesIdMap = seriesIdMap;
					applicationIdMap = applicationIdMap;
					chapterIdMap = chapterIdMap;
					projectIdMap = projectIdMap;
					syncTask();
				}); */
			getProductSeriesMap()
				.then((response) => {
					catalogSeriesMap = response;
					syncTask();
				});
		} else {
			console.log("Authentication Error for Baloo Server");
		}
	}
});

// 6. 

function modifyUpdatedResponseObj(res, updatedResponseObj) {
	res.forEach((ele) => {
		switch (ele.type) {
			case 'cms_series':
				let series = getSeries(ele._id);
				updatedResponseObj.series = series;
				updatedResponseObj.product = getProduct(ele._id);
				break;
			case 'cms_section':
				let app = getApp(ele._id);
				updatedResponseObj.application = app;
				break;
			case 'cms_chapter':
				updatedResponseObj.chapter = {
					'id': ele._id,
					'label': ele.title
				};
				break;
			case 'cms_project':
				updatedResponseObj.project = {
					'id': ele._id,
					'label': ele.title
				};
				break;
			default:
				//console.log(`${ele.type}`);
				break;
		}
	});
}

function getProduct(seriesId) {
	try {
		for (let productIndex = 0; productIndex < catalogSeriesMap.length; productIndex++) {
			let productObj = catalogSeriesMap[productIndex];
			let productId = productObj._id;
			let productLabel = productObj.title;
			let seriesArray = productObj.series;

			for (seriesIndex = 0; seriesIndex < seriesArray.length; seriesIndex++) {
				let seriesObj = seriesArray[seriesIndex];
				if (seriesObj._id === seriesId) {
					return {
						'id': productId,
						'label': productLabel
					};
				}
			}
		}
	} catch (error) {
		console.error(error.message);
	}
}

function getProductSeriesMap() {
	return new Promise((resolve, reject) => {
		try {
			let options = {
				url: `${config.baloo.url}/catalog`,
				headers: {
					'Content-Type': 'application/json',
					'Authorization': balooAuthToken
				}
			};
			request(options, function (error, response, body) {
				if (error) {
					reject(`Error in baloo get request for catalog data`);
				} else {
					resolve(JSON.parse(body));
				}
			});
		} catch (error) {
			reject(error.message);
		}
	});
}

function getSeries(id) {
	try {
		for (let seriesIndex = 0; seriesIndex < seriesIdMap.length; seriesIndex++) {
			let seriesObj = seriesIdMap[seriesIndex];
			let seriesId = seriesObj.id;
			let seriesLabel = seriesObj.label;
			let seriesArray = seriesObj.series_mapped;

			for (let index = 0; index < seriesArray.length; index++) {
				let sObj = seriesArray[index];
				if (sObj._id === id) {
					return seriesObj;
				}
			}
		}
	} catch (error) {
		console.error(error.message);
	}
}

function getApp(id) {
	try {
		for (let appIndex = 0; appIndex < applicationIdMap.length; appIndex++) {
			let appObj = applicationIdMap[appIndex];
			let appId = appObj.id;
			let appLabel = appObj.label;
			let appArray = appObj.app_mapped;

			for (let index = 0; index < appArray.length; index++) {
				let aObj = appArray[index];
				if (aObj._id === id) {
					return appObj;
				}
			}
		}
	} catch (error) {
		console.error(error.message);
	}
}