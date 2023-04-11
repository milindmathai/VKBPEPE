const platformClient = require('purecloud-platform-client-v2');
const cron = require('node-cron');
const fs = require("fs")
const { stringify } = require("csv-stringify");
const converter = require('json-2-csv');
const date = require('date-and-time')
const winston = require('winston');
const { exec } = require("child_process");
const yargs = require('yargs/yargs')
const { hideBin } = require('yargs/helpers');
const argv = yargs(hideBin(process.argv)).argv
const { combine, timestamp, printf, colorize, align, json } = winston.format;


let s3Name = argv.s3
const client = platformClient.ApiClient.instance;
let apiInstance = new platformClient.ConversationsApi();


const clientId = process.env.clientId; //'1ab47961-e61c-43f0-aa4e-0cebbff924f6'; 
const clientSecret = process.env.clientSecret; //'e9Bl3yA2zT3zUpkkdsRVGCoKhzksDhGC3W1Gg8LVzYg';


const genesysEnv = "mypurecloud.de"
client.setEnvironment(genesysEnv);

const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: combine(timestamp(), json()),
    transports: [
        new winston.transports.File({
            filename: 'combined.log',
        }),
    ],
});

var dict = {
    "INTERVALL": "",
    "DATE_TIME_KEY": 0,
    "VQ": "",
    "N_ENTER_SC": 0,
    "N_ANSWER": 0,
    "N_ABANDON": 0,
    "N_ANSWER_T1": 0,
    "N_ANSWER_T2": 0,
    "N_ANSWER_T3": 0,
    "N_ANSWER_T4": 0,
    "N_ANSWER_T5": 0,
    "N_ANSWER_T6": 0,
    "N_ANSWER_T7": 0,
    "N_ANSWER_T8": 0,
    "N_ANSWER_T9": 0,
    "N_ABANDON_T1": 0,
    "N_ABANDON_T2": 0,
    "N_ABANDON_T3": 0,
    "N_ABANDON_T4": 0,
    "N_ABANDON_T5": 0,
    "N_ABANDON_T6": 0,
    "N_ABANDON_T7": 0,
    "N_ABANDON_T8": 0,
    "N_ABANDON_T9": 0
}


const getData = async (timestamp, interval) => {
    var jsonData = {}
    let time = new Date();
    let currentTime = date.format(time, 'YYYY-MM-DDTHH:mm:ss');
    time.setDate(time.getDate() - 1)

    //The following 4 parameters are created for logging purposes. They tell us in a day how many conversations had no
    // queues and failed to get acd parameters
    let failedToGetConversation = 0
    let noAcd = 0
    let failedWithAcdPresent = 0
    let noQueueName = 0


    console.log(date.format(time, 'YYYY-MM-DDTHH:mm:ss') + "/" + currentTime)

    //A single call of the API can retrieve at max 100 interactions. To account for this we create a the loopToDo and the counter
    // variables. 
    let loopsToDo = 1
    let counter = 1
    while (counter <= loopsToDo) {
        let body = {
            "interval": date.format(time, 'YYYY-MM-DDTHH:mm:ss') + "/" + currentTime,
            "order": "asc",
            "orderBy": "conversationStart",
            "paging": {
                "pageSize": 100,
                "pageNumber": counter
            }
        }
        //Authenticating using the client ID and client Secret
        await client.loginClientCredentialsGrant(clientId, clientSecret)
        //Making the API call
        let data = await apiInstance.postAnalyticsConversationsDetailsQuery(body)

        //Getting the total number of interactions
        let totalHits = data["totalHits"]

        //Checking how many times the loop should run and incrementing the counter.
        loopsToDo = Math.ceil(totalHits / 100)
        counter++
        //Extracting the data from the JSON
        data = data["conversations"]
        try {
            var conversations = data.forEach(conversation => {
                var acd = conversation["participants"].find(name => name.purpose === "acd")
                if (acd) {
                    try {
                        let queueName = acd["participantName"]
                        if (!queueName) {
                            conversation["participants"].forEach(participants => {
                                if (participants["purpose"] == "workflow" && participants["sessions"][0]["flow"]["transferTargetName"]) {

                                    //Normally the queue name is defined in a constant place as participant name, but sometimes it is also located in another place. The line below it to account for that place.
                                    queueName = participants["sessions"][0]["flow"]["transferTargetName"]

                                    //TODO Logging
                                    if (!queueName) {
                                        logger.warn("Queue name not there.  ID: " + conversation["conversationId"] + " and the direction is " + conversation["originatingDirection"]);
                                        noQueueName++

                                    }
                                    if (typeof queueName == "undefined") {
                                        noQueueName++
                                        logger.warn("Queue name is undefined.  ID: " + conversation["conversationId"] + " and the direction is " + conversation["originatingDirection"]);
                                    }

                                }
                            })
                        }

                        //The only time we have acd with no queue name is when there is outbound calls. We dont deal with outbound calls
                        if (!jsonData[queueName]) {
                            jsonData[queueName] = JSON.parse(JSON.stringify(dict))
                        }

                        jsonData[queueName]["N_ENTER_SC"]++
                        jsonData[queueName]["VQ"] = queueName
                        jsonData[queueName]["INTERVALL"] = interval
                        jsonData[queueName]["DATE_TIME_KEY"] = timestamp

                        //
                        let tacd = acd["sessions"][0]["metrics"].find(metric => metric.name == "tAcd")
                        let tdelay = acd["sessions"][0]["metrics"].find(metric => metric.name == "tAbandon")
                        if (tacd["value"] && !tdelay) {
                            jsonData[queueName]["N_ANSWER"]++
                            let valueInSeconds = tacd["value"] / 1000
                            if (valueInSeconds < 20) {
                                jsonData[queueName]["N_ANSWER_T1"]++
                            }
                            else if (valueInSeconds < 30) {
                                jsonData[queueName]["N_ANSWER_T2"]++
                            }
                            else if (valueInSeconds < 45) {
                                jsonData[queueName]["N_ANSWER_T3"]++
                            }
                            else if (valueInSeconds < 60) {
                                jsonData[queueName]["N_ANSWER_T4"]++
                            }
                            else if (valueInSeconds < 90) {
                                jsonData[queueName]["N_ANSWER_T5"]++
                            }
                            else if (valueInSeconds < 120) {
                                jsonData[queueName]["N_ANSWER_T6"]++
                            }
                            else if (valueInSeconds < 210) {
                                jsonData[queueName]["N_ANSWER_T7"]++
                            }
                            else if (valueInSeconds < 300) {
                                jsonData[queueName]["N_ANSWER_T8"]++
                            }
                            else if (valueInSeconds > 300) {
                                jsonData[queueName]["N_ANSWER_T9"]++
                            }
                            else {
                                console.log("No TACD VALUE")
                            }
                        }
                        else if (tacd && tdelay) {
                            //logic for abandons and outbound
                            let tdelay = acd["sessions"][0]["metrics"].find(metric => metric.name == "tAbandon")
                            if (tdelay) {
                                jsonData[queueName]["N_ABANDON"]++
                                let valueInSeconds = tacd["value"] / 1000
                                if (valueInSeconds < 20) {
                                    jsonData[queueName]["N_ABANDON_T1"]++
                                }
                                else if (valueInSeconds < 30) {
                                    jsonData[queueName]["N_ABANDON_T2"]++
                                }
                                else if (valueInSeconds < 45) {
                                    jsonData[queueName]["N_ABANDON_T3"]++
                                }
                                else if (valueInSeconds < 60) {
                                    jsonData[queueName]["N_ABANDON_T4"]++
                                }
                                else if (valueInSeconds < 90) {
                                    jsonData[queueName]["N_ABANDON_T5"]++
                                }
                                else if (valueInSeconds < 120) {
                                    jsonData[queueName]["N_ABANDON_T6"]++
                                }
                                else if (valueInSeconds < 210) {
                                    jsonData[queueName]["N_ABANDON_T7"]++
                                }
                                else if (valueInSeconds < 300) {
                                    jsonData[queueName]["N_ABANDON_T8"]++
                                }
                                else if (valueInSeconds > 300) {
                                    jsonData[queueName]["N_ABANDON_T9"]++
                                }
                            }
                            else {
                                console.log("NO DATA delay")
                            }
                        }

                    } catch (error) {
                        logger.warn("Failed with ACD present: the conversation id is " + conversation["conversationId"] + " and the direction is " + conversation["originatingDirection"]);
                        conversation["participants"].forEach(participants => {
                            failedWithAcdPresent++
                        })
                    }
                }
                else {
                    logger.warn("No acd data for conversation id " + conversation["conversationId"] + " and the direction is " + conversation["originatingDirection"]);
                    noAcd++
                }
            })
        }
        catch (error) {
            console.log(error)
            logger.warn('No Conversation occured in this frame');

            failedToGetConversation++
        }
    }
    logger.info("noQueueName: " + String(noQueueName));
    logger.info("failedWithAcdPresent: " + String(failedWithAcdPresent));
    logger.info("noAcd: " + String(noAcd));
    logger.info("failedToGetConversation: " + String(failedToGetConversation));

    //Writing CSV
    let arrayQueues = Object.values(jsonData)
    let filename = date.format(time, 'YYYY_MM_DD') + ".csv"
    converter.json2csv(arrayQueues, (err, csv) => {
        if (err) {
            throw err
        }
        // print CSV string
        fs.writeFileSync(filename, csv, { encoding: 'utf8' }, function (err) {
            if (err) throw err;
            console.log('file saved');
        })
    })
    exec("aws s3 cp \""+filename+"\" s3://"+s3Name, (error, stdout, stderr) => {
        if (error) {
            console.log(`error: ${error.message}`);
            return;
        }
        if (stderr) {
            console.log(`stderr: ${stderr}`);
            return;
        }
        console.log(`stdout: ${stdout}`);
    });


    
    return true
}

// // // ---------- For a single run uncomment these lines. ------------
// let dateObj = new Date()
// let DATE_TIME_KEY = Date.now()
// let interval = String(dateObj.getUTCFullYear()) + String(dateObj.getMonth() + 1) + String(dateObj.getDate()) + String(dateObj.getHours()) + String(dateObj.getMinutes()) + String(dateObj.getSeconds())
// logger.info("Running script at");
// getData(interval, DATE_TIME_KEY).then((result) => {
//     // console.log("")
// }).catch((e) => {
//     //console.log(e)
// })

logger.info("Script Executed");



cron.schedule('0 0 0 * * *', () => {
    let dateObj = new Date()
    let DATE_TIME_KEY = Date.now()
    let interval = String(dateObj.getUTCFullYear()) + String(dateObj.getMonth() + 1) + String(dateObj.getDate()) + String(dateObj.getHours()) + String(dateObj.getMinutes()) + String(dateObj.getSeconds())
    logger.info("Function call made");
    getData(interval, DATE_TIME_KEY).then((result) => {
        console.log(result)
    }).catch((e) => {
        console.log(e)
    })
})



