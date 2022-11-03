require('dotenv').config();
const { getDB, getRange } = require("./db");
const { startApi } = require("./api");
const {  streamFirehose } = require('./firehose');

const DB = getDB(); //initalize databases

async function main(){
  console.log(await getRange());
  await startApi();
  await streamFirehose(process.env.FORCE_START_BLOCK);  
  console.log("READY")
}

var signals = {
  'SIGHUP': 1,
  'SIGINT': 2,
  'SIGTERM': 15
};
Object.keys(signals).forEach((signal) => {
  process.on(signal, () => {
    console.log(`process received a ${signal} signal`);
    let value = signals[signal];
    process.exit(128 + value);
  });
});

main().then().catch(ex=>{
  console.log("Main error",ex);
});



