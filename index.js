
require('dotenv').config();
const { getDB, pruneDB } = require("./db");
const { startApi } = require("./api");
const { bootstrapTiny } = require("./firehose");
const DB = getDB(); //initalize databases
const historyProvider = process.env.HISTORY_PROVIDER || 'firehose';

async function main(){
  await startApi();
  const startBlock = await bootstrapTiny();
  await pruneDB();

  if (historyProvider === 'firehose'){
    const {  streamFirehose } = require('./firehose');
    await streamFirehose(startBlock);  
  }
  else if (historyProvider === 'ship'){
    const SHIP = require('./ship');
    const ship = new SHIP();
    ship.start(process.env.SHIP_WS);
  }
  console.log("READY")

}

// var signals = { SIGHUP: 1, SIGINT: 2, SIGTERM: 15 };
// Object.keys(signals).forEach((signal) => {
//   process.on(signal, () => {
//     console.log(`process received a ${signal} signal`);
//     let value = signals[signal];
//     process.exit();
//     // process.exit(128 + value);
//   });
// });

main().then().catch(ex=>{
  console.log("Main error",ex);
});



