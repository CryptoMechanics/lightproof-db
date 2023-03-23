const path = require('path');
const { asBinary } = require('lmdb');

const loadProto = package => ProtoBuf.loadSync( path.resolve(__dirname, "proto", package));
const protoLoader = require("@grpc/proto-loader");
const loadGrpcPackageDefinition = package => grpc.loadPackageDefinition(protoLoader.loadSync(path.resolve(__dirname, "proto", package), {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
}));
const ProtoBuf = require("protobufjs");
const grpc = require("@grpc/grpc-js");
const eosioProto = loadProto("sf/antelope/type/v1/type.proto")
const firehoseV1Service = loadGrpcPackageDefinition("dfuse/bstream/v1/bstream.proto").dfuse.bstream.v1;
const firehoseV2Service = loadGrpcPackageDefinition("sf/firehose/v2/firehose.proto").sf.firehose.v2;
const firehoseStream = (process.env.FIREHOSE_SERVICE || "").toLocaleLowerCase() == "v2" ? firehoseV2Service.Stream : firehoseV1Service.BlockStreamV2;
const eosioBlockMsg = eosioProto.root.lookupType("sf.antelope.type.v1.Block");

const sleep = s => new Promise(resolve=>setTimeout(resolve,s*1000));
const grpcAddress = process.env.GRPC_ADDRESS;
console.log("grpcAddress",grpcAddress);

const { getDB, getStartBlock, serialize, getRange, deserialize, pruneDB, handleHashesDB } = require("./db");
const { annotateIncrementalMerkleTree } = require("./functions");

const getClient = useBootFirehose => new firehoseStream(
  useBootFirehose ? process.env.BOOT_GRPC_ADDRESS :  process.env.GRPC_ADDRESS,
  useBootFirehose ? process.env.BOOT_GRPC_INSECURE=='true' ? grpc.credentials.createInsecure(): grpc.credentials.createSsl() 
                  : process.env.GRPC_INSECURE=='true' ? grpc.credentials.createInsecure(): grpc.credentials.createSsl(),
  {"grpc.max_receive_message_length": 1024 * 1024 * 100, "grpc.max_send_message_length": 1024 * 1024 * 100 }
);
const toHex = base64 => Buffer.from(base64, 'base64').toString("hex");

const streamFirehose = forceStartBlock => new Promise( async (resolve, reject)=>{
  const {blocksDB, rootDB, statusDB} = getDB();

  const start_block_num = forceStartBlock ? forceStartBlock : await getStartBlock();
  console.log("start_block_num",start_block_num)
  console.log("Starting stream from firehose at "+ start_block_num);
  const client = getClient();
  let stream = client.Blocks({ start_block_num, fork_steps: ["STEP_NEW", "STEP_IRREVERSIBLE"]}); 
  // let stream = client.Blocks({ start_block_num, fork_steps: ["STEP_IRREVERSIBLE"]});//for testing

  stream.on("data", async (data) => {
    const { block: rawBlock } = data;
    let block = eosioBlockMsg.decode(rawBlock.value);
    // console.log("block.number",block.number,data.step)
    if( block.number%1000 === 0 && data.step === "STEP_IRREVERSIBLE") {
      console.log("LIB stored", block.number)
      await pruneDB();
    }
    await processBlock({block, step: data.step});
  });

  stream.on('error', async error => {
    // console.log("error",error);
    client.close();
    if (error.code === grpc.status.CANCELLED) console.log("stream manually cancelled");
    else {
      console.log("Error in firehose stream, retrying in 5s", error);
      await sleep(5);
      streamFirehose();
    }
  })

  function processBlock(data){
    return rootDB.transaction(async () => {
      // let block = JSON.parse(JSON.stringify(data.block, null, "  "));
      let block = data.block;

      //update status DB
      if (data.step === "STEP_IRREVERSIBLE") return statusDB.put("lib", block.number);
      else { //if STEP_NEW
        let date = (new Date(parseInt(block.header.timestamp.seconds)*1000)).toISOString().replace('Z', '');
        if (block.header.timestamp.nanos) date = date.replace('000', '500')
        statusDB.put("lastBlockTimestamp", date); 
      }

      //handle forks for the active nodes of the block;
      let blockExists = blocksDB.getBinary(block.number);
      if (blockExists && data.step=="STEP_NEW"){
        console.log("block already exists, handling the forked blocks active nodes",block.number )
        const existingBlock = await deserialize(blockExists);
        //remove the hash if not used in another block, or reduce its instance count by 1 to roll back the block
        for (var node of existingBlock.nodes) await handleHashesDB(node);
      }

      const blockMerkle = JSON.parse(JSON.stringify(block.blockrootMerkle));
      blockMerkle.activeNodes.forEach((node,index) => blockMerkle.activeNodes[index] = toHex(node) );


      const { blockToEdit } = annotateIncrementalMerkleTree(JSON.parse(JSON.stringify(blockMerkle)), false); 

      const buffer = await serialize(block.id, blockMerkle.activeNodes, blockToEdit.aliveUntil);
      blocksDB.put(block.number, asBinary(buffer));

    });
  }

});

const getBlock = req => new Promise((resolve,reject) => {
  if (!req.retries && req.retires!==0) req.retries = 10;
  const client = getClient(req.useBootFirehose);
  let stream = client.Blocks(req.firehoseOptions)

  stream.on("data", (data) => {
    const { block: rawBlock } = data;
    const block = eosioBlockMsg.decode(rawBlock.value)
    client.close();
    resolve({block, step:data.step})
  });
  stream.on('error', async error => {
    client.close();
    if (error.code === grpc.status.CANCELLED) console.log("stream manually cancelled");
    else {
      if(req.retries){
        console.log("req.retries",req.retries)
        await sleep((11-req.retries)*0.1);
        req.retries--;
        resolve(await getBlock(req)) ;
      }
      else {
        console.log("Error in get block", error);
        console.log({...req, ws: null})
        if (req.ws) req.ws.send(JSON.stringify({ type:"error", error: "Could not stream block from firehose" }));
      }
    }
  })

});

const getIrreversibleBlock = (block_num, useBootFirehose) => getBlock({
  firehoseOptions : {
    start_block_num: block_num,
    stop_block_num: block_num,
    include_filter_expr: "",
    fork_steps: ["STEP_IRREVERSIBLE"]
  },
  useBootFirehose
});

const bootstrapTiny = () => new Promise( async (resolve, reject)=>{
  const {blocksDB, rootDB, statusDB} = getDB();
  const { firstBlock } = await getRange();
  const startSyncHeight = process.env.START_SYNC_HEIGHT;
  const bootstrap = process.env.BOOTSTRAP;
 
  
  //if db contains any blocks, or no START_SYNC_HEIGHT is provided, then no bootstrapping required
  if(bootstrap!='true') console.log("Bootstrap is disabled");
  else if(firstBlock) console.log("Non-empty database, skipping bootstrap");

  if (firstBlock || bootstrap!='true') return resolve();
  
  if (!startSyncHeight){
    console.log("Bootstrap is enabled, but startSyncHeight is not configured")
    process.exit();  
  }
  
  console.log(`\nBootstrapping Tiny-db from block #${startSyncHeight}`);
  let startingBlock = await getIrreversibleBlock(startSyncHeight, true)
  const tree = startingBlock.block.blockrootMerkle;
  tree.activeNodes.forEach( (node,i) => tree.activeNodes[i] = toHex(node))
  let startSyncBlock = {number: startingBlock.block.number, id: startingBlock.block.id, activeNodes: JSON.parse(JSON.stringify(tree.activeNodes))};

  const { blocksRequired } = annotateIncrementalMerkleTree(tree, false);
  console.log("\nblocksRequired",blocksRequired);
  
  let promises = [];
  for (var b of blocksRequired) promises.push(getIrreversibleBlock(b.blockNum, true));

  let result = await Promise.all(promises);
  await rootDB.transaction(async () => {
    const fistBuffer = serialize(startSyncBlock.id, startSyncBlock.activeNodes);
    blocksDB.put(startSyncBlock.number, asBinary(fistBuffer));
    for (var i=0;i<blocksRequired.length;i++){
      const { block } = result[i];
      let firstNode = toHex(block.blockrootMerkle.activeNodes[0]);
      const buffer = serialize(block.id, [firstNode], blocksRequired[i].aliveUntil);
      blocksDB.put(block.number, asBinary(buffer));
      delete block;
    }
    statusDB.put("lib", startSyncBlock.number);
    statusDB.put("minBlockToProve", startSyncBlock.number);
  });
  delete startingBlock;
  console.log("finished bootstrapping")

  resolve(startSyncBlock.number+1);
});

module.exports = {
  streamFirehose,
  sleep,
  bootstrapTiny
}
