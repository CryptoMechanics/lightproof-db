const hex64 = require('hex64');
const path = require('path');
const loadProto = package => ProtoBuf.loadSync( path.resolve(__dirname, "proto", package));
const protoLoader = require("@grpc/proto-loader");
const loadGrpcPackageDefinition = package => grpc.loadPackageDefinition(protoLoader.loadSync(path.resolve(__dirname, "proto", package), {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
}));
const ProtoBuf = require("protobufjs");
const grpc = require("@grpc/grpc-js");
const eosioProto = loadProto("dfuse/eosio/codec/v1/codec.proto")
const bstreamService = loadGrpcPackageDefinition("dfuse/bstream/v1/bstream.proto").dfuse.bstream.v1;
const eosioBlockMsg = eosioProto.root.lookupType("dfuse.eosio.codec.v1.Block");
const sleep = s => new Promise(resolve=>setTimeout(resolve,s*1000));
const {getDB, getRange, serialize} = require("./db");
const { asBinary } = require('lmdb');
const grpcAddress = process.env.grpcAddress;
console.log("grpcAddress",grpcAddress);

const getClient = () => new bstreamService.BlockStreamV2(
  grpcAddress,
  process.env.grpcInsecure=='true' ? grpc.credentials.createInsecure(): grpc.credentials.createSsl(),{
    "grpc.max_receive_message_length": 1024 * 1024 * 100,
    "grpc.max_send_message_length": 1024 * 1024 * 100,
  }
);

//fetching from firehose
const getFirehoseBlock = (blockNum, irreversible=true, req={}) => new Promise((resolve,reject) => {
  //if (!req.retries && req.retires!==0) req.retries = 2;
  const client = getClient();
  let stream = client.Blocks({
    start_block_num: blockNum,
    stop_block_num: blockNum,
    include_filter_expr: "",
    fork_steps: irreversible?["STEP_IRREVERSIBLE"]: ["STEP_NEW"]
  })

  stream.on("data", (data) => {
    const { block: rawBlock } = data;
    const block = eosioBlockMsg.decode(rawBlock.value)
    // console.log("got block", block.number)
    client.close();
    resolve({block: JSON.parse(JSON.stringify(block, null, "  ")), step:data.step})
  });
  stream.on('error', async error => {
    console.log("error",error)
    client.close();
    if (error.code === grpc.status.CANCELLED) console.log("stream manually cancelled");
    else {
      if(req.retries){
        console.log("req.retries",req.retries)
        console.log({...req, ws: null})
        await sleep((11-req.retries)*0.1);
        req.retries--;
        resolve(await getFirehoseBlock(blockNum, irreversible, req)) ;
      }
      else {
        console.log("Error in get block", error);
        resolve(null)
      }
    }
  })
});

const streamFirehose = forceStartBlock => new Promise( async (resolve, reject)=>{
  const {blocksDB, rootDB, statusDB} = getDB();

  let { lastBlock: start_block_num, lib } = await getRange(); //set start_block_num to last indexed block
  if (lib) start_block_num = lib;  //if lib is recorded, set start_block_num to lib

  console.log("Lib is at " + lib)
  // const forceStartBlock = process.env.forceStartBlock;

  // if (forceStartBlock) start_block_num = forceStartBlock
  if (forceStartBlock) {
    console.log("DB forced to start from "+ forceStartBlock);
    start_block_num = forceStartBlock
  }
  
  if (!start_block_num) start_block_num=1;

  console.log("Starting stream from firehose at "+ start_block_num);
  const client = getClient();
  let stream = client.Blocks({ start_block_num, fork_steps: ["STEP_NEW", "STEP_IRREVERSIBLE"]});

  stream.on("data", async (data) => {
    const { block: rawBlock } = data;
    let block = eosioBlockMsg.decode(rawBlock.value);
    // console.log("block.number",block.number,data.step)
    if( block.number%1000 === 0 && data.step === "STEP_IRREVERSIBLE") console.log("got block", block.number)
    await processBlock({block, step: data.step});
  });

  stream.on('error', async error => {
    // console.log("error",error);
    client.close();
    if (error.code === grpc.status.CANCELLED) console.log("stream manually cancelled");
    else {
      console.log("Error in firehose stream, retrying in 5s", error);
      await sleep(5);
      if (!lib) streamFirehose(forceStartBlock);
      else streamFirehose();
    }
  })

  function processBlock(data){
    return rootDB.transaction(async () => {
      let block = JSON.parse(JSON.stringify(data.block, null, "  "));
      if (data.step === "STEP_IRREVERSIBLE") {
        lib = block.number;
        statusDB.put("lib", block.number);
      }
      const blockMerkle = block.blockrootMerkle;
      blockMerkle.activeNodes.forEach((node,index) => blockMerkle.activeNodes[index] = hex64.toHex(node) );
      var buffer = serialize(block.id, blockMerkle.activeNodes);
      blocksDB.put(block.number, asBinary(buffer));
    });
  }

});

const consoleBlock = async blockNum => {
  let x = await getFirehoseBlock(blockNum);
  x.block.blockrootMerkle.activeNodes.forEach((r,i)=> x.block.blockrootMerkle.activeNodes[i] = hex64.toHex(r) );
  console.log(x.block.number,x.block.id);
  console.log(x.block.blockrootMerkle)
}
module.exports = {
  getFirehoseBlock,
  consoleBlock,
  streamFirehose,
  sleep
}