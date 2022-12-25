const path = require('path');
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
const {getDB, getRange, serialize} = require("./db");
const { asBinary } = require('lmdb');
const grpcAddress = process.env.GRPC_ADDRESS;
console.log("grpcAddress",grpcAddress);

const getClient = () => new firehoseStream(
  grpcAddress,
  process.env.GRPC_INSECURE=='true' ? grpc.credentials.createInsecure(): grpc.credentials.createSsl(),{
    "grpc.max_receive_message_length": 1024 * 1024 * 100,
    "grpc.max_send_message_length": 1024 * 1024 * 100,
  }
);
const toHex = base64 => Buffer.from(base64, 'base64').toString("hex");

const streamFirehose = forceStartBlock => new Promise( async (resolve, reject)=>{
  const {blocksDB, rootDB, statusDB} = getDB();

  let { lastBlock: start_block_num, lib } = await getRange(); //set start_block_num to last indexed block
  if (lib) start_block_num = lib;  //if lib is recorded, set start_block_num to lib

  console.log("Lib is at " + lib)
  
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
      else { //if STEP_NEW
        let date = (new Date(parseInt(block.header.timestamp.seconds)*1000)).toISOString().replace('Z', '');
        if (block.header.timestamp.nanos) date = date.replace('000', '500')
        statusDB.put("lastBlockTimestamp", date); 
      }
      const blockMerkle = block.blockrootMerkle;
      blockMerkle.activeNodes.forEach((node,index) => blockMerkle.activeNodes[index] = toHex(node) );
      var buffer = serialize(block.id, blockMerkle.activeNodes);
      blocksDB.put(block.number, asBinary(buffer));
    });
  }

});

module.exports = {
  streamFirehose,
  sleep
}