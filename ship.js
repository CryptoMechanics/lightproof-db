const WebSocket = require('ws');
const { Serialize } = require('enf-eosjs');
const { getDB, getStartBlock, serialize, deserialize, handleHashesDB, pruneDB } = require("./db");
const { append, annotateIncrementalMerkleTree } = require("./functions");
const { blocksDB, rootDB, statusDB } = getDB();
const { asBinary } = require('lmdb');

class SHIP {
  constructor() {
    this.abi = null;
    this.types = null;
    this.blocksQueue = [];
    this.inProcessBlocks = false;
    this.currentArgs = null;
    this.connectionRetries = 0;
    this.maxConnectionRetries = 100;
  }

  start(endpoint){

    console.log(`Websocket connecting to ${endpoint}`);
    this.ws = new WebSocket(endpoint, { perMessageDeflate: false });
    this.ws.on('message', data =>{
      //if abi is not set, it means we are receiving an abi
      if (!this.abi) {
          this.rawabi = data;
          this.abi = JSON.parse(data);
          this.types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), this.abi);
          //request ship status
          return this.send(['get_status_request_v0', {}]);
      }
      //Receiving blocks
      const [type, response] = this.deserialize('result', data);
      this[type](response);
    });
    this.ws.on('close', code => {
      console.error(`Websocket disconnected from ${process.env.SHIP_WS} with code ${code}`);
      this.abi = null;
      this.types = null;
      this.blocksQueue = [];
      this.inProcessBlocks = false;
      if (code !== 1000) this.reconnect();
    });
    this.ws.on('error', (e) => {console.error(`Websocket error`, e)});
  }

  disconnect() {
      console.log(`Closing connection`);
      this.ws.close();
  }

  reconnect(){
    if (this.connectionRetries > this.maxConnectionRetries) return console.error(`Exceeded max reconnection attempts of ${this.maxConnectionRetries}`);
    const timeout = Math.pow(2, this.connectionRetries/5) * 1000;
    console.log(`Retrying with delay of ${timeout / 1000}s`);
    setTimeout(() => { this.start(process.env.SHIP_WS); }, timeout);
    this.connectionRetries++;
  }

  serialize(type, value) {
    const buffer = new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder });
    Serialize.getType(this.types, type).serialize(buffer, value);
    return buffer.asUint8Array();
  }

  deserialize(type, array) {
    const buffer = new Serialize.SerialBuffer({ textEncoder: new TextEncoder, textDecoder: new TextDecoder, array });
    return Serialize.getType(this.types, type).deserialize(buffer, new Serialize.SerializerState({ bytesAsUint8Array: true }));
  }

  send(request) {
    this.ws.send(this.serialize('request', request));
  }

  requestBlocks(requestArgs) {
    if (!this.currentArgs) this.currentArgs = {
      start_block_num: 0,
      end_block_num: 0xffffffff,
      max_messages_in_flight: 50,
      have_positions: [],
      irreversible_only: false,
      fetch_block: false,
      fetch_traces: false,
      fetch_deltas: false,
      ...requestArgs
    };
    this.send(['get_blocks_request_v0', this.currentArgs]);
  }

  async get_status_result_v0(response) {
    const start_block_num = await getStartBlock();
    console.log("SHIP Lib is at ", response.last_irreversible.block_num);
    console.log("Db starting to sync from",start_block_num)
    this.requestBlocks({ start_block_num, irreversible_only:false })
  }

  get_blocks_result_v0(response) {
    this.blocksQueue.push(response);
    this.processBlocks();
  }

  async processBlocks() {
    if (this.inProcessBlocks) return;
    this.inProcessBlocks = true;
    while (this.blocksQueue.length) {
      let response = this.blocksQueue.shift();
      if (response.this_block){
        let block_num = response.this_block.block_num;
        this.currentArgs.start_block_num = block_num - 50; // replay 25 seconds
      }
      this.send(['get_blocks_ack_request_v0', { num_messages: 1 }]);
      await this.receivedBlock(response);
    }
    this.inProcessBlocks = false;
  }


  async receivedBlock(response) {
    if (!response.this_block) return;
    let block_num = response.this_block.block_num;
    let block_id = response.this_block.block_id;

    //handle forks
    let blockExists = blocksDB.getBinary(block_num);
    if (blockExists){
      console.log(`overwriting block #${block_num} in lightproof-db`);
      let existingBlock
      try{ existingBlock = await deserialize(blockExists); }
      catch(ex){ console.log("ex",ex, blockExists); } 
      //remove the hash if not used in another block, or reduce its instance count by 1 to roll back the block
      if(existingBlock) for (var node of existingBlock.nodes) await handleHashesDB(node);
    }
    // }
    this.current_block = block_num;

    if (!(block_num % 1000)){
      const progress = (100 * ((this.current_block) / response.head.block_num)).toFixed(2)
      console.log(`SHIP: ${progress}% (${this.current_block}/${response.head.block_num})`);
      await pruneDB();
    }

    await rootDB.transaction(async () => {
      
      let nodes, aliveUntil=0;
      
      //if starting form genesis
      if(block_num==1) nodes = [];

      //if lightproof-db stored the previous block or starting from a snapshot
      else {
        let previousBuffer = blocksDB.getBinary(block_num-1);
        //if starting from snapshot 
        if(!previousBuffer){
          console.log(`Cannot find previous block #${block_num-1} in lightproof-db`);
          process.exit()
        } 
        //if lightproof-db has the previous block nodes
        let previous = await deserialize(previousBuffer);

        const previousNodeCount = block_num-2
        const merkleTree = append(previous.id, previous.nodes, previousNodeCount);
        nodes = merkleTree.activeNodes; 
        const tree = {...merkleTree, nodeCount: block_num-1}
        const { blockToEdit } = annotateIncrementalMerkleTree(JSON.parse(JSON.stringify(tree)), false); 
        aliveUntil = blockToEdit.aliveUntil;
      }

      var buffer = serialize(block_id, nodes, aliveUntil);
      blocksDB.put(block_num, asBinary(buffer));

      if (this.currentArgs.irreversible_only) statusDB.put("lib", block_num);
      else statusDB.put("lib", block_num - 600);
    });

    if (this.current_block === this.end_block-1){
      console.log(`SHIP done streaming`)
      this.disconnect()
    }

  } 
} 


module.exports = SHIP