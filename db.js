const { Serialize } = require("enf-eosjs")
const types = Serialize.createInitialTypes();
const { open, asBinary } = require('lmdb');

const varuint32 = types.get("varuint32");
const uint32 = types.get("uint32"); 
const uint64 = types.get("uint64"); 
const checksum256 = types.get("checksum256"); 

let rootDB,blocksDB, hashesDB, hashIndexDB, statusDB;
let dbPath = process.env.DB_PATH || 'lightproof-data'
const getDB = () => {
  if(!rootDB) {
    rootDB = open({ path: dbPath, compression: true });
    blocksDB = rootDB.openDB({name:"blocksDB"});
    hashesDB = rootDB.openDB({name:"hashesDB" });
    hashIndexDB = rootDB.openDB({name:"hashIndexDB" });
    statusDB = rootDB.openDB({name:"statusDB" });
  }
  return { rootDB, blocksDB, hashesDB, hashIndexDB, statusDB }
}

const getStartBlock = async () => {

  let { lib: startBlock } = await getRange(); 
    
  console.log("Lib is at " + startBlock);
  if(startBlock) startBlock++;
  //set startBlock to ENV FORCE_START_BLOCK
  const forceStartBlock = process.env.FORCE_START_BLOCK;
  if (forceStartBlock) {
    console.log("DB forced to start from "+ forceStartBlock);
    startBlock = forceStartBlock
  }
  // start at block 1, if lib is undefined and forceStartBlock is not provided
  if (!startBlock) startBlock=1;
  return startBlock
}

function deserialize(array){
  const buffer = new Serialize.SerialBuffer({ TextEncoder, TextDecoder, array });
  var id = Buffer.from(buffer.getUint8Array(32)).toString("hex");
  var count = buffer.getVaruint32();
  var nodes = [], nodesCount=[];
  for (var i = 0 ; i < count; i++){
    var index = buffer.getUint32();
    var hashBuff = hashesDB.getBinary(index);
    let record = JSON.parse(hashBuff.toString());
    nodes.push(record.node);
    nodesCount.push(record.count)
  }
  //Additions for aliveUntil
  const aliveUntil = buffer.getUint64AsNumber();
  return {id, nodes, aliveUntil, nodesCount} ;
}
  
function serialize(id, nodes, aliveUntil=0, addHashesCount = true){
  var mappedNodes = map(nodes, addHashesCount);

  const buffer = new Serialize.SerialBuffer({ TextEncoder, TextDecoder });
  checksum256.serialize(buffer, id);
  varuint32.serialize(buffer, mappedNodes.length);
  for (var node of mappedNodes) uint32.serialize(buffer, node);

  //Additions for aliveUntil
  uint64.serialize(buffer, aliveUntil)

  return buffer.asUint8Array();
}

function map(nodes, addHashesCount){
  if (!nodes || !nodes.length) return [];
  //  TODO turn into an atomic transaction
  //  TODO hashes count is last key of hashesDB + 1
	var map = [];
	for (var i = 0 ; i < nodes.length ;i++){
    var buffNode = Buffer.from(nodes[i], "hex");
		var index = hashIndexDB.get(buffNode);
		if (!index) {
      let hashesCount = 0;
      for (let key of hashesDB.getKeys({ limit:1, reverse:1 })) hashesCount = key + 1;
      hashIndexDB.putSync(buffNode, hashesCount);

      let record = { node: nodes[i], count: 1 };
      var buf = Buffer.from(JSON.stringify(record));

      hashesDB.putSync( hashesCount, asBinary(buf));
			map.push(hashesCount);
		}
		else {
      //increment count of node in hashesDB
      if (addHashesCount){
        const hashesDBBuffer = hashesDB.getBinary(index);
        let record = JSON.parse(hashesDBBuffer.toString());
        let editedRecord = { node: record.node, count: record.count+1 };
        var newBuffer = Buffer.from(JSON.stringify(editedRecord));
        hashesDB.putSync(index, asBinary(newBuffer));
      }
      //push index that is already assigned to the node
      map.push(index);
    }
	}
	return map;
}



const getRange = async () =>{
  //  TODO turn into an atomic transaction
  let firstBlock, lastBlock;
  for (let key of await blocksDB.getKeys({ limit:1 })) firstBlock = key;
  for (let key of await blocksDB.getKeys({ limit:1, reverse:1})) lastBlock = key;
  const lib = await statusDB.get("lib");
  const lastBlockTimestamp = await statusDB.get("lastBlockTimestamp");
  return {firstBlock, lastBlock, lib, lastBlockTimestamp}
}

const pruneDB = async () => {
  const cuttoff = process.env.PRUNING_CUTOFF || 86400; //default to 12hr cuttoff
  let { lastBlock } = await getRange(); 
  const pruneMaxBlock = lastBlock - parseInt(cuttoff) ;
  console.log("\n###########################################################################\n")
  console.log("Pruning database at a max block of",pruneMaxBlock, `(-${cuttoff} from head)`)

  let prunedRecords = 0;
  let deletedNodes = 0;
  let deletedRecords = 0; 
  //iterate over all blocksDB, 
  return rootDB.transaction(async () => {

    for (let key of await blocksDB.getKeys({end:pruneMaxBlock })) {
      
      let nodesBuffer = blocksDB.getBinary(key);
      if (!nodesBuffer) continue;
      let result = deserialize(nodesBuffer);

      if (result.aliveUntil && result.aliveUntil < pruneMaxBlock){
        
        //remove from blocksDb
        blocksDB.remove(key); 
        deletedRecords++;

        //handle the nodes to be removed from this block
        for (var i=0;i<result.nodes.length;i++) handleHashesDB(result.nodes[i])

      }
      else if (key < pruneMaxBlock && result.nodes.length>1){

        //handle the nodes to be removed from this block
        for (var i=1;i<result.nodes.length;i++) handleHashesDB(result.nodes[i])

        //update block from blocksDB with aliveUntil value
        const editedBuffer = serialize(result.id, [result.nodes[0]], result.aliveUntil);
        blocksDB.put(key, asBinary(editedBuffer));
        prunedRecords++;
        deletedNodes+=result.nodes.length - 1;

      }
    }

    console.log("\nFinsihed pruning:\n")
    console.log("Records deleted:",deletedRecords)
    console.log("Records pruned:",prunedRecords)
    console.log("Nodes removed:",deletedNodes)
    console.log("\n###########################################################################\n");
  });


}

async function handleHashesDB(node){
  var buffNode = Buffer.from(node, "hex");
  var index = hashIndexDB.get(buffNode);

  //get hashesDB record of node using the index
  const hashesDBBuffer = await hashesDB.getBinary(index);
  let record = JSON.parse(hashesDBBuffer.toString());
  if (record.count === 1){
    //delete the index and the node if its not used in any other block
    hashesDB.removeSync(index);
    hashIndexDB.removeSync(buffNode)
  }
  else if (record.count>1){
    //decrement the count of the node in hashesDB
    let editedRecord = { node: record.node, count: record.count-1 };
    var newBuffer = Buffer.from(JSON.stringify(editedRecord));
    hashesDB.putSync(index, asBinary(newBuffer));
  }
}
module.exports = {
  getDB,
  getRange,
  serialize,
  deserialize,
  getStartBlock,
  pruneDB,
  handleHashesDB
}