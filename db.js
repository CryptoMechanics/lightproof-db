
const { SerialBuffer, createInitialTypes } = require("eosjs/dist/eosjs-serialize");
const types = createInitialTypes();
const { open, asBinary } = require('lmdb');

const varuint32 = types.get("varuint32");
const uint32 = types.get("uint32"); 
const checksum256 = types.get("checksum256"); 

let rootDB,blocksDB, hashesDB, hashIndexDB, statusDB;
let dbPath = process.env.dbPath || 'lightproof-data'
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

function deserialize(array){
  const buffer = new SerialBuffer({ TextEncoder, TextDecoder, array });
  var id = Buffer.from(buffer.getUint8Array(32)).toString("hex");
  var count = buffer.getVaruint32();
  var nodes = [];
  for (var i = 0 ; i < count; i++){
    var index = buffer.getUint32();
    var hashBuff = hashesDB.getBinary(index);
    nodes.push(hashBuff.toString('hex'));
  }
  return {id, nodes} ;
}
  
function serialize(id, nodes){
  var mappedNodes = map(nodes);
  const buffer = new SerialBuffer({ TextEncoder, TextDecoder });
  checksum256.serialize(buffer, id);
  varuint32.serialize(buffer, mappedNodes.length);
  for (var node of mappedNodes) uint32.serialize(buffer, node);
  return buffer.asUint8Array();
}

function map(nodes){
  //TODO hashes count is last key of hashesDB + 1
	var map = [];
	for (var i = 0 ; i < nodes.length ;i++){
    var buffNode = Buffer.from(nodes[i], "hex");
		var index = hashIndexDB.get(buffNode);
		if (!index) {
      let hashesCount = 0;
      for (let key of hashesDB.getKeys({ limit:1, reverse:1 })) hashesCount = key + 1;
      hashIndexDB.putSync(buffNode, hashesCount);
      hashesDB.putSync( hashesCount, asBinary(buffNode));
			map.push(hashesCount);
			// hashesCount++;
		}
		else map.push(index);
	}
	return map;
}



const getRange = async () =>{
  let firstBlock, lastBlock;
  for (let key of await blocksDB.getKeys({ limit:1 })) firstBlock = key;
  for (let key of await blocksDB.getKeys({ limit:1, reverse:1})) lastBlock = key;
  const lib = await statusDB.get("lib");
  return {firstBlock, lastBlock, lib}
}

module.exports = {
  getDB,
  getRange,
  serialize,
  deserialize
}