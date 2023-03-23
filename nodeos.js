
const { getDB, getStartBlock, serialize, deserialize, handleHashesDB, pruneDB } = require("./db");
const { append, annotateIncrementalMerkleTree } = require("./functions");
const { blocksDB, rootDB, statusDB } = getDB();
const { asBinary } = require('lmdb');
const axios = require('axios');
const sleep = s => new Promise(resolve=>setTimeout(resolve,s*1000));

let batchBlocks = 800;
let head;
const startNodeos = async () => {

  //catch up to lib
  await catchUpToLib();

  //handle reversible blocks
  const start_block_num = await getStartBlock();
  console.log("start_block_num",start_block_num);

  let currentBlock = start_block_num;
  let previousBlockId;

  if(currentBlock > 1) {
    let previousBuffer = await blocksDB.getBinary(currentBlock-1);
    if(!previousBuffer){
      console.log(`Cannot find previous block #${currentBlock-1} in lightproof-db`);
      process.exit()
    } 
    let previous = await deserialize(previousBuffer);
    previousBlockId = previous.id;
    console.log("previousBlockId",previousBlockId)
  }
  while (true){
    let block;
    while (!block){
      try{ block = (await fetchBlockInfo(currentBlock)).data; }
      catch(ex){ await sleep(0.5) }
    }
    if(block.previous === previousBlockId){
      await processBlock(block);
      previousBlockId = block.id;
      currentBlock++;
    }
    else {
      console.log("fork detected, block previous doesnt match the previous blocks id")
      currentBlock--;
      let previousBuffer = await blocksDB.getBinary(currentBlock-1);
      if(!previousBuffer){
        console.log(`Cannot find previous block #${currentBlock-1} in lightproof-db`);
        process.exit()
      } 
      let previous = await deserialize(previousBuffer);
      previousBlockId = previous.id;
    }

  }

}

const catchUpToLib = async () =>{
  head = (await fetchHeadInfo()).data;
  let nodeosLib = head.last_irreversible_block_num;
  const start_block_num = await getStartBlock();
  console.log("start_block_num",start_block_num);
  console.log("nodeosLib",nodeosLib);

  if (nodeosLib <=  start_block_num) return;
  let currentBlock = start_block_num;
  console.time("start");

  //handle irreeversible blocks in bulk
  while (currentBlock+batchBlocks < nodeosLib){
    
    let promises = [];
    for (var i=0;i<batchBlocks;i++) promises.push(fetchBlockInfo(currentBlock + i))
    
    let blockLot;
    while (!blockLot){
      try{ blockLot = (await Promise.all(promises)).map(r=>r.data); }
      catch(ex){ await sleep(5) }
    }
    
    await processIrreversibleLot(blockLot);
    currentBlock+=batchBlocks;
  }

  //handle remaining irreeversible blocks (<batchBlocks)
  let promises = [];
  for (var i=0;i<nodeosLib-currentBlock + 1;i++) promises.push(fetchBlockInfo(currentBlock + i));
  
  let blockLot;
  while (!blockLot){
      try{ blockLot = (await Promise.all(promises)).map(r=>r.data); }
      catch(ex){await sleep(5)}
  }

  await processIrreversibleLot(blockLot);
  currentBlock=nodeosLib;
  console.log("done catching up to lib")
  console.timeEnd("start");
  console.log("Total blocks processed",nodeosLib - start_block_num )
}

const processIrreversibleLot = async blockLot =>{
  if (!blockLot.length) return;
  return rootDB.transaction(async () => {
      // let block_num, block_id;
    for (var block of blockLot) {
      let block_num = block.block_num;
      let block_id = block.id;
  
      let blockExists = blocksDB.getBinary(block_num);
      if (blockExists){
        console.log(`overwriting block #${block_num} in lightproof-db`);
        let existingBlock
        try{ existingBlock = deserialize(blockExists); }
        catch(ex){ console.log("ex deserializing",ex, blockExists); } 
        //remove the hash if not used in another block, or reduce its instance count by 1 to roll back the block
        if(existingBlock) for (var node of existingBlock.nodes) await handleHashesDB(node);
      }
  
  
      if (!(block_num % 5000)){
        head = (await fetchHeadInfo()).data;
        const progress = (100 * ((block_num) / head.head_block_num)).toFixed(2)
        console.log(`NODEOS: ${progress}% (${block_num}/${head.head_block_num})`);
        await pruneDB();
      }
  
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
        let previous = deserialize(previousBuffer);
        const previousNodeCount = block_num-2;

        const merkleTree = append(previous.id, previous.nodes, previousNodeCount);
        nodes = merkleTree.activeNodes; 
        const tree = {...merkleTree, nodeCount: block_num-1};
        
        const { blockToEdit } = annotateIncrementalMerkleTree(JSON.parse(JSON.stringify(tree)), false); 
        if (blockToEdit) aliveUntil = blockToEdit.aliveUntil;
      }
  
      var buffer = serialize(block_id, nodes, aliveUntil);
      blocksDB.put(block_num, asBinary(buffer));

    }
    await statusDB.put("lib", blockLot[blockLot.length-1].block_num);
    console.log("finished",blockLot.length, blockLot[0].block_num,"->",blockLot[blockLot.length-1].block_num)
  });
}

const processBlock = async block =>{
  return rootDB.transaction(async () => {
    let block_num = block.block_num;
    let block_id = block.id;
    // console.log("Processing",block_num)

    let blockExists = blocksDB.getBinary(block_num);
    if (blockExists){
      console.log(`overwriting block #${block_num} in lightproof-db`);
      let existingBlock
      try{ existingBlock = deserialize(blockExists); }
      catch(ex){ console.log("ex deserializing",ex, blockExists); } 
      //remove the hash if not used in another block, or reduce its instance count by 1 to roll back the block
      if(existingBlock) for (var node of existingBlock.nodes) await handleHashesDB(node);
    }


    if (!(block_num % 5000)){
      head = (await fetchHeadInfo()).data;
      const progress = (100 * ((block_num) / head.head_block_num)).toFixed(2)
      console.log(`NODEOS: ${progress}% (${block_num}/${head.head_block_num})`);
      statusDB.put("lib", block.block_num - 600);
      await pruneDB();
    }

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
      let previous = deserialize(previousBuffer);
      const previousNodeCount = block_num-2
      const merkleTree = append(previous.id, previous.nodes, previousNodeCount);
      nodes = merkleTree.activeNodes; 
      const tree = {...merkleTree, nodeCount: block_num-1};

      const { blockToEdit } = annotateIncrementalMerkleTree(JSON.parse(JSON.stringify(tree)), false); 
      if (blockToEdit) aliveUntil = blockToEdit.aliveUntil;
    }

    var buffer = serialize(block_id, nodes, aliveUntil);
    blocksDB.put(block_num, asBinary(buffer));
  });

}


const fetchBlockInfo = block_num => axios.post(`${process.env.NODEOS_HTTP}/v1/chain/get_block_info`, JSON.stringify({block_num}));
const fetchHeadInfo = () => axios(`${process.env.NODEOS_HTTP}/v1/chain/get_info`);

module.exports = {
  startNodeos
}
