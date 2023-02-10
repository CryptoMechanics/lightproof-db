const crypto = require("crypto");

const makeCanonicalPair = (l,r) => ({ l: maskLeft(l), r: maskRight(r) });

function append(digest, activeNodes, nodeCount, stopAtDepth = -1) {

  const maxDepth = calculateMaxDepth(nodeCount + 1);
  const currentDepth = stopAtDepth == -1 ? maxDepth - 1 : stopAtDepth;

  const count = 0;
  const nodes = [];
  let partial = false;
  let index = nodeCount;
  let top = digest;

  while (currentDepth > 0) {

    if (!(index & 0x1)) {
      if (!partial) nodes.push(top);
      top = hashPair(makeCanonicalPair(top, top));
      partial = true;
    } 
    else {
      var left_value = activeNodes[count];
      count++;
      if (partial) nodes.push(left_value);
      top = hashPair(makeCanonicalPair(left_value, top));
    }

     currentDepth--;
     index = index >> 1;
  }

  nodes.push(top);
  return { nodes, root: top };
}

function hashPair(p){
  var buffLeft = Buffer.from(p.l, "hex")
  var buffRight = Buffer.from(p.r, "hex")

  var buffFinal = Buffer.concat([buffLeft, buffRight]);
  var finalHash = crypto.createHash("sha256").update(buffFinal).digest("hex");

  return finalHash;
}

function annotateIncrementalMerkleTree(tree, log){

  var npo2 = nextPowerOf2(tree.nodeCount);
  var ppo2 = prevPowerOf2(tree.nodeCount+1);
  
  if (log){
    console.log("\nAnnotating tree for block : ", tree.nodeCount);
    console.log("       -> next power of 2 : ", npo2);
    console.log("       -> prev power of 2 : ", ppo2);
    console.log("")
  }

  var accountedForLower = 0;
  var accountedForUpper = 0;
  let rootIntegers;
  let blocksRequired = [];
  let blockToEdit;
  for (var i = tree.activeNodes.length-1; i>=0; i--){
    tree.activeNodes[i] = { node : tree.activeNodes[i] };

    if (i == tree.activeNodes.length-1 ){
      if(log) console.log("  node ", tree.activeNodes[i].node, "root", "  po2 : ", npo2);
      rootIntegers = npo2.toString().length;
      if (tree.activeNodes.length == 1) updateActiveNode(i)
    }
    else {
      if (accountedForUpper+ppo2 >= tree.nodeCount+1){
        do {
          if(log) console.log("  no node ", " ".repeat(68), "po2 : ", ppo2);
          ppo2/=2;
        } while (accountedForUpper+ppo2 >= tree.nodeCount+1);

        updateActiveNode(i, "     partial    ");
      }
      else updateActiveNode(i, " fully realized ");
    }
  }

  function updateActiveNode(i, type){
    accountedForLower = accountedForUpper + 1;
    accountedForUpper+= ppo2;
    ppo2/= 2;

    const po2 = accountedForUpper - accountedForLower + 1;
    tree.activeNodes[i].accountedForLower = accountedForLower;
    tree.activeNodes[i].accountedForUpper = accountedForUpper;
    tree.activeNodes[i].po2 = type ? po2 : ppo2;

    if (type){
      let aliveUntil; 
      tree.activeNodes[i].lifetime = accountedForUpper + po2;
      tree.activeNodes[i].ttl = tree.activeNodes[i].lifetime - tree.nodeCount;
      if (po2>1) {
        aliveUntil =  accountedForUpper + po2;
        blocksRequired.push({blockNum: accountedForUpper +1, aliveUntil });
        if(log)  console.log("  node ", tree.activeNodes[i].node," ".repeat(5), " po2 : ", po2, " ".repeat(rootIntegers - po2.toString().length), "alive until", aliveUntil, "ttl", tree.activeNodes[i].ttl," ".repeat(rootIntegers - tree.activeNodes[i].ttl.toString().length), "(" + type +")", "accounts for leaves : ", accountedForLower, " to ", accountedForUpper);
      }
      else {
        aliveUntil =   accountedForLower+1;
        if(log) console.log("  node ", tree.activeNodes[i].node," ".repeat(5), " po2 : ", 1, " ".repeat(rootIntegers-1), "alive until",aliveUntil, "ttl", 1," ".repeat(rootIntegers-1), "(" + type +")","accounts for leaves : ", accountedForLower);
      }
      blockToEdit = {blockNum: tree.nodeCount, aliveUntil }
    }
  }

  return { blocksRequired, blockToEdit };
}

function nextPowerOf2(value) {
  value -= 1;
  value |= value >> 1;
  value |= value >> 2;
  value |= value >> 4;
  value |= value >> 8;
  value |= value >> 16;
  value |= value >> 32;
  value += 1;   
  return value;
}

function prevPowerOf2(value) {
  value -= 1;
  value |= (value >> 1);
  value |= (value >> 2);
  value |= (value >> 4);
  value |= (value >> 8);
  value |= (value >> 16);
  value |= (value >> 32);
  return value - (value >> 1);
}

function clzPower2( value) {
  var count = 1;

  for (var i = 0; i < 30; i++){
    count*=2;
    if (value == count) return i+1;
  }

  return 0;
}

function calculateMaxDepth( nodeCount) {
  if (nodeCount == 0) return 0;
  var impliedCount = nextPowerOf2(nodeCount);
  return clzPower2(impliedCount) + 1;
}

function maskLeft(n){
  var nn = Buffer.from(n, "hex");
  nn[0] &= 0x7f;
  if (nn[0] < 0) nn[0] += (1 << 30) * 4;
  return nn;
}

function maskRight(n){
  var nn = Buffer.from(n, "hex");
  nn[0] |= 0x80;
  if (nn[0] < 0) nn[0] += (1 << 30) * 4;
  return nn;
}

module.exports = {
  append,
  annotateIncrementalMerkleTree
}
