const crypto = require("crypto");

const make_canonical_pair = (l,r) => ({ l: maskLeft(l), r: maskRight(r) });

function append(digest, _active_nodes, node_count, stop_at_depth = -1) {
  var partial = false;
  var max_depth = calculate_max_depth(node_count + 1);
  // var implied_count = next_power_of_2(node_count);

  var current_depth = stop_at_depth == -1 ? max_depth - 1 : stop_at_depth;
  var index = node_count;
  var top = digest;

  var count = 0;
  var updated_active_nodes = [];

  while (current_depth > 0) {

    if (!(index & 0x1)) {
      if (!partial) updated_active_nodes.push(top);
      top = hashPair(make_canonical_pair(top, top));
      partial = true;
    } 

    else {
      var left_value = _active_nodes[count];
      count++;
      if (partial)  updated_active_nodes.push(left_value);
      top = hashPair(make_canonical_pair(left_value, top));
     }

     current_depth--;
     index = index >> 1;
  }

  updated_active_nodes.push(top);

  return { 
    nodes: updated_active_nodes, 
    root: top
  };

}

function hashPair(p){
  var buffLeft = Buffer.from(p.l, "hex")
  var buffRight = Buffer.from(p.r, "hex")

  var buffFinal = Buffer.concat([buffLeft, buffRight]);
  var finalHash = crypto.createHash("sha256").update(buffFinal).digest("hex");

  return finalHash;
}
function next_power_of_2( value) {
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

function clz_power_2( value) {
  var count = 1;

  for (var i = 0; i < 30; i++){
    count*=2;
    if (value == count) return i+1;
  }

  return 0;
}


function calculate_max_depth( node_count) {
  if (node_count == 0) return 0;
  var implied_count = next_power_of_2(node_count);
  return clz_power_2(implied_count) + 1;
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
  append
}
