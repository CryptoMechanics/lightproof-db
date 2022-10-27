
const url = require('url');
const http = require('http');
const { getDB, getRange, deserialize } = require("./db");

const PORT = process.env.PORT || 8285;

async function startApi(){
  return new Promise(resolve=>{
    let { blocksDB } = getDB();

    const server = http.createServer(async (req, res) => {
      if (req.url.startsWith("/?blocks=") && req.method === "GET") {
        try{
          const blocksNumbers = url.parse(req.url, true).query.blocks.split(',').map(r=>parseInt(r));
          let ids = [];
          //convert to transaction
          for (var blockNum of blocksNumbers) {
            let nodesBuffer = await blocksDB.getBinary(blockNum);
            if (!nodesBuffer) continue;
            const result = await deserialize(nodesBuffer);
            ids.push({...result, num: blockNum});
          }

          res.writeHead(200, { "Content-Type": "application/json" });
          res.write(JSON.stringify(ids));
          res.end();
        }catch(ex){
          console.log("ex",ex)
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ message: "Error fetching blocks, contact admin" }));
        }
      }
      else if (req.url.startsWith("/status") && req.method === "GET") {
        const range = await getRange();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.write(JSON.stringify(range));
        res.end();
      }
      else {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ message: "Unsupported request" }));
      }
    });
    server.listen(PORT, ()=>{
      console.log("\nRest Server started at port ", PORT);
      resolve();
    })
  })
}

module.exports = {
  startApi
}