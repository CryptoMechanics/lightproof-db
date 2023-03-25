# lightproof-db

Lightproof-db for antelope stores the block id, number and active nodes for each block. It exposes the data through an http GET server.
Currently regular nodeos, firehose and SHIP are supported for consuming blocks and extracting the data for the lmdb database tables.

## Instructions

#### Clone the repo and install dependencies
```
sudo apt install build-essential
git clone https://github.com/CryptoMechanics/lightproof-db.git
cd lightproof-db
npm install
```


#### Configuration

- `cp .env.example .env`

- edit the `.env` file variables
```
# host port to use for http (consumed by ibc-proof-server)
PORT=8285      

# Used to get required blocks for bootstapping lightproof-db from a full firehose
BOOTSTRAP=true
BOOT_GRPC_ADDRESS=eos.firehose.eosnation.io:9000
BOOT_GRPC_INSECURE=false

# block to bootstrap until, must be irreversible and HISTORY_PROVIDER must have the blocks after it
START_SYNC_HEIGHT=293690288   

# firehose or ship
HISTORY_PROVIDER=firehose

# Firehose GRPC address and mode (if HISTORY_PROVIDER is firehose)
GRPC_ADDRESS=eos.firehose.eosnation.io:9000
GRPC_INSECURE=false

# SHIP websocket address (if HISTORY_PROVIDER is SHIP)
SHIP_WS=ws://localhost:8080

# Nodeos HTTP (if HISTORY_PROVIDER is nodeos)
NODEOS_HTTP=http://localhost:8888

# EXPERIMENTAL. Automatically prune lightproof-db records that are PRUNING_CUTOFF blocks from last written block to the db
PRUNING_CUTOFF=0

# FORCE_START_BLOCK=BLOCK_NUMBER  #force indexing to start from a certain block
# DB_PATH="lightproof-data"
# FIREHOSE_SERVICE=v2
```


#### Run
```
node index.js
```

#### API
```
http://localhost:8285/status
http://localhost:8285/?blocks=20,21,22
```
