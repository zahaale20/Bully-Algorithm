# Bully Algorithm Leader Election

A Python implementation of the Bully Algorithm for distributed leader election with failure detection and recovery.

## Overview

This project implements the classic Bully Algorithm, which enables a group of distributed processes to elect a leader. When the current leader fails, the algorithm ensures that the process with the highest ID becomes the new leader. The implementation includes a Group Coordination Daemon (GCD) for membership management and periodic health checks (PROBE messages) for failure detection.

## Architecture

The system consists of two main components:

1. **Node (`node.py`)**: Individual process participating in the election
2. **GCD (`gcd.py`)**: Group Coordination Daemon that maintains membership list

### Message Types

- `HOWDY`: Join request sent to GCD
- `ELECT`: Election message sent to higher-priority nodes
- `GOT_IT`: Response acknowledging an ELECT message
- `I_AM_LEADER`: Leadership announcement broadcast
- `PROBE`: Health check sent to current leader

## Usage

### Start the GCD
```bash
python gcd.py 
```

### Start a Node
```bash
python node.py GCD_HOST GCD_PORT MONTH YEAR SU_ID
```

### Example
```bash
# Terminal 1 - Start GCD
python gcd.py 5000

# Terminal 2 - Start Node 1
python node.py localhost 5000 3 15 1234567

# Terminal 3 - Start Node 2
python node.py localhost 5000 6 20 2345678

# Terminal 4 - Start Node 3
python node.py localhost 5000 9 10 3456789
```
