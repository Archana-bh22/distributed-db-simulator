# Distributed Database Simulator

A Python-based simulator that models a **distributed database system** with multiple nodes, coordinated by a central controller.  
The project demonstrates **distributed systems concepts** such as coordination, state management, benchmarking, and performance evaluation.

---

## 📌 Features

- Simulates **multiple database nodes** (n1–n9)
- Central **controller** to coordinate nodes
- Supports **benchmarking** with configurable:
  - Number of transactions
  - Read/Write ratio
  - Consistency settings
  - Skew
- Tracks **performance metrics**
- Uses persistent **SQLite database files**
- Interactive command-based execution

---

## 🏗️ Project Structure

distributed-db-simulator/
├── controller.py # Central coordinator for all nodes
├── node.py # Logic for individual database nodes
├── common.py # Shared utilities and constants
├── run.py # Entry point to start the simulator
├── testcases.csv # Predefined test scenarios
├── db/ # Persistent storage for nodes
│ ├── n1_data.db
│ ├── n2_data.db
│ └── ...
└── README.md


---

## ⚙️ How It Works

1. `run.py` starts the controller
2. Controller initializes all nodes
3. Each node maintains its own database
4. User can run benchmarks or inspect system state
5. Results are collected and printed by the controller

---

## ▶️ How to Run

### Prerequisites
- Python 3.9+
- No external libraries required

### Run the simulator
```bash
python run.py

🧪 Available Commands

Once the system starts, you can use:
benchmark N RW CS SK – Run benchmark
PrintDB – Print all balances
Performance – Show performance metrics
(Press Enter) – Continue test execution

📊 Sample Output

[CONTROLLER] Controller started on 127.0.0.1:6000
[CONTROLLER] Loaded 10 test sets
[n1] State reset for new test set
...

🧠 Concepts Demonstrated

-Distributed systems fundamentals
-Node coordination
-State reset & isolation
-Performance benchmarking
-Fault-tolerant simulation design

## 👤 Project Ownership

This is a **solo project** designed and implemented independently to demonstrate backend and distributed systems concepts, including node coordination, benchmarking, and performance evaluation.
