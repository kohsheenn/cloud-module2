#  OrderFlow Simulator

An interactive **Distributed Systems Simulator** that visually demonstrates how modern backend systems handle real-world challenges like failures, retries, and scalability.
---
##  Project Structure
```
├── backend/
│   └── cloud.py          # Python simulation (optional)
│
├── frontend/
│   └── index.html        # Main interactive simulator (RUN THIS)
│
├── requirements.txt
└── README.md
```
---
##  How to Run
###  Run the Project (Main UI)
No setup required.
1. Open the file:
```
frontend/index.html
```
2. Double click OR open in browser
This launches the full interactive simulator.
---
### Run Backend Simulation (Optional)
If you want terminal-based logs:
```
cd backend
python cloud.py
```
---
##  What This Project Demonstrates

* Priority Queues (VIP orders)
* Rate Limiting
* Idempotency (duplicate prevention)
* Circuit Breaker
* Retry with Exponential Backoff
* Saga Pattern (distributed transactions)
* Dead Letter Queue (DLQ)
* Event Replay
* Fan-out (one event → multiple services)
* Graceful Shutdown
---
##  How It Works
### Frontend (Main Simulator)
* Visual pipeline of system components
* Real-time logs
* Step-by-step scenario execution
* 12 predefined system scenarios
### Backend (cloud.py)
* Simulates actual system behavior
* Implements core distributed system patterns
---
##  Scenarios Included

1. Normal Order
2. VIP Priority Order
3. Duplicate Order Handling
4. Invalid Input
5. Rate Limiting
6. Saga Rollback
7. Broker Failure (Circuit Breaker)
8. Dead Letter Queue
9. Event Replay
10. Fan-out Dispatch
11. Graceful Shutdown
12. System Summary
---
##  Tech Stack
* Frontend: HTML, CSS, JavaScript (Vanilla)
* Backend: Python (Standard Library Only)
---
##  Key Highlight
Zero dependencies required to run the project UI.
Just open `index.html` and you're good to go.
---
##  Use Cases

* Distributed Systems learning
* System Design interviews
* Academic projects & presentations
* Backend architecture visualization


##  License

For educational purposes.
