## Data analysis sqlite3 based

### Dependencies
* pandas
* sqlite3
* pika
* requests
* RabitMQ server

### Run an analysis
Download and unzip the repository

From the repository folder run in two terminals,

### First run example: 

##### Terminal 1:
```
python recieve.py
```
##### Terminal 2:
```
python send.py usa 2010 http://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip
```
While the reciever terminal is still consuming, you can send different properties for a new analysis, like a different country or year
##### From sender terminal
```
python send.py brazil 2009
```

Reply to the sender(RPC) is out of scope!
