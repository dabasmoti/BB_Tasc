## Data analysis sqlite3 based

### Dependencies
* pandas
* sqlite3
* pika
* requests
* RabitMQ server

### Run an analysis
Download and unzip the repository
from the repository folder run in two terminals,

first run example: 

#### Terminal 1:
```
python send.py usa 2010 http://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip
```
#### Terminal 2:
```
python recieve.py
```
while the reciever terminal is still open, you can send different properties for an analysis
```
python send.py brazil 2009
```

Reply to the sender(RPC) is out of scope!
