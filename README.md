## Data analysis sqlite3 based

### Dependencies
* pandas
* sqlite3
* pika
* requests
* RabitMQ server

### Run an analysis
from the repository run in two terminals 
first run example: 
```
terminal 1 - python send.py usa 2010 www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip
terminal 2 - python recieve.py
```
while the reciever terminal is still open, you can send diffrent properties for an analysis
```
python send.py brazil 2009
```

Reply to the sender is out of scope!
