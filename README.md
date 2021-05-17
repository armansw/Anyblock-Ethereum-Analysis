# anyblock_ethereum
Calculate LTH/STH shares from Anyblock.com ethereum mainnet db.

## Main function
* dumping [anyblock.net](https://www.anyblockanalytics.com/)'s ethereum mainnet database
* analyze Long-Term Holding and Short-Term Holding wallets
* visualize the history of LTH/STH share

## Tested Platform
* Ubuntu 20.04
* python 3.9

## Work flow
### Virtual environment
> $ cd {project folder}

> $ python3 -m venv venv

> $ source venv/bin/activate

> (venv)$ pip3 install -r requirements.txt

### Configuration
In the configuration.py, you can modify parameters.
Especially, you should modify the ANY_USER and ANY_PASSWORD parameters with the values you got from the anyblock.net site.

#### Local postgre SQL setting
In order to calculate and analyse, you should install PostgreSQL Server on your computer.
When we use only SQLite, it would be better for deploy. But as the default sqlite engine doesn't support embedded Math functions, we moved to PostgreSQL.
> $ sudo -u postgres psql

> postgres=# create database {db_name};

> postgres=# create user {user_name} with encrypted password '{password}';

> postgres=# grant all privileges on database {db_name} to {user_name};

> postgres=# \q

Those db_name, user_name, and password should be stored in the configuration.py. 

### Dumping 
> (venv)$ python3 do_cache.py

This script shows the progress of dumping. Every day's data is dumped in a single sqlite file. Cache files are stores in the dbs/ folder.

When the dumping is done till yesterday's data, it automatically finishes. 

Or you can press Ctrl+C key to pause it. Then it finishes the current chunk and quits.

### Calculating
> (venv)$ python3 do_calc.py

This script reads the cached db files and processes it in the local PostgreSQL database.

### Visualization
> (venv)$ python3 do_anal.py

It launches your web browser, and you can see some graphs there. 
