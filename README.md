# XE Data Engineer Challenge

This is my solution to the Data Engineer - XE Code Challenge

### Notes

* The rationale behind the script pushing Kafka messages to MySQL is a 2-mode operation:
	* On times with low activity, check every 10 seconds if there is data on the buffer, then commit
	* On times with high activity, commit as soon as the buffer reaches 100 messages

* Duplicate messages are handled by “INSERT IGNORE INTO Classifieds”, where the “id” (message id) column is the PRIMARY KEY. The length of the associated index has been optimized for fast inserts, by using the binary string value (BINARY (16)) of the hexadecimal id instead of using a multibyte charset string data type


### Prerequisites

The application dependencies are the following:

- python=3.7
- mysql-connector-python
- kafka-python

An 'environment.yml' file has been included and can be used as shown below, in order to create a new environment

```
conda env create -f environment.yml
```
```
conda activate XE_DataEngineerChallenge
```

### Running

```
python main.py
```

The -v argument can be provided for increased output verbosity

```
python main.py -v
```

### Running the tests

The following command can be used for running the unit tests

```
python test_kafka_2_mysql.py
```

### SQL Stored Procedure

The necessary code and comments for setting up the stored procedure in MySQL can be found in the following file

```
stored_procedure.sql
```

## Authors

* **Fotis Ntouskas**


## Acknowledgments

* Kudos to the XE team for their time in preparing this interesting and fun challenge, thank you!
