CREATE KEYSPACE insight WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

USE insight;
 
CREATE TABLE recommendationlist (userid text PRIMARY KEY, rlist text);

CREATE TABLE realtimeaction (action int, count int, timestamp timestamp, PRIMARY KEY(action, count)) WITH CLUSTERING ORDER BY (timestamp DESC, count DESC);
        
CREATE TABLE realtimestate (state int, count int, timestamp timestamp, PRIMARY KEY(state, count)) WITH CLUSTERING ORDER BY (timestamp DESC, count DESC);

CREATE TABLE realtimestateproduct (state int, pid text, count int, timestamp timestamp, PRIMARY KEY(state, count)) WITH CLUSTERING ORDER BY (state DESC, count DESC);

CREATE TABLE realtimeage (age int, count int, timestamp timestamp, PRIMARY KEY(age,count)) WITH CLUSTERING ORDER BY (timestamp DESC, count DESC);

CREATE TABLE popularproduct (pid text, count int, timestamp timestamp, PRIMARY KEY(pid, timestamp)) WITH CLUSTERING ORDER BY (count DESC);


