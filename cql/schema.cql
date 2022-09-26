// Create key space
CREATE KEYSPACE IF NOT EXISTS log_analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

// To use a keyspace
use log_analytics;

// Create tables

CREATE TABLE IF NOT EXISTS status_code(status int, hour int, date text, crawler text, count int, PRIMARY KEY(status, hour));

