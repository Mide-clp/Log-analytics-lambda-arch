// Create key space
CREATE KEYSPACE IF NOT EXISTS log_analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

// To use a keyspace
use log_analytics;

// Create tables

CREATE TABLE IF NOT EXISTS status_code(status int, hour int, month int, year int, date date , crawler text, frequency int, PRIMARY KEY(status, hour, date));
CREATE TABLE IF NOT EXISTS file_type(type text, hour int, month int, year int, date date, crawler text, frequency int, PRIMARY KEY(type, hour, date));
CREATE TABLE IF NOT EXISTS crawler_frequency(top_directory text, hour int, month int, year int,  crawler text, last_crawl datetime, frequency int, PRIMARY KEY(top_directory, hour, date));

