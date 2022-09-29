// Create key space
CREATE KEYSPACE IF NOT EXISTS log_analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

// To use a keyspace
use log_analytics;

// Create tables
// drop table status_code;
// drop table file_type;
// drop table crawler_frequency;
// drop table bot_hits;

CREATE TABLE IF NOT EXISTS status_code(status int, hour int, month int, year int, date text , crawler text, frequency int, PRIMARY KEY(status, hour, date));
CREATE TABLE IF NOT EXISTS file_type(file_type text, hour int, month int, year int, date text, crawler text, frequency int, PRIMARY KEY(file_type, hour, date));
CREATE TABLE IF NOT EXISTS crawler_frequency(hour int, month int, year int, crawler text, date text, top_directory text, frequency int, PRIMARY KEY(top_directory, hour, date));
CREATE TABLE IF NOT EXISTS bot_hits(hour int, month int, year int, crawler text, date text, frequency int, PRIMARY KEY(hour, date));
