// Create key space
CREATE KEYSPACE IF NOT EXISTS log_analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

// To use a keyspace
use log_analytics;

// drop tables
drop table IF EXISTS status_code_realtime;
drop table IF EXISTS file_type_realtime;
drop table IF EXISTS crawler_frequency_realtime;
drop table IF EXISTS bot_hits_realtime;

drop table IF EXISTS status_code;
drop table IF EXISTS file_type;
drop table IF EXISTS crawler_frequency;
drop table IF EXISTS bot_hits;

// Create tables
// Realtime job tables
CREATE TABLE IF NOT EXISTS status_code_realtime(status int, hour int, month int, year int, date text , crawler text, frequency int, current_timestamp TIMESTAMP, PRIMARY KEY(status, hour, date, crawler));
CREATE TABLE IF NOT EXISTS file_type_realtime(file_type text, hour int, month int, year int, date text, crawler text, frequency int, current_timestamp TIMESTAMP, PRIMARY KEY(file_type, hour, date, crawler));
CREATE TABLE IF NOT EXISTS crawler_frequency_realtime(hour int, month int, year int, crawler text, date text, top_directory text, frequency int, current_timestamp TIMESTAMP, PRIMARY KEY(top_directory, hour, date, crawler));
CREATE TABLE IF NOT EXISTS bot_hits_realtime(hour int, month int, year int, crawler text, date text, frequency int, current_timestamp TIMESTAMP, PRIMARY KEY(hour, date, crawler));

// batch job tables
CREATE TABLE IF NOT EXISTS status_code(status int, hour int, month int, year int, date text , crawler text, frequency int, current_timestamp TIMESTAMP, PRIMARY KEY(status, hour, date, crawler));
CREATE TABLE IF NOT EXISTS file_type(file_type text, hour int, month int, year int, date text, crawler text, frequency int, current_timestamp TIMESTAMP, PRIMARY KEY(file_type, hour, date, crawler));
CREATE TABLE IF NOT EXISTS crawler_frequency(hour int, month int, year int, crawler text, date text, top_directory text, frequency int, current_timestamp TIMESTAMP, PRIMARY KEY(top_directory, hour, date, crawler));
CREATE TABLE IF NOT EXISTS bot_hits(hour int, month int, year int, crawler text, date text, frequency int, current_timestamp TIMESTAMP, PRIMARY KEY(hour, date, crawler));
