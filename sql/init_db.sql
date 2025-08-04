-- Create schema for raw ingested data
CREATE SCHEMA IF NOT EXISTS raw;

-- Drop and recreate players table
DROP TABLE IF EXISTS raw.players;

CREATE TABLE raw.players (
    player_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    sg_total FLOAT,
    sg_t2g FLOAT,
    sg_app FLOAT,
    sg_arg FLOAT,
    sg_putt FLOAT
);