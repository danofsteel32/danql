-- danql test tables
PRAGMA foreign_keys = ON; -- Turn on foreign key constraints

-- BREED
CREATE TABLE IF NOT EXISTS breed (
    breed_id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS breed_name_index 
ON breed (name);

-- OWNER
CREATE TABLE IF NOT EXISTS owner (
    owner_id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- DOG
CREATE TABLE IF NOT EXISTS dog (
    breed_id INTEGER NOT NULL,
    owner_id INTEGER NOT NULL,
    name     TEXT NULL DEFAULT NULL,
    PRIMARY KEY(breed_id, owner_id, name), -- fair assumption
    FOREIGN KEY (breed_id) REFERENCES breed (breed_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (owner_id) REFERENCES owner (owner_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
