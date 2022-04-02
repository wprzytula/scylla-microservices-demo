DROP KEYSPACE IF EXISTS advertisementService;

CREATE KEYSPACE advertisementService WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE advertisementService;

CREATE TABLE advertiser (
	name text PRIMARY KEY,
	budget counter,
);

-- Advertiser as partition key.
-- Advertiser may want to fetch all his advertisements or all his advertisements from specific site.

CREATE TABLE advertisement (
	id uuid, -- should be uqique!
	advertiser text,
	site text,
	active_to date,
	PRIMARY KEY (advertiser, site, id) 
);

-- Possible actions:
-- SELECT * FROM advertisement WHERE advertiser = 'Orange' AND site = 'wp.pl';
-- SELECT * FROM advertisement WHERE advertiser = 'Orange';

-- To perform something like
-- SELECT * FROM advertisement WHERE advertiser = 'Orange' AND site = 'wp.pl' AND active_to > toDate(now()) use ALLOW_FILTERING.
-- We need to be able to update active_to so it's not included in primary key.

-- The only other columns in a table with a counter column can be columns of the primary key (which cannot be updated).
-- No other kinds of column can be included. 
CREATE TABLE advertisement_rate (
    rate counter,
    id uuid PRIMARY KEY
);

CREATE TABLE site (
	name text PRIMARY KEY,
	pricing int
);

-- INSERT INTO advertiser (name, budget) VALUES ('Orange', 1000);
-- A table with a counter column is created empty and gets populated and initialized by updates, rather than inserts.
UPDATE advertiser SET budget = budget + 1000 WHERE name = 'Orange';
UPDATE advertiser SET budget = budget + 500 WHERE name = 'Play';

INSERT INTO site (name, pricing) VALUES ('onet.pl', 10);
INSERT INTO site (name, pricing) VALUES ('wp.pl', 15);

-- Buy a new advertisement for 1 day:
INSERT INTO advertisement (id, advertiser, site, active_to) VALUES (uuid(), 'Orange', 'wp.pl', toDate(now()) + 1d);
INSERT INTO advertisement (id, advertiser, site, active_to) VALUES (bfe220df-8c7c-478e-a380-1c7c3402285e, 'Play', 'onet.pl', toDate(now()) + 1d);
-- Nested queries are not allowed, so you need to fetch the pricing and then update the advertiser's budget
-- SELECT pricing FROM site WHERE name = 'wp.pl';
UPDATE advertiser SET budget = budget - 15 WHERE name = 'Orange';
UPDATE advertiser SET budget = budget - 10 WHERE name = 'Play';

-- On-click on the advertisement increase its rate
UPDATE advertisement_rate SET rate = rate + 1 WHERE id = bfe220df-8c7c-478e-a380-1c7c3402285e;

-- Prolong the duration of activity of the advertisement.
-- You should firstly fetch the active_to of the interesting advertisement and then update it with the increased value
UPDATE advertisement SET active_to = toDate(now()) + 1w WHERE advertiser = 'Play' and site = 'onet.pl' AND id = bfe220df-8c7c-478e-a380-1c7c3402285e;
-- You can have some pre-defined options of prolonging the advertisement activity like 1 day, 1 week, 1 month etc.
-- Remember pay for it (update the advertisor's budget)
