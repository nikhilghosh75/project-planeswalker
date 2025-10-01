CREATE TABLE cards (
    card_id SERIAL NOT NULL PRIMARY KEY,
	english_name CHAR(192) NOT NULL,
	released_at DATE NOT NULL,
	mana_cost CHAR(48),
	typeline CHAR(192),
	power SMALLINT,
	toughness SMALLINT,
	loyalty SMALLINT,
	oracle_text VARCHAR,
	set_code CHAR(8),
	art_url VARCHAR
);