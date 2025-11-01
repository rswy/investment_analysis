-- Fund Positions for data loading
BEGIN TRANSACTION;

DROP TABLE IF EXISTS fund_positions; -- Should remove in production set up as this is dangerous. Track files that's been processed instead of dropping to remove duplicates..
CREATE TABLE IF NOT EXISTS fund_positions (
    fund_name TEXT NOT NULL,
    eom_date TEXT NOT NULL,
    financial_type TEXT,
    symbol TEXT,
    security_name TEXT,
    sedol TEXT,
    isin TEXT,
    price REAL,
    quantity REAL,
    realised_p_l REAL,
    market_value REAL,
    PRIMARY KEY (fund_name, eom_date, symbol)
);
COMMIT;

