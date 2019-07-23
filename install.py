import mysql.connector

from config import *

conn = mysql.connector.connect(
    host=db_host,
    user=db_username,
    password=db_password,
    database=db_name,
    auth_plugin='mysql_native_password'
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS `currency` (
    `currency_id` INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL,
    `currency_code` VARCHAR(4) NOT NULL,
    `currency_name` VARCHAR(50) NOT NULL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS `pairing` (
    `pairing_id` INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL,
    `primary_currency_id` INTEGER NOT NULL,
    `secondary_currency_id` INTEGER NOT NULL,
    `primary_min` DECIMAL(15, 8) NOT NULL,
    `secondary_min` DECIMAL(15,8) NOT NULL,
    `active` BOOLEAN NOT NULL,
    `trading` BOOLEAN DEFAULT FALSE,
    `expected_percent_win` DECIMAL(5,2) DEFAULT NULL,
    `step_number` INTEGER DEFAULT NULL,
    `step_factor` DECIMAL(5,2) DEFAULT NULL,
    `step_gap_percent` DECIMAL(5,2) DEFAULT NULL,
    FOREIGN KEY (`primary_currency_id`) REFERENCES `currency`(`currency_id`)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (`secondary_currency_id`) REFERENCES `currency`(`currency_id`)
        ON UPDATE CASCADE ON DELETE CASCADE
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS `trade` (
    `trade_id` INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL,
    `pairing_id` INTEGER NOT NULL,
    `rate`DECIMAL(20, 8) NOT NULL,
    `amount` DECIMAL(20, 8) NOT NULL,
    `trade_date` INTEGER NOT NULL,
    `order_id` INTEGER NOT NULL,
    `trade_type` VARCHAR(4) NOT NULL,
    FOREIGN KEY (`pairing_id`) REFERENCES `pairing`(`pairing_id`)
)
""")

cursor.execute("""
INSERT INTO `currency`
SELECT `currency_id`, `currency_code`, `currency_name` FROM (
SELECT 1 as `currency_id`, 'THB' as `currency_code`, 'Thai Baht' as `currency_name`
UNION SELECT 2, 'BTC', 'Bitcoin'
UNION SELECT 3, 'LTC', 'Litecoin'
UNION SELECT 4, 'NMC', 'Namecoin'
UNION SELECT 5, 'DOG', 'Dogecoin'
UNION SELECT 6, 'PPC', 'Peercoin'
UNION SELECT 7, 'FTC', 'Feathercoin'
UNION SELECT 8, 'XPM', 'Primecoin'
UNION SELECT 9, 'ZEC', 'Zcash'
UNION SELECT 10, 'ZET', 'Zetacoin'
UNION SELECT 11, 'CPT', 'Cryptaur'
UNION SELECT 12, 'HYP', 'Hyperstake'
UNION SELECT 13, 'PND', 'Pandacoin'
UNION SELECT 14, 'XCN', 'Cryptonite'
UNION SELECT 15, 'XPY', 'Paycoin'
UNION SELECT 16, 'LEO', 'LEOcoin'
UNION SELECT 17, 'ETH', 'Ethereum'
UNION SELECT 18, 'DAS', 'Dash'
UNION SELECT 19, 'REP', 'Augur'
UNION SELECT 20, 'GNO', 'Gnosis'
UNION SELECT 21, 'XRP', 'Ripple'
UNION SELECT 22, 'OMG', 'OmiseGO'
UNION SELECT 23, 'BCH', 'Bitcoin Cash'
UNION SELECT 24, 'EVX', 'Everex'
UNION SELECT 25, 'XZC', 'Zcoin'
UNION SELECT 26, 'POW', 'Power Ledger'
UNION SELECT 27, 'ZMN', 'ZMINE'
UNION SELECT 28, 'EOS', 'EOS'
UNION SELECT 29, 'BSV', 'Bitcoin SV'
UNION SELECT 30, 'GUSD', 'Gemini US Dollar'
) `temporary_data` WHERE `temporary_data`.`currency_id` NOT IN (SELECT `currency_id` FROM `currency`);
""")

conn.commit()
conn.close()
