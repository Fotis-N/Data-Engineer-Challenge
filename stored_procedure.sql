/* TABLE FOR AGGREGATING THE OUTPUT OF THE STORED PROCEDURE */
CREATE TABLE IF NOT EXISTS Margin (
id MEDIUMINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
ad_type ENUM('Free', 'Premium', 'Platinum') NOT NULL, 
payment_type VARCHAR(50) NOT NULL, 
start_date TIMESTAMP NOT NULL, 
end_date TIMESTAMP NOT NULL, 
margin DECIMAL(10,4) NOT NULL) 
ENGINE=InnoDB DEFAULT CHARSET=utf8;


/* THE STORED PROCEDURE REQUIRES AT LEAST THE FIRST HOUR OF ANALYTICS TO BE PRESENT INSIDE THE "MARGIN" TABLE */
/* THIS LINE INSERTS THE MARGINS OF THE FIRST HOUR -- FEEL FREE TO ADJUST THE TIMES ACCORDINGLY */
/* THE "CLASSIFIEDS" TABLE IS CREATED (IF NOT EXISTS) SEPARATELY WHEN RUNNING THE MAIN APPLICATION SCRIPT */
INSERT INTO Margin (ad_type, payment_type, start_date, end_date, margin) SELECT `ad_type`, `payment_type`, '2020-02-01 00:00:00', '2020-02-01 01:00:00', AVG((price - payment_cost) / price) FROM `Classifieds` WHERE `ad_type` NOT IN ('Free') AND `created_at` BETWEEN '2020-02-01 00:00:00' AND '2020-02-01 01:00:00' GROUP BY `ad_type`, `payment_type`;


/* STORED PROCEDURE -- BEGIN */
DELIMITER $$
CREATE PROCEDURE CalcMargin()
BEGIN

DECLARE latest_calculated_hour TIMESTAMP;
DECLARE latest_available_hour TIMESTAMP;
SELECT end_date INTO latest_calculated_hour FROM Margin ORDER BY end_date DESC LIMIT 1;
SELECT created_at INTO latest_available_hour FROM Classifieds ORDER BY created_at DESC LIMIT 1;

WHILE TIMESTAMPADD(HOUR,1,latest_calculated_hour) < latest_available_hour
DO

INSERT INTO Margin (ad_type, payment_type, start_date, end_date, margin) SELECT `ad_type`, `payment_type`, latest_calculated_hour, TIMESTAMPADD(HOUR,1,latest_calculated_hour), AVG((price - payment_cost) / price) FROM `Classifieds` WHERE `ad_type` NOT IN ('Free') AND `created_at` BETWEEN latest_calculated_hour AND TIMESTAMPADD(HOUR,1,latest_calculated_hour) GROUP BY `ad_type`, `payment_type`;

SET latest_calculated_hour = TIMESTAMPADD(HOUR,1,latest_calculated_hour);
END WHILE;
END$$
DELIMITER ;
/* STORED PROCEDURE -- END */


/* SCHEDULE AN HOURLY EVENT FOR THE ABOVE PROCEDURE */
CREATE EVENT hourly_margin
    ON SCHEDULE EVERY 1 HOUR
    COMMENT 'Calculates hourly margin aggregated into Margin table.'
    DO
      CALL CalcMargin();


/* REMEMBER TO ENABLE THE GLOBAL EVENT_SCHEDULER SYSTEM VARIABLE */