-- Data Cleaning
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Remove any columns or rows

-- Remove duplicates

SELECT * FROM healthcare_dataset;

CREATE TABLE healthcare_dataset1
LIKE healthcare_dataset;

INSERT healthcare_dataset1
SELECT *
FROM healthcare_dataset;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Name`, Age, Gender, `Blood Type`, `Medical Condition`, `Date of Admission`, Doctor, Hospital, `Insurance Provider`, `Billing Amount`, `Room Number`, `Admission Type`, `Discharge Date`, Medication, `Test Results`) AS row_num
FROM healthcare_dataset1;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Name`, Age, Gender, `Blood Type`, `Medical Condition`, `Date of Admission`, Doctor, Hospital, `Insurance Provider`, `Billing Amount`, `Room Number`, `Admission Type`, `Discharge Date`, Medication, `Test Results`) AS row_num
FROM healthcare_dataset1
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `healthcare_dataset2` (
  `Name` text,
  `Age` int DEFAULT NULL,
  `Gender` text,
  `Blood Type` text,
  `Medical Condition` text,
  `Date of Admission` text,
  `Doctor` text,
  `Hospital` text,
  `Insurance Provider` text,
  `Billing Amount` double DEFAULT NULL,
  `Room Number` int DEFAULT NULL,
  `Admission Type` text,
  `Discharge Date` text,
  `Medication` text,
  `Test Results` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO healthcare_dataset2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Name`, Age, Gender, `Blood Type`, `Medical Condition`, `Date of Admission`, Doctor, Hospital, `Insurance Provider`, `Billing Amount`, `Room Number`, `Admission Type`, `Discharge Date`, Medication, `Test Results`) AS row_num
FROM healthcare_dataset1;

DELETE
FROM healthcare_dataset2
WHERE row_num > 1;

-- Standardize data

UPDATE healthcare_dataset2
SET `Name` = CASE
   -- Four-part name (e.g., Mr. John Smith Phd)
   WHEN LENGTH(`Name`) - LENGTH(REPLACE(`Name`, ' ', '')) = 3 THEN
	CONCAT_WS(' ',
	  CONCAT(
		UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', 1)), 1, 1)),
		LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', 1)), 2))
	  ),
	  CONCAT(
		UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(`Name`, ' ', 2), ' ', -1)), 1, 1)),
		LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(`Name`, ' ', 2), ' ', -1)), 2))
	  ),
	  CONCAT(
		UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(`Name`, ' ', 3), ' ', -1)), 1, 1)),
		LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(`Name`, ' ', 3), ' ', -1)), 2))
	  ),
	  CONCAT(
		UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', -1)), 1, 1)),
		LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', -1)), 2))
	  )
	)

  -- Three-part name (First Middle Last)
  WHEN LENGTH(`Name`) - LENGTH(REPLACE(`Name`, ' ', '')) = 2 THEN
    CONCAT_WS(' ',
      CONCAT(
        UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', 1)), 1, 1)),
        LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', 1)), 2))
      ),
      CONCAT(
        UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(`Name`, ' ', 2), ' ', -1)), 1, 1)),
        LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(`Name`, ' ', 2), ' ', -1)), 2))
      ),
      CONCAT(
        UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', -1)), 1, 1)),
        LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', -1)), 2))
      )
    )

  -- Two-part name (First Last)
  WHEN LENGTH(`Name`) - LENGTH(REPLACE(`Name`, ' ', '')) = 1 THEN
    CONCAT_WS(' ',
      CONCAT(
        UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', 1)), 1, 1)),
        LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', 1)), 2))
      ),
      CONCAT(
        UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', -1)), 1, 1)),
        LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(`Name`, ' ', -1)), 2))
      )
    )

  -- One-part name (Single Name)
  ELSE
    CONCAT(
      UPPER(SUBSTRING(TRIM(`Name`), 1, 1)),
      LOWER(SUBSTRING(TRIM(`Name`), 2))
    )
END;

ALTER TABLE healthcare_dataset2
MODIFY COLUMN `Date of Admission` DATE;

ALTER TABLE healthcare_dataset2
MODIFY COLUMN `Discharge Date` DATE;

ALTER TABLE healthcare_dataset2
MODIFY `Billing Amount` DECIMAL(7,2);

-- Remove any columns or rows

ALTER TABLE healthcare_dataset2
DROP COLUMN row_num;