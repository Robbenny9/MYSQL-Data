-- DATA Cleaning


SELECT *
FROM layoffs;

-- What i did in this project are as follows
-- I Removed the Duplicates
-- I Standardize the Data
-- I Tackled Null Values or blank
-- I also Removed Any unwanted columns and Rows

-- when cleaning a data, we need to create 
-- a staging database where we edits and correct the 
-- data without touching the real data

-- Removing Duplicates

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- first create a Row num

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date` ) AS row_num
FROM layoffs_staging;

-- creating a row_num is for the purpose of filtering
-- to see if the nums are greater than >2
-- if its greater than then its a duplicates
-- we need to use a CTE expression to deal with it

WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;
 
 -- to check if they afre truly the duplicates
 -- and if the are not truly duplicates then 
 -- do partition by every columns in the data
 
SELECT *
FROM layoffs_staging
WHERE company = 'Casper'
;
-- we want to keep the real data and delete the duplicates 
-- because not all might by duplicates. so we do this


WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
DELETE

FROM duplicate_cte
WHERE row_num > 1;

-- this is how to delete duplicates
-- from copy and paste the duplicates_cte and delete from there
-- we need to create another database in other to delete on msql
-- right click on the layoffs_staging or click on the symbol i
-- copy to clipboard and create statement
-- rename it 'layoffs_staging2
-- after the null value and a
-- `row_num` INT

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

-- then lets insert the information of layoffs into the new staging

SELECT *
FROM layoffs_staging2
WHERE row_num > 1 ;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1 ;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1 ;



-- Standardizing the data

SELECT company, TRIM(company)
FROM layoffs_staging2;


-- lets update the data 

UPDATE layoffs_staging2
SET COMPANY = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- TIME SERIES

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


-- to change the date type from text to 'date'

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;


-- REMOVING the NULL VALUES
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry ='';



SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- removing unwanted columns and rows
SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;