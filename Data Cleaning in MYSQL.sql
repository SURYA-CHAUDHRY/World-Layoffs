-- Data Cleaning 

create database world_layoffs;
use world_layoffs;
select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank VALUES
-- 4. Remove any Columns

CREATE TABLE layoffs_staging like layoffs;  -- Creating the Table using the imported data table

select * from layoffs_staging;

-- Inserting Data into the new Table
insert layoffs_staging
SELECT * 
from layoffs;

-- Adding row_num column by using all the data for Partition
SELECT *,
ROW_NUMBER() OVER( PARTITION BY industry, total_laid_off, percentage_laid_off, `date`)
as row_num
from layoffs_staging;

-- Selecting all the duplicates values
WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER( PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num>1;

-- Deleting all the duplicate values but realised to create another table for deletion purposes
WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER( PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging
)
delete from duplicate_cte
where row_num>1;


-- Check for Duplicate values
SELECT * from layoffs_staging
where company = 'Casper';


-- Creating another table layoffs_staging2 for further deletion purposes
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
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * from layoffs_staging2;

-- Inserting data to new table using data in layoffs_staging
INSERT into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER( PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging;

SELECT * from layoffs_staging2
where row_num>1;

-- Deleting the Duplicate Data
delete from layoffs_staging2
where row_num>1;

-- Standardizing Data

-- Checking for error in spacing in company names
SELECT company, trim(company) 
From layoffs_staging2;

-- Updating company names after modifying
UPDATE layoffs_staging2
set company = trim(company);


SELECT distinct industry 
From layoffs_staging2
order by 1;

-- Selecting industry with almost same names
SELECT * 
From layoffs_staging2
where industry like 'Crypto%';

-- Updating the names
UPDATE layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- Standardize check for Country
select distinct country
from layoffs_staging2
ORDER BY 1;

-- -- Selecting for errors in country names or spacing
select DISTINCT country, trim( trailing '.' from country)
from layoffs_staging2
ORDER BY 1;

-- Updating the country names 
update layoffs_staging2
set country = trim( trailing '.' from country)
where country like 'United States%';

-- Standardize check for Dates
select `date`
-- str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

-- Changing the column type of date form text to date
Alter TABLE layoffs_staging2
modify column `date` DATE;

-- Changing the date format
UPDATE layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

-- Standard check for total_laid_off and percentage_laid_off
select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- Deleting the companies where there is no layoff
delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


-- Standard check for company
SELECT * from 
layoffs_staging2
where company = 'Airbnb';

-- Undating compnay with blank industry to industry as null
update layoffs_staging2
set industry  = NULL
where industry = '';

-- Using join to find companies with null industry
SELECT t1.industry, t2.industry
from layoffs_staging2 t1
join 
	layoffs_staging2 t2
		on t1.company = t2.company
		and t1.location = t2.location
where t1.industry is null
and t2.industry is not null; 

-- Updating the same industry for same company from null to a specific is avilable
update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

SELECT * 
from layoffs_staging2;

-- Droping the extra column of row_num we created for our help
alter table layoffs_staging2
drop column row_num;

-- Final Check 
SELECT * 
from layoffs_staging2
