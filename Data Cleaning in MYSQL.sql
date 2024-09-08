-- Data Cleaning 

create database world_layoffs;
use world_layoffs;
select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank VALUES
-- 4. Remove any Columns

CREATE TABLE layoffs_staging like layoffs;

select * from layoffs_staging;

insert layoffs_staging
SELECT * 
from layoffs;

SELECT *,
ROW_NUMBER() OVER( PARTITION BY industry, total_laid_off, percentage_laid_off, `date`)
as row_num
from layoffs_staging;

WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER( PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num>1;

WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER( PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging
)
delete from duplicate_cte
where row_num>1;

SELECT * from layoffs_staging
where company = 'Casper';



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

INSERT into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER( PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging;

SELECT * from layoffs_staging2
where row_num>1;

delete from layoffs_staging2
where row_num>1;

-- Standardizing Data

SELECT company, trim(company) 
From layoffs_staging2;

UPDATE layoffs_staging2
set company = trim(company);

SELECT distinct industry 
From layoffs_staging2
order by 1;

SELECT * 
From layoffs_staging2
where industry like 'Crypto%';

UPDATE layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country
from layoffs_staging2
ORDER BY 1;

select DISTINCT country, trim( trailing '.' from country)
from layoffs_staging2
ORDER BY 1;

update layoffs_staging2
set country = trim( trailing '.' from country)
where country like 'United States%';

select `date`
-- str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

Alter TABLE layoffs_staging2
modify column `date` DATE;

UPDATE layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

SELECT * from 
layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry  = NULL
where industry = '';

SELECT t1.industry, t2.industry
from layoffs_staging2 t1
join 
	layoffs_staging2 t2
		on t1.company = t2.company
		and t1.location = t2.location
where t1.industry is null
and t2.industry is not null; 


update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

SELECT * 
from layoffs_staging2;


delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;


SELECT * 
from layoffs_staging2
