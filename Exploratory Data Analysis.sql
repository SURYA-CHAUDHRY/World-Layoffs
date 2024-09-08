-- Exploratory Data Analyis

SELECT 
    *
FROM
    layoffs_staging2;

SELECT 
    MAX(total_laid_off), MAX(percentage_laid_off)
FROM
    layoffs_staging2;

-- Selecting companies with 100% layoff
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    percentage_laid_off = 1;

-- Analyzing the total_laid_off by some companies
SELECT 
    company, SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- minimum date and maximum date
SELECT 
    MIN(`date`), MAX(`date`)
FROM
    layoffs_staging2;

-- Which industry got hit most 
SELECT 
    industry, SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Which country got hit hard 
SELECT 
    country, SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Which year 
SELECT 
    YEAR(`date`), SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

-- By Stage Grouping 
SELECT 
    stage, SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Percentage 
SELECT 
    company, AVG(percentage_laid_off)
FROM
    layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Calculating layoffs in each month
SELECT 
    SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off)
FROM
    layoffs_staging2
WHERE
    SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 2 DESC;

-- Calculating layoffs in each month and the progression
With Rolling_total as 
(
select substring(`date`,1,7) as `Month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`, 1, 7) is not null
GROUP BY `Month`
order by 1 asc
)
select `Month`, total_off
,sum(total_off) over(order by `Month`) as Rolling_total
from Rolling_total;


-- Layoffs by compnay each year
SELECT 
    company, YEAR(`date`), SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY company , YEAR(`date`)
ORDER BY 3 DESC;

-- Ranking 
With Company_year(compnay, years, total_laid_off) as
(
SELECT 
    company, YEAR(`date`), SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY company , YEAR(`date`)
), Company_year_rank as
(
Select *, DENSE_RANK() over(PARTITION BY years order by total_laid_off desc) as Ranking
 from Company_year
 where years is not null
 )
 select * from Company_year_rank
 where ranking<=5;
 