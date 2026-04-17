-- Data Cleaning

SELECT *
FROM `layoffs delimited`;


create table layoffs_staging
like `layoffs delimited`;

insert layoffs_staging
select *
from `layoffs delimited`;

-- 1. duplikat 
select * 
from layoffs_staging;


SELECT *,
   ROW_NUMBER() OVER(
     PARTITION BY Company, Location, Industry, Total_Laid_Off, Percentage_Laid_Off, `Date`, Stage, Country, Funds_Raised_Millions
   ) AS Row_NUM
   FROM `layoffs delimited`
ORDER BY Company;

WITH duplicate_cte AS (
   SELECT *,
   ROW_NUMBER() OVER(
     PARTITION BY Company, Location, Industry, Total_Laid_Off, Percentage_Laid_Off, `Date`, Stage, Country, Funds_Raised_Millions
   ) AS Row_NUM
   FROM `layoffs delimited`
)
SELECT *
FROM duplicate_cte;



SELECT * FROM duplicate_cte
WHERE Row_NUM > 1;


 select * 
from layoffs_staging
where Company = 'Bevi' ;

CREATE TABLE `layoffs_staging2` (
  `Company` text,
  `Location` text,
  `Industry` text,
  `Total_Laid_Off` int DEFAULT NULL,
  `Percentage_Laid_Off` text,
  `Date` text,
  `Stage` text,
  `Country` text,
  `Funds_Raised_Millions` int DEFAULT NULL,
  `Row_NUM` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select*
from layoffs_staging2;
 

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM layoffs_staging;


select * 
from layoffs_staging2
where Row_NUM > 1;

delete
from layoffs_staging2
where Row_NUM > 1;

select * 
from layoffs_staging2;

-- standardasi data
select Company, TRIM(Company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET Company = TRIM(Company);


Select distinct Industry
from layoffs_staging2
order by 1;

Select distinct Industry
from layoffs_staging2
where Industry like 'Crypto%';

update layoffs_staging2
set Industry ='Crypto'
where Industry like 'Crypto%';

select distinct country
from layoffs_staging2
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
Where country like 'United States%';

ALTER table layoffs_staging2
modify COLUMN `Date` DATE;

select *
from layoffs_staging2;

update layoffs_staging2
set `Date` = str_to_date (`Date`, '%m/%d/%Y');


-- Memperbaiki data kosong

select *
from layoffs_staging2
WHERE Industry is null or Industry = '';

select  *
from layoffs_staging2
Where Company ='Ada';


-- mengisi data kosong dengan data lain sbg refresnsi menggunakan join
select t1.Industry, t2.Industry from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.Company = t2.Company
and t1.Location= t2.Location
where (t1.Industry is null or t1.Industry =  '')
And t2.Industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.Company = t2.Company
and t1.Location= t2.Location
set t1.Industry = t2.Industry
where (t1.Industry is null or t1.Industry =  '')
And t2.Industry is not null;

update layoffs_staging2
set Industry = null
where industry ='';

TRUNCATE TABLE layoffs_staging2;
INSERT INTO layoffs_staging2
SELECT 
    `Company`, 
    `Location`, 
    `Industry`, 
    `Total_Laid_Off`, 
    `Percentage_Laid_Off`, 
    STR_TO_DATE(`Date`, '%m/%d/%Y'),
    `Stage`, 
    `Country`, 
    `Funds_Raised_Millions`,
    ROW_NUMBER() OVER (
        PARTITION BY Company, Location, Industry, Total_Laid_Off, Percentage_Laid_Off, `Date`, Stage, Country, Funds_Raised_Millions
    ) AS Row_NUM
FROM `layoffs delimited`;

DELETE 
FROM layoffs_staging2
WHERE Row_NUM > 1;

ALTER table layoffs_staging2
DROP column Row_NUM;

CREATE TABLE `layoffs_staging3`(
  `Company` text,
  `Location` text,
  `Industry` text,
  `Total_Laid_Off` int DEFAULT NULL,
  `Percentage_Laid_Off` text,
  `Date` date DEFAULT NULL,
  `Stage` text,
  `Country` text,
  `Funds_Raised_Millions` int DEFAULT NULL,
  `Row_NUM` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging3
where Total_Laid_Off is null and Percentage_Laid_Off is null;

INSERT INTO layoffs_staging3
SELECT *
FROM layoffs_staging2;

ALTER table layoffs_staging3
DROP column Row_NUM;

delete
FROM layoffs_staging3
where Total_Laid_Off is null and Percentage_Laid_Off is null;

-- MELAKUKAN EDA (EXPLORASI DATA ANALIS)
SELECT *
FROM layoffs_staging3;


SELECT Company, `date`, Total_Laid_Off
FROM layoffs_staging3
order by Total_Laid_Off desc;



-- Looking at Percentage to see how big these layoffs were
SELECT MAX(Percentage_Laid_Off),  MIN(Percentage_Laid_Off)
FROM layoffs_staging3
WHERE  percentage_laid_off IS not null;

SELECT *
FROM layoffs_staging3
WHERE  percentage_laid_off = 1 and Funds_Raised_Millions is not null
order by Funds_Raised_Millions;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM layoffs_staging3
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time

-- Companies with the biggest single Layoff

SELECT company, total_laid_off, `date`
FROM layoffs_staging3
ORDER BY 2 DESC; 
-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;



-- by location
SELECT location, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- based on location
SELECT country, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY country
ORDER BY 2 DESC;

-- based on how many people got laid off from 2020 to 2023
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging3
where year (date) is not null
GROUP BY YEAR(date)
ORDER BY 1 ASC;

-- based on how many people got laid off from each industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY industry
ORDER BY 2 DESC;

-- this query is used to see how many people in each company and year got laid off and also to rank them
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging3
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;