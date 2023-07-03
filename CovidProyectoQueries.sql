-- Inicio exploracion de datos

SELECT * FROM CovidProyecto.dbo.Muertes;

SELECT * FROM CovidProyecto.dbo.Vacunas;

SELECT *
FROM CovidProyecto.dbo.Muertes
WHERE continent IS null AND location = 'South America'
ORDER BY date;

-- Filtrando informacion para paises de Sudamerica

SELECT location, total_cases, new_cases, total_deaths, population
FROM ProyectoCovid.dbo.MuertesCovid
WHERE continent IS NOT NULL AND continent = 'South America'
ORDER BY location;

-- Total muertes y casos para paises de Sudamerica

SELECT location, SUM(new_cases) AS total_casos, SUM(new_deaths) AS total_muertes
FROM CovidProyecto.dbo.Muertes
WHERE continent IS NOT NULL AND continent = 'South America'
GROUP BY location
ORDER BY location;

-- Cambiar datatype de las columnas a bigint para poder hacer calculos

ALTER TABLE CovidProyecto.dbo.Muertes
ALTER COLUMN total_deaths bigint;

ALTER TABLE CovidProyecto.dbo.Muertes
ALTER COLUMN total_cases bigint;


-- Muertes totales vs Casos totales en Sudamerica como porcentaje

SELECT location, date, total_cases, total_deaths, (CAST(total_deaths as float)/total_cases)*100 AS PorcentajeMuertes
FROM CovidProyecto.dbo.Muertes
WHERE continent = 'South America'
ORDER BY location, date;


-- Total de casos vs poblacion en porcentaje en paises de america del sur

SELECT location, date, population, total_cases, (CAST(total_cases as float)/population)*100 AS PorcentajeInfectados
FROM CovidProyecto.dbo.Muertes
WHERE continent = 'South America'
ORDER BY location, date;

-- Maximo porcentaje de infectados alcanzado para paises de america del sur

SELECT location, population, MAX(total_cases) as MayorCantidadInfectados, MAX((CAST(total_cases as float)/population))*100 AS MayorPorcentajeInfectados
FROM CovidProyecto.dbo.Muertes
WHERE continent = 'South America'
GROUP BY location, population
ORDER BY MayorPorcentajeInfectados DESC;


-- Ranking de paises sudamericanos con mayor porcentaje de muertes segun poblacion

SELECT location, population, MAX(total_deaths) as MayorCantidadMuertes, MAX((CAST(total_deaths as float)/population))*100 AS MayorPorcentajeMuertes
FROM CovidProyecto.dbo.Muertes
WHERE continent = 'South America'
GROUP BY location, population
ORDER BY MayorPorcentajeMuertes DESC;


-- Inicio analisis de vacunas

SELECT *
FROM CovidProyecto.dbo.Muertes mue
JOIN CovidProyecto.dbo.Vacunas vac
ON mue.location = vac.location
AND mue.date = vac.date
ORDER BY mue.location, mue.date;


-- Uso de windows function para calcular total acumulado de nuevas vacunas

SELECT mue.location, mue.date, mue.population, new_vaccinations, SUM(CAST(new_vaccinations as bigint)) OVER(PARTITION BY mue.location ORDER BY mue.location, mue.date) as VacunasAcumuladas
FROM CovidProyecto.dbo.Muertes mue
JOIN CovidProyecto.dbo.Vacunas vac
	ON mue.location = vac.location
	AND mue.date = vac.date
WHERE mue.continent IS NOT NULL AND mue.continent = 'South America'
ORDER BY mue.location, mue.date;



-- Uso de CTE para realizar mas calculos en la query anterior, especificamente para calcular el porcentaje de vacunas que hay segun poblacion (considerando que se aplico mas de una vacuna por persona)

WITH CTE AS(
SELECT mue.location, mue.date, mue.population, SUM(CAST(new_vaccinations as bigint)) OVER(PARTITION BY mue.location ORDER BY mue.location, mue.date) as VacunasAcumuladas
FROM CovidProyecto.dbo.Muertes mue
JOIN CovidProyecto.dbo.Vacunas vac
	ON mue.location = vac.location
	AND mue.date = vac.date
WHERE mue.continent IS NOT NULL and mue.continent = 'South America'
)

SELECT *, (VacunasAcumuladas/population)*100 as PorcentajeVacunasPoblacion
FROM CTE
ORDER BY location, date;



-- CTE para calcular diferencias en casos por mes

WITH diferencia_mes AS (
 SELECT
   location,
   YEAR(date) as year,
   MONTH(date) as month,
   SUM(new_cases) as total_casos
 FROM CovidProyecto.dbo.Muertes
 WHERE continent is not null and continent = 'South America'
 GROUP BY location, YEAR(date), MONTH(date)
)
SELECT 
  location, year, month, total_casos,
  LAG(total_casos) OVER (PARTITION BY location ORDER BY year, month) as casos_mes_anterior,
  total_casos - LAG(total_casos) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_por_mes
FROM diferencia_mes
ORDER BY 1,2,3;



-- Diferencias en vacunas por mes

WITH diferencia_vacunas_mes AS (
SELECT mue.location, YEAR(mue.date) as year, MONTH(mue.date) as month, SUM(CAST(new_vaccinations as bigint)) as vacunas
FROM CovidProyecto.dbo.Muertes mue
JOIN CovidProyecto.dbo.Vacunas vac
	ON mue.location = vac.location
	AND mue.date = vac.date
WHERE mue.continent IS NOT NULL and mue.continent = 'South America'
GROUP BY mue.location, YEAR(mue.date), MONTH(mue.date)
)
SELECT 
  location, year, month, vacunas,
  LAG(vacunas) OVER (PARTITION BY location ORDER BY year, month) as vacunas_mes_anterior,
  vacunas - LAG(vacunas) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_por_mes
FROM diferencia_vacunas_mes
ORDER BY 1,2,3;


-- Diferencias en muertes y casos por mes

WITH diferencia_mes AS (
 SELECT
   location,
   YEAR(date) as year,
   MONTH(date) as month,
   SUM(new_deaths) as muertes,
   SUM(new_cases) as casos
 FROM CovidProyecto.dbo.Muertes
 WHERE continent is not null and continent = 'South America'
 GROUP BY location, YEAR(date), MONTH(date)
)
SELECT 
  location, year, month, muertes,
  LAG(muertes) OVER (PARTITION BY location ORDER BY year, month) as muertes_mes_anterior,
  muertes - LAG(muertes) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_muertes,
  casos,
  LAG(casos) OVER (PARTITION BY location ORDER BY year, month) as casos_mes_anterior,
  casos - LAG(casos) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_casos
FROM diferencia_mes
ORDER BY 1,2,3;



--Diferencias en vacunas y casos por mes

WITH diferencia_vacunas_mes AS (
SELECT mue.location, YEAR(mue.date) as year, MONTH(mue.date) as month, SUM(new_cases) as casos, SUM(CAST(new_vaccinations as bigint)) as vacunas
FROM CovidProyecto.dbo.Muertes mue
JOIN CovidProyecto.dbo.Vacunas vac
	ON mue.location = vac.location
	AND mue.date = vac.date
WHERE mue.continent IS NOT NULL and mue.continent = 'South America'
GROUP BY mue.location, YEAR(mue.date), MONTH(mue.date)
)
SELECT 
  location, year, month, vacunas,
  LAG(vacunas) OVER (PARTITION BY location ORDER BY year, month) as vacunas_mes_anterior,
  vacunas - LAG(vacunas) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_vacunas,
  casos,
  LAG(casos) OVER (PARTITION BY location ORDER BY year, month) as casos_mes_anterior,
  casos - LAG(casos) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_casos
FROM diferencia_vacunas_mes
ORDER BY 1,2,3;



-- Views para visualizacion

USE CovidProyecto
GO
CREATE VIEW casos_y_muertes AS
SELECT location, SUM(new_cases) AS Total_Casos, SUM(new_deaths) AS Total_Muertes
FROM CovidProyecto.dbo.Muertes
WHERE continent IS NOT NULL AND continent = 'South America'
GROUP BY location;


USE CovidProyecto
GO
CREATE VIEW porcentaje_muertes AS
SELECT location, date, total_cases, total_deaths, (CAST(total_deaths as float)/total_cases)*100 AS PorcentajeMuertes
FROM CovidProyecto.dbo.Muertes
WHERE continent = 'South America';


USE CovidProyecto
GO
CREATE VIEW porcentaje_infectados AS
SELECT location, date, population, total_cases, (CAST(total_cases as float)/population)*100 AS PorcentajeInfectados
FROM CovidProyecto.dbo.Muertes
WHERE continent = 'South America';


USE CovidProyecto
GO
CREATE VIEW max_porcentaje_infectados AS
SELECT location, population, MAX(total_cases) as MayorCantidadInfectados, MAX((CAST(total_cases as float)/population))*100 AS MayorPorcentajeInfectados
FROM CovidProyecto.dbo.Muertes
WHERE continent = 'South America'
GROUP BY location, population;


USE CovidProyecto
GO
CREATE VIEW max_porcentaje_muertes AS
SELECT location, population, MAX(total_deaths) as MayorCantidadMuertes, MAX((CAST(total_deaths as float)/population))*100 AS MayorPorcentajeMuertes
FROM CovidProyecto.dbo.Muertes
WHERE continent = 'South America'
GROUP BY location, population;


USE CovidProyecto
GO
CREATE VIEW porcentaje_vacunas_poblacion AS
WITH CTE AS(
SELECT mue.location, mue.date, mue.population, SUM(CAST(new_vaccinations as bigint)) OVER(PARTITION BY mue.location ORDER BY mue.location, mue.date) as VacunasAcumuladas
FROM CovidProyecto.dbo.Muertes mue
JOIN CovidProyecto.dbo.Vacunas vac
	ON mue.location = vac.location
	AND mue.date = vac.date
WHERE mue.continent IS NOT NULL and mue.continent = 'South America'
)

SELECT *, (VacunasAcumuladas/population)*100 as PorcentajeVacunasPoblacion
FROM CTE;


USE CovidProyecto
GO
CREATE VIEW  diferencia_casos_mes AS
WITH diferencia_mes AS (
 SELECT
   location,
   YEAR(date) as year,
   MONTH(date) as month,
   SUM(new_cases) as total_casos
 FROM CovidProyecto.dbo.Muertes
 WHERE continent is not null and continent = 'South America'
 GROUP BY location, YEAR(date), MONTH(date)
)
SELECT 
  location, year, month, total_casos,
  LAG(total_casos) OVER (PARTITION BY location ORDER BY year, month) as casos_mes_anterior,
  total_casos - LAG(total_casos) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_por_mes
FROM diferencia_mes;



USE CovidProyecto
GO
CREATE VIEW diferencia_vacunas AS
WITH diferencia_vacunas_mes AS (
SELECT mue.location, YEAR(mue.date) as year, MONTH(mue.date) as month, SUM(CAST(new_vaccinations as bigint)) as vacunas
FROM CovidProyecto.dbo.Muertes mue
JOIN CovidProyecto.dbo.Vacunas vac
	ON mue.location = vac.location
	AND mue.date = vac.date
WHERE mue.continent IS NOT NULL and mue.continent = 'South America'
GROUP BY mue.location, YEAR(mue.date), MONTH(mue.date)
)
SELECT 
  location, year, month, vacunas,
  LAG(vacunas) OVER (PARTITION BY location ORDER BY year, month) as vacunas_mes_anterior,
  vacunas - LAG(vacunas) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_por_mes
FROM diferencia_vacunas_mes


USE CovidProyecto
GO
CREATE VIEW diferencia_casos_muertes AS
WITH diferencia_mes AS (
 SELECT
   location,
   YEAR(date) as year,
   MONTH(date) as month,
   SUM(new_deaths) as muertes,
   SUM(new_cases) as casos
 FROM CovidProyecto.dbo.Muertes
 WHERE continent is not null and continent = 'South America'
 GROUP BY location, YEAR(date), MONTH(date)
)
SELECT 
  location, year, month, muertes,
  LAG(muertes) OVER (PARTITION BY location ORDER BY year, month) as muertes_mes_anterior,
  muertes - LAG(muertes) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_muertes,
  casos,
  LAG(casos) OVER (PARTITION BY location ORDER BY year, month) as casos_mes_anterior,
  casos - LAG(casos) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_casos
FROM diferencia_mes;


USE CovidProyecto
GO
CREATE VIEW diferencia_vacunas_casos AS
WITH diferencia_vacunas_mes AS (
SELECT mue.location, YEAR(mue.date) as year, MONTH(mue.date) as month, SUM(new_cases) as casos, SUM(CAST(new_vaccinations as bigint)) as vacunas
FROM CovidProyecto.dbo.Muertes mue
JOIN CovidProyecto.dbo.Vacunas vac
	ON mue.location = vac.location
	AND mue.date = vac.date
WHERE mue.continent IS NOT NULL and mue.continent = 'South America'
GROUP BY mue.location, YEAR(mue.date), MONTH(mue.date)
)
SELECT 
  location, year, month, vacunas,
  LAG(vacunas) OVER (PARTITION BY location ORDER BY year, month) as vacunas_mes_anterior,
  vacunas - LAG(vacunas) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_vacunas,
  casos,
  LAG(casos) OVER (PARTITION BY location ORDER BY year, month) as casos_mes_anterior,
  casos - LAG(casos) OVER (PARTITION BY LOCATION ORDER BY year, month) as diferencia_casos
FROM diferencia_vacunas_mes;





