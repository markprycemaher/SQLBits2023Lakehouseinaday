-- Databricks notebook source
SELECT *
FROM SparkSilver.rolls

-- COMMAND ----------

show tables from sparkgold

-- COMMAND ----------

use sparkgold;

select dc.Class, dc.Race, avg(Roll) 
from factrolls fr
inner join dimcharacter dc on fr.CharacterID = dc.CharacterID
group by dc.Class, dc.Race
order by avg(Roll) desc