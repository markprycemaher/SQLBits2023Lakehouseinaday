-- dotnet run produce topic_0 /Users/mark/examples/clients/cloud/csharp/.vscode/getting_started.properties
--  dotnet run consume topic_0 /Users/mark/examples/clients/cloud/csharp/.vscode/getting_started.properties
-- /Users/mark/examples/clients/cloud/csharp


select count(*)  from dbo.rolls


select *  from dbo.rolls order by [RollDateTime] desc


select count(*), class,race,name,location  from dbo.rolls
group by class,race,name,location 
order by count(*) desc


select rolldatetime, ttt
, DATEDIFF (second, rolldatetime, ttt) rr
 from (
select  rolldatetime , 
isnull(LAG(rolldatetime, 1,getdate()) OVER (ORDER BY rolldatetime),getdate()) ttt
 from dbo.rolls

) q
order by rolldatetime

truncate table dbo.rolls

    select *  from dbo.rolls where name in (select 'Simon' union select 'Mark' union select 'Stijn' union select 'Filip' )

delete from dbo.rolls where name in (select 'Simon' union select 'Mark' union select 'Stijn' union select 'Filip' )


select count(*), avg(roll),min(roll),max(roll),Dice from dbo.rolls
group by Dice


update dbo.rolls 
set roll = roll + 1 

select count(*),[Name] from dbo.rolls group by [Name]
having count(*) > 1
order by count(*) desc


select * from 
rolls order by g DESC

select count(*),[location] from dbo.rolls group by [location]
having count(*) > 1
order by count(*) desc


