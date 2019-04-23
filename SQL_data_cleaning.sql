
/* 
######################################
Task C.1 - b: data cleaning : 20 records deleted
###################################### 
*/ 



-----
-- passengers
-----

-- check for age < 0 or age > 100: 5 invalid records
select * from passengers
where age < 0 or age > 100;

-- delete records
delete from passengers
where age < 0 or age > 100;

-----
-- transactions
-----

-- flightid in transactions but no in flights : 5 records
select * from transactions
where flightid not in 
    (select flightid from flights);

-- transactions date not in the range : 5 records
select * from transactions
where to_char(bookingdate, 'YYYY') < 2006 or to_char(bookingdate, 'YYYY') > 2009;


delete from transactions
where to_char(bookingdate, 'YYYY') < 2006 or to_char(bookingdate, 'YYYY') > 2009;


-----
-- flights
-----

-- fare < 0 or routeid < 0 : 1 record 
select * from flights
where fare < 0 or routeid < 0;

-- invalid aircraftid : 2 records
select * from flights
where aircraftID not in 
    (select aircrafts.IATACODE from aircrafts);

-- flight date out of range : 2 records 
select * from flights
where to_char(flightdate,'yyyymm') > 201201;

delete from flights 
where aircraftID not in 
    (select aircrafts.IATACODE from aircrafts);

-----
-- airports
-----
-- airportid < 0 : 5 records (duplicated records too) 
select * from airports
where airportid < 0 ;

-- use to check bad records
-- bad records for Goroka, same city, country, iata_faacode, icao, lat, long, timezone
select name, icao, city, country, count(icao)
from airports
group by name, icao, city, country
having count(icao) > 1;

delete from airports
where airportid < 0;


-----
-- airlines
-----

-- airlineid < 0 : 1 record
select * from airlines
where airlineid < 0;

-- invalid airline in the bridge table : 1 record
select * from airlines
where AIRLINEID not in 
    (select airlineid from provides);

delete from airlines
where airlineid < 0;
-----
-- routes
-----
-- numeric fields have values < 0 : 1 record
select * from routes
where routeid < 0 or distance < 0 or servicecost < 0;

-- invalid airlineid : 1 record
select * from routes
where AIRLINEID not in 
    (select airlineid from airlines);
    
delete from routes
where AIRLINEID not in 
    (select airlineid from airlines) 
    or routeid < 0 
    or distance < 0 
    or servicecost < 0;
