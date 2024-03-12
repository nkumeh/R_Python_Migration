---
title: "R Notebook"
output: html_notebook
author: Kristine Umeh, Shital Waters
email: umeh.k@northeastern.edu, waters.s@northeastern.edu
---


#1
# ```{r}
# 1. Library
# library(RMySQL)
# library(RSQLite)
library(lubridate)
library(reader)
library(base)
library(tidyverse)
library(anytime)


# 2. Settings
db_user <- 'admin'
db_password <- 'SQLlite10!'
db_name <- 'practicumDB'
db_host <- 'practicumdb.cdzicwecvvdd.us-east-2.rds.amazonaws.com' # AWS Host
db_port <- 3306

# 3. Read data from db
mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                 dbname = db_name, host = db_host, port = db_port)

```


Must drop parents before dropping kids
```{sql connection=mydb}
  DROP TABLE IF EXISTS Incident
```


```{sql connection=mydb}
DROP TABLE IF EXISTS Airport
```


```{sql connection=mydb}
DROP TABLE IF EXISTS Airline

```


Airport look-up table with FK references in pointing to the Incident table and 
unknown attributes set to 'unknown' all airport names are assumed to be unique
```{sql connection=mydb}
CREATE TABLE Airport (
  code VARCHAR(45) NULL DEFAULT 'unknown',
  airportName VARCHAR(45) NOT NULL DEFAULT 'unknown',
  city VARCHAR(45) NOT NULL DEFAULT 'unknown',
  state VARCHAR(45) NOT NULL DEFAULT 'unknown',
  country VARCHAR(45) NULL DEFAULT 'unknown',
  pid INT NOT NULL,
  PRIMARY KEY(pid)
  )
```


Airline look-up table with FK references in pointing to the Incident table and 
unknown attributes set to 'unknown', airline name is assumed to be unique
```{sql connection=mydb}
CREATE TABLE Airline (
  code VARCHAR(45) NOT NULL DEFAULT 'unknown',
  airlineName VARCHAR(45) NOT NULL DEFAULT 'unknown',
  aid INT NOT NULL, 
  PRIMARY KEY(aid)
  )
```



Creating Incident table, the parent table to hold incidences 
original headers have been renamed 
arrPort and depPort have been consolidated to a single port since they are 
assumed to be the same per the problem statement
and all unknown attributes have been set to 'unknown'

```{sql connection=mydb}
CREATE TABLE Incident (
  iid INT NOT NULL,
  flightDate DATE DEFAULT NULL,
  port INT NOT NULL,
  airline INT NOT NULL,
  aircraft VARCHAR(45) NOT NULL DEFAULT 'unknown',
  phase VARCHAR(12) NOT NULL DEFAULT 'unknown',
  impact VARCHAR(45) NOT NULL DEFAULT 'unknown',
  
  PRIMARY KEY(iid),
  FOREIGN KEY (airline) REFERENCES Airline(aid),
  FOREIGN KEY (port) REFERENCES Airport(pid),
  CHECK (phase IN ('takeoff', 'landing', 'inflight', 'unknown'))
  )
```


#2
making sure no duplicate Record ID 
```{r}
bsDF <- read_csv("BirdStrikesData.csv")

bsDF[!duplicated(bsDF$`Record ID`), ] #remove duplicates

```


Using a hashmap
```{r}
library(hash)
flightPhaseMap <- hash()
flightPhaseMap[["Take-off run"]] <- "takeoff"
flightPhaseMap[["Landing Roll"]] <- "landing"
flightPhaseMap[["Climb"]] <- "inflight" # takeoff
flightPhaseMap[["Approach"]] <- "inflight"
flightPhaseMap[["Descent"]] <- "inflight"
flightPhaseMap[["Taxi"]] <- "takeoff"
flightPhaseMap[["Parked"]] <- "unknown"
flightPhaseMap[[" "]]<- "unknown"
```


function to harmonise phase values
```{r}
phaseHarmonizer <- function(flightPhase){
if (is.null(flightPhase)){
return("unknown")
}
if (has.key(flightPhase,flightPhaseMap)){
return(flightPhaseMap[[flightPhase]])
}
return("unknown")
}

```

function to check anomalies
```{r}

num_rows <- nrow(bsDF)
for (i in 1:num_rows){
  if(is.na(bsDF[[i,14]]) | bsDF[[i,14]] == '' )
    bsDF[[i,14]] == "unknown"
  else if (is.null(bsDF[[i,14]]))
    bsDF[[i,14]] == "unknown"
  else if (bsDF[[i,14]] == ""){
    bsDF[[i,14]] <- "unknown"
  }
  else{
    temp <- bsDF[[i,14]]
    temp <- phaseHarmonizer(temp)
    bsDF[[i,14]] <- temp
  }
}
```

assigning dataframes
```{r}
print(typeof(bsDF))
print(typeof(bsDF[,12]))


#this is for creation of the primary keys
airlines <- data.frame(airlineName = bsDF$`Aircraft: Airline/Operator`)

airlinesFull <- data.frame(airline = distinct(bsDF[,12])) #look up table for airlines
airlinesFull <- transmute(airlines, airlineName= airlineName, aid=1:n())

airports <- data.frame(airportName = bsDF$`Airport: Name`)
airports$state <- bsDF$`Origin State`

airportsFull <- data.frame(airportName = bsDF[,3], state=bsDF[,13])
airportsFull <- distinct(airportsFull, `Airport..Name`, .keep_all = TRUE)
airportsFull <- transmute(airports, airportName = airportName, 
                          state= state, pid=1:n())


incidentsFull <- transmute(bsDF, iid = `Record ID`, flightDate=anydate(`FlightDate`), 
                           aircraft=`Aircraft: Make/Model`, 
                           phase=`When: Phase of flight`, 
                           airline = airlines$airlineName, 
                           port = airports$`airportName`,
                           impact=bsDF$`Effect: Impact to flight`)
```


Joining the incident table to airport and airline with join clause
```{r}
incidentsFull <- left_join(incidentsFull, airportsFull, by = `Airport: Name`)
incidentsFull <- inner_join(incidentsFull, airlinesFull, by = "airlineName")
```


```{r}
print(incidentsFull)
#print(airlinesFull)
```



Connecting the dataframe as a database
```{r}
dbListTables(mydb)
```



delete all rows first , append f 
```{r}
dbWriteTable(mydb, 'Airline', airlinesFull, overwrite=F, append=T, row.names = F)

```

```{sql connection=mydb}
select * from Airline
```



```{r}
dbWriteTable(mydb, "Airport", airportsFull, overwrite=F, append=T, row.names = F)

```

```{sql connection=mydb}
select * from Airport
```


```{r}
dbWriteTable(mydb, "Incident", incidentsFull, overwrite=F, append=T, row.names = F)

```



```{sql connection=mydb}
select * from Incident
```



```{sql connection=mydb}
describe Incident
```



```{sql connection=mydb}
SELECT
  SUM(Incident.iid) AS 'NumBirdStrikes',
  Airline.airlineName,
  Airport.airportName,
  Incident.phase
FROM
  Incident
  join Airline on Incident.airline = Airline.airlineName
  join Airport on Incident.arrPort = Airport.airportName
WHERE
  Airport.airportName like 'LaGuardia'
GROUP BY
  Airline.airlineName,
  Airport.airportName,
  Incident.Phase
ORDER BY
  NumBirdStrikes DESC


```




```{sql connection=mydb}
SELECT
SUM(Incident.iid) AS 'NumBirdStrikes',
Airline.airlineName,
Airport,
Incident.phase,

FROM
Incident join Airline on Incident.airline = Airline.airlineName join
Airport on Incident.arrPort = Airport.airportName

WHERE Airline.airlineName NOT LIKE 

AND 'business' OR 'private', OR 'military' AND
NumBirdStrikes = (SELECT MAX(NumBirdStrikes) FROM Incident)

GROUP BY port, airports.pid, airports.name
order by count_of_incidents
Airport,
Incident.phase

```


```{sql connection=mydb}
SELECT
year(dateOnly) as "YEAR"
count * as "bird_strike_data"
from
incident
group by year(dateOnly)
order by year(dateOnly)


```






```{r}
library(tidyverse);
library(sqldf)

frame <- dbGetQuery(mydb, "Select * ")
ggplot(data = tdf, mapping = aes(x=ItemNumber, y=Total)) + geom_point() 

```



```{sql connection=mydb}
create deleteAirport (in airportID int)
BEGIN
  delete 
  from airports
    where airport.pid = airportID;
  delete
    from airlines.aid = ailineID
  END;
```


```{sql connection=mydb}
call deleteAirport(12);
```

```{sql connection=mydb}
select * from airline
where airline.id = 15
```

```{sql connection=mydb}
select * Airport
where airport.pid = 15
```

```{sql connection=mydb}
drop procedure if exists deleteAirport
```



```{r}
dbDisconnect(mydb)
```