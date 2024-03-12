---
title: "R Notebook"
output: html_notebook
---
```{r}
# Loading package
install.packages("sqldf")
install.packages("XML")
install.packages("anytime")
install.packages("xml2")
install.packages("dplyr")
library(sqldf)
library(XML)
library(anytime)
```
```{r}
# 1. Library
library(RMySQL)
library(DBI)
# 2. Settings
db_user <- 'admin'
db_password <- 'SQLlite10!'
db_name <- 'database2'
db_host <- 'database2.cdzicwecvvdd.us-east-2.rds.amazonaws.com'
db_port <- 3306

# 3. Read data from db
mydb <- dbConnect(MySQL(), user = db_user, password = db_password,
dbname = db_name, host = db_host, port = db_port)
```

```{sql connection=mydb}
DROP TABLE IF EXISTS author_article;
```

```{sql connection=mydb}
DROP TABLE IF EXISTS author;
```

```{sql connection=mydb}
DROP TABLE IF EXISTS article;
```

```{sql connection=mydb}
DROP TABLE IF EXISTS journal;
```

```{sql connection=mydb}
DROP TABLE IF EXISTS affiliation;
```

```{sql connection=mydb}
CREATE TABLE affiliation(
  afid INT NOT NULL,
  affiliation VARCHAR(500) NOT NULL DEFAULT 'unknown',
  PRIMARY KEY (afid)
)
```

```{sql connection=mydb}
CREATE TABLE journal (
  issn VARCHAR(200) NOT NULL,
  name VARCHAR(200) UNIQUE NOT NULL DEFAULT 'unknown',
  volume INTEGER,
  issue INTEGER,
  publication_date DATE,
  PRIMARY KEY (issn)
);
```

```{sql connection=mydb}
create table author(
aid INT NOT NULL AUTO_INCREMENT,
last_name VARCHAR(200)NOT NULL DEFAULT 'unknown',
first_name VARCHAR(200)NOT NULL DEFAULT 'unknown',
initial VARCHAR(200) NOT NULL DEFAULT 'unknown',
afid INT,
PRIMARY KEY (aid),
FOREIGN KEY (afid) REFERENCES affiliation(afid)
);

```

```{r}
path <- "C:/Users/crosb/OneDrive/Desktop/Northeastern/Databases/Practicum2/"
xmlFile <- "pubmed_sample.xml"
fp <- paste0(path,xmlFile)
xmlObj <- xmlParse(fp)
xmlObjTree <- xmlTreeParse(fp)
```

```{r}
library(xml2)
x <- read_xml(fp)

auths <- xml_find_all(x,".//Author")
auths <- as_list(auths)

#I created separate variables to hold the data in XML nodeset form, and list form 
#respectively. This is needed for the aggregate pmid/authors table below
articlesxml <- xml_find_all(x,".//MedlineCitation")
articles <- as_list(articlesxml)


```
Aggregate Table with article PMID and Authors. Each MedlineCitation node is traversed for a PMID
and then the inner list of authors is traversed to extract data appropriately.
```{r}
a_size <- length(articles)
au_df_agg <-(data.frame(pmid = integer(),last_name=character(),first_name=character(),
                        initial=character(),affiliation=character()))
#for every Article in the XML
for (var in 1:a_size){
  pmid <- toString((unlist(articles[var])["PMID"]))
  local_auths <- xml_find_all(articlesxml[var], ".//Author")
  local_auths <- as_list(local_auths)
  #for every author in the author list of the article
  for (author in 1:length(local_auths)){
     f_name <- toString(unlist(local_auths[author])["ForeName"])
     l_name <- toString(unlist(local_auths[author])["LastName"])
     initials <- toString(unlist(local_auths[author])["Initials"])
     affil <- toString(unlist(local_auths[author])["Affiliation"])
     r <- c(pmid,l_name, f_name,initials,affil)
     au_df_agg[nrow(au_df_agg)+1,] <-r
  }
 
  #print(length(local_auths))
}
print(au_df_agg)
```

##This Logic will extract all needed values from Authors and place them in an R Dataframe
```{r}

au_df <-(data.frame(last_name =character(),first_name=character(),initial=character(),affiliation=character()))
size <- length(auths)

for (var in 1:size){
  #pmid <- as.integer((unlist(articles[var])["PMID"]))
  aid_val <- var
  f_name <- toString(unlist(auths[var])["ForeName"])
  l_name <- toString(unlist(auths[var])["LastName"])
  initials <- toString(unlist(auths[var])["Initials"])
  affil <- toString(unlist(auths[var])["Affiliation"])
  
  
  r <- c(l_name, f_name,initials,affil)
  au_df[nrow(au_df)+1,] <-r
}

#au_df_agg_test <- au_df_agg_test %>% distinct(, .keep_all = TRUE)
au_df <- au_df %>% distinct(last_name,first_name,initial, .keep_all=TRUE)
#au_df <- (transmute(au_df,))
#affiliation_table <- (transmute(affiliation_table,affiliation = unique(affil), afid = 1:n()))

au_df
```
```{r}
library(anytime)
articles <- xml_find_all(x,".//MedlineCitation")
articles <- as_list(articles)
```
#Custom Date Function to convert dates
```{r}
library(anytime)
dateIt <- function(ys,ms,ds){
  tmp <- toString((anytime(c(paste(ys,ms,ds,sep="-")))))
  return(tmp)
}
```

```{r}
dateDifference <- function(date_created,pub_date,quarter){
  q = as.integer(substr(quarters(as.Date(pub_date)), 2, 2))
  #print(typeof(q))
  #print(q)
  if (quarter == q)
     return(as.integer(difftime(as.Date(pub_date),as.Date(date_created),units = "days")))
  else
    return(0)
 
}
```

```{r}
t <- "2013-07-22"
t2<- "2013-07-29"

dateDifference(t,t2,4)
```

##This chunk parses the XML file and puts the attributes for the article table in an R dataframe
```{r}
a_size <- length(articles)
article_df <-(data.frame(pmid =integer(),articleTitle=character(),dateCreated=character()))
for (var in 1:a_size){
  pmid <- as.integer((unlist(articles[var])["PMID"]))
  date_createdy <- toString(unlist(articles[var])["DateCreated.Year"])
  date_createdm <- toString(unlist(articles[var])["DateCreated.Month"])
  date_createdd <- toString(unlist(articles[var])["DateCreated.Day"])
  article_title <- toString(unlist(articles[var])["Article.ArticleTitle"])
  ds <- dateIt(date_createdy,date_createdm,date_createdd)
  
  r <- c(pmid, article_title, ds)
  article_df[nrow(article_df)+1,] <-r
}
print(article_df)

```
 
##This R chunk traverses the XML file and grabs attributes for the Journal Table. They are
##then stored in a dataframe
```{r}
journals <- xml_find_all(x,".//Journal")
journals <- as_list(journals)
journal_pubdates <- xml_find_all(x,".//History")
j_size <- length(journals)
journal_df <-(data.frame(pmid = integer(),issn=integer(),name=character(),volume=integer(),issue=integer(),publication_date=character()))
for (var in 1:j_size){
  pmid <- as.integer((unlist(articles[var])["PMID"]))
  issn <- toString((unlist(journals[var])["ISSN"]))
  name <- toString(unlist(journals[var])["Title"])
  volume <- toString(unlist(journals[var])["JournalIssue.Volume"])
  issue <- toString(unlist(journals[var])["JournalIssue.Issue"])
  
  l <- as_list(xml_child(journal_pubdates[var],length(xml_children(journal_pubdates[var]))))
  y <- toString(unlist(l["Year"]))
  m <-toString(unlist(l["Month"]))
  d <-toString(unlist(l["Day"]))
  pubdate <-paste(y,m,d,sep="-")
 
  r <- c(pmid,issn,name,volume,issue,pubdate)
  journal_df[nrow(journal_df)+1,] <- r
}

journal_df <- journal_df[order(journal_df$issn),]
un_journaldf <- journal_df %>% distinct(issn, .keep_all = TRUE)
un_journaldf <- un_journaldf[order(un_journaldf$issn),]
print(un_journaldf)
print(journal_df)
```

```{r}
journal_fact <- un_journaldf
journal_fact <- journal_fact %>% mutate(Quarter = as.numeric(substr(quarters(as.Date(publication_date)), 2, 2)))
journal_fact <- journal_fact %>% mutate(Year = as.numeric(format(as.Date(publication_date),"%Y")))
journal_fact <- left_join(article_df,journal_fact, by="pmid")
journal_fact <-  journal_fact[order(journal_fact$issn),]
journal_fact<- subset(journal_fact,select = -c(volume,issue,articleTitle))
journal_fact <- journal_fact[!duplicated(as.list(journal_fact))]
journal_fact
```

#Stuff below here uses xpath to extract normalized tables, like affiliations but the raw tables for author, articles,
#and journals are below

```{r}
#PubmedArticle$MedlineCitation$Article$AuthorList$Author$LastName
data <- xmlToList(xmlObj)
#print(head(data,1))
```

```{r}
authorlists <- as.list(data[["PubmedArticle"]][["MedlineCitation"]][["Article"]][["AuthorList"]])
print(authorlists)
```


This chunk uses xpath to extract attributes for the Article 
```{r}
#Xpath expressions
pmidxpath <- "//PMID[@Version=1]"

datexpath <-"//DateCreated"

titleXpath <- "//Article/ArticleTitle"
priceXpath <- "//"

#xml Objects
pmids <- xpathSApply(xmlObj,pmidxpath,xmlValue)

dateObj <- xpathSApply(xmlObj,datexpath,xmlValue)

titleObj <- xpathSApply(xmlObj,titleXpath,xmlValue)

#print(pmids)
#head(pmids)
#head(dateObj)
#print(dDay)
#print(anydate(dateObj[1]))
#head(titleObj)
```
##This chunk gets the attributes for the author table
```{r}
affiliationxpath <- "//Article/AuthorList/Author/Affiliation"
firstnamexpath <- "//Article/AuthorList/Author/ForeName"
initialxpath <- "//Article/AuthorList/Author/Initials"
lastnamexpath <- "//Article/AuthorList/Author/LastName"

affil <- xpathSApply(xmlObj,affiliationxpath,xmlValue)
first <- xpathSApply(xmlObj,firstnamexpath,xmlValue)
initials <- xpathSApply(xmlObj,initialxpath,xmlValue)
last <- xpathSApply(xmlObj,lastnamexpath,xmlValue)

print(length(affil))
print(length(first))
print(length(initials))
print(length(last))
```
##Affiliation Table
```{r}
library(dplyr)
affiliation_table <- (data.frame(affiliation = unique(affil)))
affiliation_table <- (transmute(affiliation_table,affiliation = unique(affil), afid = 1:n()))
print(affiliation_table)
```

```{r}
author_aggdf <- left_join(au_df,affiliation_table,by="affiliation")
author_aggdf
```

Big aggregate Data Frame
```{r}
au_df_agg_test <- left_join(au_df_agg,affiliation_table, by ="affiliation")

au_df_agg_test <- left_join(au_df_agg_test,article_df,by = "pmid")
au_df_agg_test <-left_join(au_df_agg_test,journal_df,by="pmid")

au_df_agg_test_unique <- au_df_agg_test %>% distinct(pmid, .keep_all = TRUE)
au_df_agg_fact <- au_df_agg_test %>% distinct(pmid,issn, .keep_all = TRUE)
au_df_agg_fact <- au_df_agg_fact[!duplicated(as.list(au_df_agg_fact))]
au_df_agg_fact <-  subset(au_df_agg_fact, select = -c(1,2,3,4,5,6,7,11,12))

fact_table <- au_df_agg_fact[,c(2,3,1,4)][order(au_df_agg_fact$issn),]
#fact_table <- fact_table %>% mutate(Quarter = as.numeric(substr(quarters(as.Date(publication_date)), 2, 2)))
#fact_table <- fact_table %>% mutate(q1 = as.integer(difftime(as.Date(publication_date),as.Date(dateCreated),units = "day")))
#fact_table <- fact_table %>% mutate(q1 = dateDifference(dateCreated,publication_date,1),units = "day")
#journal_fact <- journal_fact %>% mutate(Year = as.numeric(format(as.Date(publication_date),"%Y")))
fact_table<- fact_table %>% mutate(Year = as.numeric(format(as.Date(dateCreated), "%Y")))
fact_table$numArticles <- 1
fact_table <- fact_table %>% mutate(diffs = as.integer(difftime(as.Date(publication_date),as.Date(dateCreated),units = "day")))
fact_table <- fact_table %>% mutate(q1 = as.integer(difftime(as.Date(publication_date),as.Date(dateCreated),units = "day")))
fact_table <- fact_table %>% mutate(q2 = as.integer(difftime(as.Date(publication_date),as.Date(dateCreated),units = "day")))
fact_table <- fact_table %>% mutate(q3 = as.integer(difftime(as.Date(publication_date),as.Date(dateCreated),units = "day")))
fact_table <- fact_table %>% mutate(q4 = as.integer(difftime(as.Date(publication_date),as.Date(dateCreated),units = "day")))
fact_table$q1 = ifelse(as.numeric(substr(quarters(as.Date(fact_table$publication_date)), 2, 2)) == 1,fact_table$diffs,0)
fact_table$q2 = ifelse(as.numeric(substr(quarters(as.Date(fact_table$publication_date)), 2, 2)) == 2,fact_table$diffs,0)
fact_table$q3 = ifelse(as.numeric(substr(quarters(as.Date(fact_table$publication_date)), 2, 2)) == 3,fact_table$diffs,0)
fact_table$q4 = ifelse(as.numeric(substr(quarters(as.Date(fact_table$publication_date)), 2, 2)) == 4,fact_table$diffs,0)

fact_table <- fact_table[,c(1,5,3,4,6,7,8,9,10,11)]
#fact_table has all data but there are duplicate issn rows - each representing an article
fact_table
merge_test <- fact_table[,c(1,5,7,8,9,10)]
#This is the thing that worked =)
merge_testdf <- aggregate(x = merge_test[ , colnames(merge_test) != "issn"], by = list(merge_test$issn),FUN = sum)
merge_testdf$avg_elap_days_yr <- rowSums(cbind(merge_testdf$q1,merge_testdf$q2,merge_testdf$q3,merge_testdf$q4))/
  merge_testdf$numArticles
merge_testdf$q1 <- merge_testdf$q1 / merge_testdf$numArticles
merge_testdf$q2 <- merge_testdf$q2 / merge_testdf$numArticles
merge_testdf$q3 <- merge_testdf$q3 / merge_testdf$numArticles
merge_testdf$q4 <- merge_testdf$q4 / merge_testdf$numArticles
#THIS IS THE FINAL FACT TABLE!! (merge_testdf)
merge_testdf

unique_facts <- fact_table %>% distinct(issn, .keep_all = TRUE)
a <- rle(sort(fact_table$issn))
b <- data.frame(number = a$values,n=a$lengths)
unique_facts$numArticles = b$n
#No duplicate ISSNs in unique_facts
#unique_facts
```

#Auxillary drop statements
```{sql connection=mydb}
DROP TABLE IF EXISTS journalAUX
```

```{sql connection=mydb}
DROP TABLE IF EXISTS affiliationAUX
```

```{sql connection=mydb}
DROP TABLE IF EXISTS authorAUX
```

```{sql connection=mydb}
DROP TABLE IF EXISTS articleAUX
```

```{r}
dbWriteTable(mydb,"affiliationAUX",affiliation_table,overwrite=F,append=T)
dbWriteTable(mydb,"authorAUX",author_aggdf,overwrite=F,append=T)
dbWriteTable(mydb, "articleAUX", au_df_agg_test_unique,overwrite = F, append=T)
dbWriteTable(mydb,"journalAUX",un_journaldf,overwrite=F,append=T)
```

## Inserting data into tables from auxillary tables
```{sql connection=mydb}
INSERT INTO affiliation(afid,affiliation) SELECT afid,affiliation FROM affiliationAUX;
```

```{sql connection=mydb}
INSERT INTO journal(issn,name,volume,issue,publication_date) SELECT issn,name,volume,issue,publication_date FROM journalAUX;
```

```{sql connection=mydb}
INSERT INTO article(pmid,issn,article_title,date_created) SELECT pmid, issn,articleTitle,dateCreated FROM articleAUX
```

```{sql connection=mydb}
INSERT INTO author(last_name,first_name,initial,afid) SELECT last_name,first_name,initial,afid FROM authorAUX;
```

Lets look at the SQL tables in the AWS database
```{sql connection=mydb}
SELECT * FROM journal

```

```{sql connection=mydb}
SELECT * FROM affiliation
```

```{sql connection=mydb}
SELECT * FROM author
```

```{sql connection=mydb}
SELECT * FROM article
```

