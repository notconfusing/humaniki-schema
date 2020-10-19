## Installation 


### define an .env file
- copy .env.sample to .env
- then `pipenv install`
- `pipenv shell`

### mysql8.0
- Following instructions from 
- `docker run -p 3306:3306 --name some-mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:latest`
- `docker exec -it some-mysql bash`
- `mysql -uroot -h localhost -p` #from inside the docker container 
- CREATE DATABASE `humaniki` DEFAULT CHARACTER SET 'utf8mb4';
- CREATE USER 'humaniki'@'localhost' IDENTIFIED BY 'xxxxxxx'; #exclude identified by for brew no password
- CREATE USER 'humaniki'@'127.0.0.1' IDENTIFIED BY 'xxxxxxx'; #exclude identified by for brew no password
- GRANT ALL ON `humaniki`.* TO 'humaniki'@'localhost';
- GRANT ALL ON `humaniki`.* TO 'humaniki'@'127.0.0.1'; #double check for 172.0.xxx aka docker container IP address 
- GRANT FILE on *.* to 'humaniki'@'localhost';

-docker start some-mysql 

### alembic
- alembic init alembic


### starting mysql server
docker start some-mysql

### sql config
in my.cnf
secure-file-priv= ""



## Provenance of example data
1. most example data came from querying an old version of denelezh, until that very inopportunely went down. then I used Wikdiata Query service to create 
2. country labels and iso codes
3. and occupations
```
#List of present-day countries and capital(s)
SELECT DISTINCT ?country ?countryLabel ?iso_3166_1
WHERE
{
  ?country wdt:P31 wd:Q3624078 .
  #not a former country
  FILTER NOT EXISTS {?country wdt:P31 wd:Q3024240}
  #and no an ancient civilisation (needed to exclude ancient Egypt)
  FILTER NOT EXISTS {?country wdt:P31 wd:Q28171280}
  ?country wdt:P297 ?iso_3166_1 .

  SERVICE wikibase:label { bd:serviceParam wikibase:language "fr" }
}
ORDER BY ?countryLabel
```
```
SELECT distinct ?occupation ?occupationLabel
WHERE
{
  ?occupation wdt:P31 wd:Q12737077
  SERVICE wikibase:label { bd:serviceParam wikibase:language "fr". }
}
limit 2000
```

## troubleshooting 

- If you can't connect to database check if docker container is running 
- `docker ps`
-If the container isn't running you need to run the following: 
-`docker start some-mysql`
- You should see an entry to some-mysql

-Then try to connect to my-sql using the command line client with the humaniki user
-`mysql -uhumaniki -h 127.0.0.1 `

-To run flask app: 
-Add configuration for file: python app 
-`pipenv shell` #loads our specific python with .env environment variables 
-`python humaniki_backend/app.py` #runs the flask app entry point
-or just run the configuration from PyCharm
