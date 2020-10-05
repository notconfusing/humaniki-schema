## Installation 


### define an .env file
- copy .env.sample to .env
- then `pipenv install`
- `pipenv shell`

### mysql8.0
- Following instructions from 
- `docker run -p 3306:3306 --name some-mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:latest`
- CREATE DATABASE `humaniki` DEFAULT CHARACTER SET 'utf8mb4';
- CREATE USER 'humaniki'@'localhost' IDENTIFIED BY 'xxxxxxx'; #exclude identified by for no password
- GRANT ALL ON `humaniki`.* TO 'humaniki'@'localhost';
- GRANT FILE on *.* to 'humaniki'@'localhost';


### alembic
- alembic init alembic


### starting mysql server
docker start some-mysql

### sql config
in my.cnf
secure-file-priv= ""
