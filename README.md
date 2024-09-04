### MSSQL > PostgreSQL Migration Project

This project is a Node.js project that facilitates the transfer of data from MSSQL to PostgreSQL.

Create a docker network.

Create an mssql instance running in docker network.

The program needs to be built using the commands as follows:

docker build -t {app_name} .

docker run --name {app_name} --network {network_name} -d {app_name}