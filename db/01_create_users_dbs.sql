
CREATE USER trafficuser WITH PASSWORD 'trafficpass';
CREATE USER airflowuser WITH PASSWORD 'airflowpass';

CREATE DATABASE trafficdb OWNER trafficuser;
CREATE DATABASE airflowdb OWNER airflowuser;

GRANT ALL PRIVILEGES ON DATABASE trafficdb TO trafficuser;
GRANT ALL PRIVILEGES ON DATABASE airflowdb TO airflowuser;