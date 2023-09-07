## Project Context
NSW Government seeks to provide targetted financial assistance to residents of NSW to ease cost of living pressures.

## Architecture
![images/architecture.png](images/architecture.png)

## Installation / Running Instructions

### 1. Clone Git Repository
In terminal:

    git clone https://github.com/zeisaaclow/dec-group-6-fuel-etl.git
### 2. Build Docker image
Ensure aws login has been configured.
 
    aws ecr get-login-password --region [##region##] | docker login --username AWS --password-stdin [##AWS-ID##.dkr.ecr.##region##]
    docker build -t dec-project-1 .
    docker tag dec-project-1:latest [##AWS-ID##.dkr.ecr.##region##]/dec-project-1:latest
    
### 3. Push Docker image to ECR
    docker push [##AWS-ID##.dkr.ecr.##region##]/dec-project-1:latest
### 4. Set up .env file in S3
    TARGET_DATABASE_NAME=
    TARGET_SERVER_NAME=
    TARGET_DB_USERNAME=
    TARGET_DB_PASSWORD=
    TARGET_PORT=

    LOGGING_DATABASE_NAME=
    LOGGING_SERVER_NAME=
    LOGGING_USERNAME=
    LOGGING_PASSWORD=
    LOGGING_PORT=

    API_KEY=[fuel api key]
    AUTHORIZATION=[fuel api authorization]
### 5. Configure ECS
 
