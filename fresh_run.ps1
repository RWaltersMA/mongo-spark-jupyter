# Fetch test data
Write-Host  "Uploading test data into Stocks database"
docker-compose exec mongo1 apt-get update
docker-compose exec mongo1 apt-get install wget
docker-compose exec mongo1 wget https://github.com/RWaltersMA/mongo-spark-jupyter/raw/master/Source.bson


Write-Host  "Running mongorestore Source.bson"
Start-Sleep -s 5
docker-compose exec mongo1 /usr/bin/mongorestore Source.bson -h rs0/mongo1:27017,mongo2:27018,mongo3:27019 -d Stocks -c Source --drop
