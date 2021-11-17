function Wait-KeyPress
{
    param
    (
        [ConsoleKey]
        $Key = [ConsoleKey]::Escape
    )
    do
    {
        $keyInfo = [Console]::ReadKey($false)
    } until ($keyInfo.Key -eq $key)
}

function isMongoOnline($port)
{
    $ipaddress = "127.0.0.1"

    try {
        $connection = New-Object System.Net.Sockets.TcpClient($ipaddress, $port)
        if ($connection.Connected) {
                return $true
        }
    }
    catch {
       return $false
    }

}

Write-Host "Checking to see if MongoDB is running..."
if(isMongoOnline 27017) {
    Write-Host "MongoDB is running on port 27017, please terminate the local mongod on this port." -ForegroundColor Yellow
    Exit 1
}
else {
    Write-Host "No MongoDB running..."
}

Write-Host  "Starting docker ."

docker-compose up -d --build

Write-Host  "Configuring the MongoDB ReplicaSet."

docker-compose exec mongo1 /usr/bin/mongo --eval "if (rs.status()['ok'] == 0) { rsconf = { _id : 'rs0', members: [ { _id : 0, host : 'mongo1:27017', priority: 1.0 }, { _id : 1, host : 'mongo2:27017', priority: 0.5 }, { _id : 2, host : 'mongo3:27017', priority: 0.5 } ] }; rs.initiate(rsconf); } rs.conf();"


Write-Host  "

==============================================================================================================

MongoDB Spark Demo

Jypterlab"

docker exec -it jupyterlab /opt/conda/bin/jupyter notebook list

Write-Host  -ForegroundColor Yellow "NOTE: When running on Windows change 0.0.0.0 to localhost in the URL above"

Write-Host "

Spark Master - http://localhost:8080

Spark Worker 1

Spark Worker 2

MongoDB Replica Set - port 27017-27019

==============================================================================================================

Press <Escape> to quit"

Wait-KeyPress
 
# if we don't specify -v then issue this one -> docker-compose exec mongo1 /usr/bin/mongo localhost:27017/SparkDemo --eval "db.dropDatabase()"

#docker-compose down -v
docker-compose down

# note: we use a -v to remove the volumes, else you'll end up with old data
