# find . -name \*.bz2 -exec bzip2 -d \{\} \;


Helpful commands:
  docker exec -it -u root jupyterlab /bin/bash
    docker exec -it -u root jupyterlab sh
  docker rmi $(docker images -a -q)
  docker image prune -a
  docker rm -f $(docker ps -a -q)
  docker exec -it jupyterlab /opt/conda/bin/jupyter notebook list
  docker logs --tail 100 --follow --timestamps jupyterlab



    #1a66dd36ff82 3.8.4
    #7d0e50e30763 3.8.5
    #229c7fea9d60 
    #5197709e9f23 
    #04f7f60d34a6 3.7.6
    #c094bb7219f9 
    #b90cce83f37b 
    #54462805efcb
    #image: jupyter/pyspark-notebook:1a66dd36ff82