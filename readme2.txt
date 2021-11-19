# find . -name \*.bz2 -exec bzip2 -d \{\} \;


Helpful commands:
  docker exec -it -u root jupyterlab /bin/bash
    docker exec -it -u root jupyterlab sh
  docker rmi $(docker images -a -q)
  docker image prune -a
  docker rm -f $(docker ps -a -q)
  docker exec -it jupyterlab /opt/conda/bin/jupyter notebook list
  docker logs --tail 100 --follow --timestamps jupyterlab