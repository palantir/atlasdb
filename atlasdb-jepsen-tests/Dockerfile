FROM tjake/jepsen:latest

ADD . /jepsen/atlasdb

CMD wrapdocker bash -c 'docker rm $(docker ps -a -q) 2>/dev/null; source ~/.bashrc && cd /jepsen/atlasdb && lein test'
