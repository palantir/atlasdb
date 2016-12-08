FROM tjake/jepsen:latest

ADD . /jepsen/atlasdb

CMD wrapdocker bash -c 'source ~/.bashrc && cd /jepsen/atlasdb && lein test'
