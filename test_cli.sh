sudo ifconfig lo0 alias 127.0.0.2 up
sudo ifconfig lo0 alias 127.0.0.3 up

ccm start

cd ~/code/atlasdb4
rm -rf atlasdb-cli-distribution/build
ls | grep "atlasdb-cli-0.70.0.*" | xargs rm -rf

./gradlew distTar

tar -xvf atlasdb-cli-distribution/build/distributions/`ls atlasdb-cli-distribution/build/distributions`

cd `find . -iname atlasdb-cli-0.70.0-*`/service/bin

cp ~/code/atlasdb4/atlasConfig.yml .

./atlasdb-cli --offline -c atlasConfig.yml timestamp  --timestamp 4060 clean-transactions




