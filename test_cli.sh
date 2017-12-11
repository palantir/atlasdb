cd ~/code/atlasdb4
rm -rf atlasdb-cli-distribution/build
rm -rf "atlasdb-cli-distribution-0.70.0.*"

./gradlew distTar

tar -xvf atlasdb-cli-distribution/build/distributions/`ls atlasdb-cli-distribution/build/distributions`

cd `find . -iname atlasdb-cli-0.70.0-*`/service/bin

cp ~/code/atlasdb4/atlasConfig.yml .

./atlasdb-cli --offline -c atlasConfig.yml timestamp  --timestamp 4060 clean-transactions




