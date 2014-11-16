# Run the Models
FILENAME=`date +20%y_%m%d_%H%M`

python ./BlueToadAnalysis.py today 'similar_dow.json' -w -t & > similar_dow_log.json
if [[ $(date +%u) -gt 5 ]] ; then
  python ./BlueToadAnalysis.py weekend 'similar_weekends.json' -w -t & > similar_weekends_log.json
else
  python ./BlueToadAnalysis.py weekday 'similar_weekdays.json' -w -t & > similar_weekdays_log.json
fi
wait
echo "model runs complete"

# Compress, Archive, and Upload the Model Outputs
gzip -9 --force update/similar_dow.json
mv update/similar_dow.json.gz update/similar_dow_$FILENAME.json.gz
aws s3 cp update/similar_dow_$FILENAME.json.gz s3://www.traffichackers.com/data/predictions/similar_dow.json --region us-east-1 --content-encoding gzip --content-type application/json &
if [[ $(date +%u) -gt 5 ]] ; then
  gzip -9 --force update/similar_weekends.json
  mv update/similar_weekends.json.gz update/similar_weekends_$FILENAME.json.gz
  aws s3 cp update/similar_weekends_$FILENAME.json.gz s3://www.traffichackers.com/data/predictions/similar_weekends.json --region us-east-1 --content-encoding gzip --content-type application/json &
else
  gzip -9 --force update/similar_weekdays.json
  mv update/similar_weekdays.json.gz update/similar_weekdays_$FILENAME.json.gz
  aws s3 cp update/similar_weekdays_$FILENAME.json.gz  s3://www.traffichackers.com/data/predictions/similar_weekdays.json --region us-east-1 --content-encoding gzip --content-type application/json &
fi
wait
echo "compression, archiving, and upload complete"
