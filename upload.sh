# Run the Models
python ./BlueToadAnalysis.py today 'similar_dow.json' -w -t & > similar_dow_log.json
if [[ $(date +%u) -gt 5 ]] ; then
  python ./BlueToadAnalysis.py weekend 'similar_weekends.json' -w -t & > similar_weekends_log.json
else
  python ./BlueToadAnalysis.py weekday 'similar_weekdays.json' -w -t & > similar_weekdays_log.json
fi
wait
echo "model runs complete"

# Compress the Model Outputs
gzip -9 --keep --force update/similar_dow.json &
if [[ $(date +%u) -gt 5 ]] ; then
  gzip -9 --keep --force update/similar_weekends.json &
else
  gzip -9 --keep --force update/similar_weekdays.json &
fi
wait
echo "compression complete"

# Upload to the Amazon Web Services S3 Store
aws s3 cp update/similar_dow.json.gz s3://traffichackers/data/predictions/similar_dow.json --region us-east-1 --content-encoding gzip --content-type application/json &
if [[ $(date +%u) -gt 5 ]] ; then
  aws s3 cp update/similar_weekends.json.gz s3://traffichackers/data/predictions/similar_weekends.json --region us-east-1 --content-encoding gzip --content-type application/json &
else
  aws s3 cp update/similar_weekdays.json.gz  s3://traffichackers/data/predictions/similar_weekdays.json --region us-east-1 --content-encoding gzip --content-type application/json &
fi
wait
echo "aws s3 upload complete"
 
