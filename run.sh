for file in "./database/schema"/*
do
  psql -U "phuongdo" -d bdjuno -f $file
done