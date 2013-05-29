epilogue() {
cd ../;
mkdir reports/; find hive -name TEST*.xml -exec mv -t reports/ {} \; ;
test -d hive/ && rm -rf hive/; 
}
