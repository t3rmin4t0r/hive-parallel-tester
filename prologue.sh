set -e;
prologue() {
export ANT_OPTS='-Xms768m -Xmx1024m -XX:PermSize=128m -XX:MaxPermSize=128m';
export TESTARGS='-Dtest.junit.output.format=xml -Dtest.output=yes -Dtest.junit.output.usefile=true -Dcheckstyle.failOnViolation=false';
export PATH=$PATH:$PWD/ant/bin;
umask 022
exec &> stdout;
mkdir hive; 
cd hive; 
tar xf ../hive-build.tar; 
find -name TEST*.xml -delete;
# hcatalog runs checkstyle during test
sed -i~ "s@<antcall.*checkstyle/>@@" hcatalog/build.xml;
}
