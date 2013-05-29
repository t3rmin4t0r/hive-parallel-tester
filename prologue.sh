set -e;
exec &> stdout;
prologue() {
export ANT_OPTS='-Xms768m -Xmx2048m -XX:PermSize=128m -XX:MaxPermSize=768m';
export TESTARGS='-Dtest.junit.output.format=xml -Dtest.output=yes -Dtest.junit.output.usefile=true -Djavadoc.executable=/bin/true';
test -d ./ant || (mv ant ant.zip && mkdir ant && cd ant && $JAVA_HOME/bin/jar xf ../ant.zip);
export PATH=$PATH:$PWD/ant/*/bin;
umask 022;
# Redirect stdout ( > ) into a named pipe ( >() ) running "tee"
mkdir hive; 
cd hive; 
tar xf ../hive-build.tar; 
find -name TEST*.xml -delete;
# hcatalog runs checkstyle during test
sed -i~ "s@<antcall.*checkstyle/>@@" hcatalog/build.xml;
}
