set -e -x ;
exec &> stdout;
prologue() {

export HOME=$PWD;
export TESTARGS="-Dtest.junit.output.format=xml -Dtest.output=yes -Dtest.junit.output.usefile=true -Djavadoc.executable=/bin/true -Duser.home=$PWD -Divy.default.ivy.user.dir=$PWD/.ivy/ -DDmaven.local.repo=$PWD/.m2";
export PATH=$PATH:$(ls -d $PWD/ant/*/bin);
umask 022;
mkdir hive; 
cd hive; 
tar xf ../hive-build.tar; 
find -name TEST*.xml -delete;
# hcatalog runs checkstyle during test
sed -i~ "s@<antcall.*checkstyle/>@@" hcatalog/build.xml;
ant package $TESTARGS;
}
