set -e ;
exec &> stdout;
prologue() {
export ANT_OPTS='-Xms768m -Xmx2048m -XX:PermSize=128m -XX:MaxPermSize=768m';
export HOME=$PWD;
export TESTARGS="-Dtest.junit.output.format=xml -Dtest.output=yes -Dtest.junit.output.usefile=true -Djavadoc.executable=/bin/true -Duser.home=$PWD -Divy.default.ivy.user.dir=$PWD/.ivy2/ -DDmaven.local.repo=$PWD/.m2";
export PATH=$PATH:$(ls -d $PWD/ant/*/bin);
umask 022;
test -d .m2 && chmod 0700 -R .m2;
test -d .ivy2 && chmod 0700 -R .ivy2;
mkdir hive; 
cd hive; 
tar xf ../hive-build.tar; 
find -name TEST*.xml -delete;
# hcatalog runs checkstyle during test
sed -i~ "s@<antcall.*checkstyle/>@@" hcatalog/build.xml;
# if you ship your maven dirs, this is not required
test -e ../.m2 || ant package $TESTARGS;
}
