#%{!?configure_options: %define configure_options %{nil}}
###################################
%define _prefix /usr
#%define _sysconfdir /usr/etc
#%define _libdir /usr/lib
#%define _includedir /usr/include
###################################
Prefix: %{_prefix}
#Prefix: %{_sysconfdir}
#Prefix: %{_libdir}
#Prefix: %{_includedir}
###################################
Name: hamster		
Version: 0.8	
#Release: 1%{?dist}_ompi1.6
Release: 1_ompi1.6
Summary: Hamster (Hadoop And Mpi on the same cluSTER) 	

Group:	Pivotal	
License: *** License	
URL:    http://		
#Source0: ompi172-plugin-0.8.tar.gz  
#BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root

AutoReq: no
#BuildRequires: libtool	
#BuildRequires: automake	
#BuildRequires: autoconf
#Requires: openmpi >= 1.6.4 

%description
Hamster (Hadoop And Mpi on the same cluSTER) targets the re-use of Hadoop clusters for MPI applications.


%files 
%defattr(-,root,root) 
#%{_bindir} 
#%{_libdir} 
#%{_datadir} 
#%exclude %{_libdir}/debug

#%files
#%defattr(-,root,root,-)
#%doc
#%config(noreplace) /usr/include/openmpi/common
/usr/etc/protos/hamster_protos.pb
/usr/include/openmpi/common/base/hdclient-constants.h
/usr/include/openmpi/common/base/net_utils.h
/usr/include/openmpi/common/base/pbc/pbc.h
/usr/include/openmpi/common/base/pbc/src/alloc.h
/usr/include/openmpi/common/base/pbc/src/array.h
/usr/include/openmpi/common/base/pbc/src/bootstrap.h
/usr/include/openmpi/common/base/pbc/src/context.h
/usr/include/openmpi/common/base/pbc/src/descriptor.pbc.h
/usr/include/openmpi/common/base/pbc/src/map.h
/usr/include/openmpi/common/base/pbc/src/pattern.h
/usr/include/openmpi/common/base/pbc/src/proto.h
/usr/include/openmpi/common/base/pbc/src/stringpool.h
/usr/include/openmpi/common/base/pbc/src/varint.h
/usr/include/openmpi/common/base/str_utils.h
/usr/include/openmpi/common/hdclient.h
/usr/lib/libhamster_common.so
/usr/lib/libhamster_common.so.0
/usr/lib/libhamster_common.so.0.0.0
/usr/lib/openmpi/mca_odls_yarn.so
/usr/lib/openmpi/mca_plm_yarn.so
/usr/lib/openmpi/mca_ras_yarn.so
#----------------------------
/usr/lib64/libhamster_common.so
/usr/lib64/libhamster_common.so.0
/usr/lib64/libhamster_common.so.0.0.0
/usr/lib64/openmpi/mca_odls_yarn.so
/usr/lib64/openmpi/mca_plm_yarn.so
/usr/lib64/openmpi/mca_ras_yarn.so

#######################
/usr/bin/hamster
   /usr/hamster-core-0.8.0/conf/hamster-site.xml
   /usr/hamster-core-0.8.0/conf/log4j.properties
   /usr/hamster-core-0.8.0/lib/hamster/hamster-core-0.8.0-SNAPSHOT.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/activation-1.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/aopalliance-1.0.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/asm-3.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/avro-1.5.3.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-beanutils-1.7.0.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-beanutils-core-1.8.0.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-cli-1.2.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-codec-1.4.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-collections-3.2.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-configuration-1.6.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-daemon-1.0.13.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-digester-1.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-el-1.0.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-httpclient-3.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-io-2.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-lang-2.5.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-logging-1.1.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-math-2.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/commons-net-3.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/gmbal-api-only-3.0.0-b023.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/grizzly-framework-2.1.1-tests.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/grizzly-framework-2.1.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/grizzly-http-2.1.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/grizzly-http-server-2.1.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/grizzly-http-servlet-2.1.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/grizzly-rcm-2.1.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/guava-11.0.2.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/guice-3.0.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/guice-servlet-3.0.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/hadoop-annotations-2.0.5-alpha.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/hadoop-auth-2.0.5-alpha.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/hadoop-common-2.0.5-alpha.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/hadoop-hdfs-2.0.5-alpha.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/hadoop-yarn-api-2.0.5-alpha.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/hadoop-yarn-client-2.0.5-alpha.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/hadoop-yarn-common-2.0.5-alpha.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/hadoop-yarn-server-common-2.0.5-alpha.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jackson-core-asl-1.8.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jackson-jaxrs-1.7.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jackson-mapper-asl-1.8.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jackson-xc-1.7.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jasper-compiler-5.5.23.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jasper-runtime-5.5.23.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/javax.inject-1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/javax.servlet-3.0.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jaxb-api-2.2.2.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jaxb-impl-2.2.3-1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jersey-client-1.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jersey-core-1.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jersey-grizzly2-1.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jersey-guice-1.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jersey-json-1.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jersey-server-1.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jersey-test-framework-core-1.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jersey-test-framework-grizzly2-1.8.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jets3t-0.6.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jettison-1.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jetty-6.1.26.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jetty-util-6.1.26.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jline-0.9.94.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jsch-0.1.42.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jsp-api-2.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/jsr305-1.3.9.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/junit-4.8.2.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/kfs-0.3.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/log4j-1.2.17.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/management-api-3.0.0-b012.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/netty-3.2.2.Final.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/netty-3.5.11.Final.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/paranamer-2.3.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/protobuf-java-2.4.0a.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/servlet-api-2.5.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/slf4j-api-1.6.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/slf4j-log4j12-1.6.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/snappy-java-1.0.3.2.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/stax-api-1.0.1.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/xmlenc-0.52.jar
   /usr/hamster-core-0.8.0/lib/hamster/lib/zookeeper-3.4.2.jar

#%exclude    /usr/lib/libhamster_common.so
#%exclude    /usr/lib/libhamster_common.so.0
#%exclude    /usr/lib/libhamster_common.so.0.0.0
#%exclude    /usr/lib/openmpi/mca_odls_yarn.so
#%exclude    /usr/lib/openmpi/mca_plm_yarn.so
#%exclude    /usr/lib/openmpi/mca_ras_yarn.so
#%exclude    /usr/lib/openmpi/mca_state_yarn.so
#%post
#cp /usr/lib/libhamster_common.so  %{_prefix}/lib/
#cp /usr/lib/libhamster_common.so.0 %{_prefix}/lib/
#cp /usr/lib/libhamster_common.so.0.0.0 %{_prefix}/lib/
#cp /usr/lib/openmpi/mca_odls_yarn.so %{_prefix}/lib/openmpi/
#cp /usr/lib/openmpi/mca_plm_yarn.so %{_prefix}/lib/openmpi/
#cp /usr/lib/openmpi/mca_ras_yarn.so %{_prefix}/lib/openmpi/
#cp /usr/lib/openmpi/mca_state_yarn.so %{_prefix}/lib/openmpi/

#%post
#echo $RPM_BUILD_ROOT
#cp $RPM_BUILD_ROOT/usr/lib/libhamster_common.so  %{_prefix}/lib/
#cp $RPM_BUILD_ROOT/hamster-0.8-1.el6.x86_64/usr/lib/libhamster_common.so.0 %{_prefix}/lib/
#cp $RPM_BUILD_ROOT/hamster-0.8-1.el6.x86_64/usr/lib/libhamster_common.so.0.0.0 %{_prefix}/lib/
#cp $RPM_BUILD_ROOT/hamster-0.8-1.el6.x86_64/usr/lib/openmpi/mca_odls_yarn.so %{_prefix}/lib/openmpi/
#cp $RPM_BUILD_ROOT/hamster-0.8-1.el6.x86_64/usr/lib/openmpi/mca_plm_yarn.so %{_prefix}/lib/openmpi/
#cp $RPM_BUILD_ROOT/hamster-0.8-1.el6.x86_64/usr/lib/openmpi/mca_ras_yarn.so %{_prefix}/lib/openmpi/
#cp $RPM_BUILD_ROOT/hamster-0.8-1.el6.x86_64/usr/lib/openmpi/mca_state_yarn.so %{_prefix}/lib/openmpi/

%changelog
* Wed Aug 21 2013  Jimmy Haijun Cao <jcao@gopivotal.com>
- Specfile created
