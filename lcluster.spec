Name:           lcluster
Version:        2.1.0
Release:        051901
Group:          Applications/Internet
Summary:        LevelDB cluster
License:        MIT License
URL:            https://www.livejournal.com
Packager:        Mikhail Kirillov <m.kirillov@rambler-co.ru>
Source0:        lcluster.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-root-%(%{__id_u} -n)
Requires:       daemonize
Requires(pre):  shadow-utils

# pull in golang libraries by explicit import path, inside the meta golang()
# [...]

%description
# include your full description of the application here.

%prep
%setup -q -n %{name}

# many golang binaries are "vendoring" (bundling) sources, so remove them. Those dependencies need to be packaged independently.
rm -rf vendor

%build
cd lnode
go build -v -a -ldflags "-B 0x$(head -c20 /dev/urandom|od -An -tx1|tr -d ' \n')" -tags 'netgo'
cd ../lproxy
go build -v -a -ldflags "-B 0x$(head -c20 /dev/urandom|od -An -tx1|tr -d ' \n')" -tags 'netgo'
cd ../lsize
go build -v -a -ldflags "-B 0x$(head -c20 /dev/urandom|od -An -tx1|tr -d ' \n')" -tags 'netgo'
cd ..

%install
rm -rf %{buildroot}
install -d %{buildroot}
install -d %{buildroot}%{_bindir}
install -d %{buildroot}%{_sysconfdir}
install -d  %{buildroot}%{_sysconfdir}/lcluster
install -d  %{buildroot}%{_sysconfdir}/init.d
install -d  %{buildroot}/var/log/lcluster

install -p -m 0755 ./lnode/lnode %{buildroot}%{_bindir}/lnode
install -p -m 0755 ./lproxy/lproxy %{buildroot}%{_bindir}/lproxy
install -p -m 0755 ./lsize/lsize %{buildroot}%{_bindir}/lsize

install -p -m 0755 ./lnode/config.json.example %{buildroot}%{_sysconfdir}/lcluster/node.json.example
install -p -m 0755 ./lproxy/config.json.example %{buildroot}%{_sysconfdir}/lcluster/proxy.json.example

install -p -m 0755 ./lnode/init.d %{buildroot}%{_sysconfdir}/init.d/lnode.example
install -p -m 0755 ./lnode/init.d %{buildroot}%{_sysconfdir}/init.d/lproxy.example

%files
%defattr(-,root,root,-)
%attr(0755,root,root) %{_bindir}/lnode
%attr(0755,root,root) %{_bindir}/lproxy
%attr(0755,root,root) %{_bindir}/lsize
%attr(0755,root,root) %{_sysconfdir}/lcluster/node.json.example
%attr(0755,root,root) %{_sysconfdir}/lcluster/proxy.json.example
%attr(0755,root,root) %{_sysconfdir}/init.d/lnode.example
%attr(0755,root,root) %{_sysconfdir}/init.d/lproxy.example

%pre

%changelog

* Tue Mar 26 2019 Mikhail Kirillov <mikkirillov@yandex.ru> - 1.1.9
 - final version
