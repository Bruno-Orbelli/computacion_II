# Makefile template for SQLite ODBC Driver

prefix =	/usr/local
exec_prefix =	${prefix}
libdir =	${exec_prefix}/lib
drvdir =	$(libdir)/odbc
CC =		gcc
INSTALL =	/usr/bin/install -c
LIBTOOL =	./libtool
CFLAGS=		-g -O2 -DDRIVER_VER_INFO=\"0.9998\" 
SQLITE_INC =	UNKNOWN
SQLITE_LIB =	UNKNOWN
SQLITE_FLAGS =	-DHAVE_LIBVERSION=0 \
		-DHAVE_ENCDEC=0 \
		-DHAVE_SQLITEATOF=0 \
		-DHAVE_SQLITEMPRINTF=0 \
		-DHAVE_SQLITETRACE=0
SQLITE3_INC =	/usr/include
SQLITE3_LIB =	-lsqlite3
SQLITE3_FLAGS = -DHAVE_SQLITE3COLUMNTABLENAME=1 \
		-DHAVE_SQLITE3COLUMNDATABASENAME=1 \
		-DHAVE_SQLITE3COLUMNORIGINNAME=1 \
		-DHAVE_SQLITE3LOADEXTENSION=1 \
		-DHAVE_SQLITE3OVERLOADFUNCTION=1 \
		-DHAVE_SQLITE3PREPAREV2=1 \
		-DHAVE_SQLITE3CLEARBINDINGS=1 \
		-DHAVE_SQLITE3CREATEMODULEV2=1 \
		-DHAVE_SQLITE3VFS=1 \
		-DHAVE_SQLITE3PROFILE=1 \
		-DHAVE_SQLITE3STRNICMP=1 \
		-DHAVE_SQLITE3TABLECOLUMNMETADATA=1 \
		-DHAVE_SQLITE3CLOSEV2=1 \
		
SQLITE3_A10N_C =	
SQLITE3_A10N_O =	

SQLITE4_INC =		UNKNOWN
SQLITE4_FLAGS =		 
SQLITE4_A10N_C =	
SQLITE4_A10N_O =	

ODBC_FLAGS =	-DHAVE_UNIXODBC=1 -I/usr/include -DHAVE_ODBCINSTEXT_H=1 -Imissing -DHAVE_SQLLEN=1 -DHAVE_SQLULEN=1 -DHAVE_SQLSETPOSIROW=1 -DCOLATTRIBUTE_LAST_ARG_TYPE=SQLLEN_PTR -DSETSTMTOPTION_LAST_ARG_TYPE=SQLULEN -DHAVE_SQLWCHAR=1 -DHAVE_LONG_LONG=1 -DPTRDIFF_T=ptrdiff_t -DUSE_DLOPEN_FOR_GPPS -DCANT_PASS_VALIST_AS_CHARPTR=1 -DHAVE_USLEEP=1 -DHAVE_NANOSLEEP=1 -DHAVE_LOCALECONV=1 -DHAVE_LOCALTIME_R=1 -DHAVE_GMTIME_R=1 -DWITHOUT_WINTERFACE=1
ODBC_LIB =	-ldl 
VER_INFO =	0.9998

XML2_FLAGS =	
XML2_LIBS =	

all:		 libsqlite3odbc.la libsqlite3_mod_blobtoxy.la libsqlite3_mod_impexp.la libsqlite3_mod_csvtable.la libsqlite3_mod_zipfile.la

libsqliteodbc.la:	sqliteodbc.lo
		$(LIBTOOL) --mode=link $(CC) $(CFLAGS) -o libsqliteodbc.la \
		    sqliteodbc.lo -rpath $(drvdir) $(SQLITE_LIB) \
		    $(ODBC_LIB) -release $(VER_INFO)

libsqlite3odbc.la:	sqlite3odbc.lo $(SQLITE3_A10N_O)
		$(LIBTOOL) --mode=link $(CC) $(CFLAGS) -o libsqlite3odbc.la \
		    sqlite3odbc.lo $(SQLITE3_A10N_O) -rpath $(drvdir) \
		    $(SQLITE3_LIB) $(ODBC_LIB) -release $(VER_INFO) \
		    

libsqlite4odbc.la:	sqlite4odbc.lo $(SQLITE4_A10N_O)
		$(LIBTOOL) --mode=link $(CC) $(CFLAGS) -o libsqlite4odbc.la \
		    sqlite4odbc.lo $(SQLITE4_A10N_O) -rpath $(drvdir) \
		    $(ODBC_LIB) -release $(VER_INFO) 

libsqlite3_mod_blobtoxy.la:	blobtoxy.lo
		$(LIBTOOL) --mode=link $(CC) $(CFLAGS) \
		    -o libsqlite3_mod_blobtoxy.la \
		    blobtoxy.lo -rpath $(drvdir) -release $(VER_INFO)

libsqlite3_mod_impexp.la:	impexp.lo
		$(LIBTOOL) --mode=link $(CC) $(CFLAGS) \
		    -o libsqlite3_mod_impexp.la \
		    impexp.lo -rpath $(drvdir) -release $(VER_INFO)

libsqlite3_mod_csvtable.la:	csvtable.lo
		$(LIBTOOL) --mode=link $(CC) $(CFLAGS) \
		    -o libsqlite3_mod_csvtable.la \
		    csvtable.lo -rpath $(drvdir) -release $(VER_INFO)

libsqlite3_mod_zipfile.la:	zipfile.lo
		$(LIBTOOL) --mode=link $(CC) $(CFLAGS) \
		    -o libsqlite3_mod_zipfile.la \
		    zipfile.lo -rpath $(drvdir) -release $(VER_INFO) -lz

libsqlite3_mod_xpath.la:	xpath.lo
		$(LIBTOOL) --mode=link $(CC) $(CFLAGS) \
		    -o libsqlite3_mod_xpath.la \
		    xpath.lo -rpath $(drvdir) -release $(VER_INFO) $(XML2_LIBS)

sqliteodbc.lo:	sqliteodbc.c sqliteodbc.h
		$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c \
		    -I$(SQLITE_INC) $(ODBC_FLAGS) \
		    $(SQLITE_FLAGS) sqliteodbc.c

sqlite3odbc.lo:	sqlite3odbc.c sqlite3odbc.h
		$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c \
		    -I$(SQLITE3_INC) $(ODBC_FLAGS) \
		    $(SQLITE3_FLAGS) sqlite3odbc.c

sqlite4odbc.lo:	sqlite4odbc.c sqlite4odbc.h
		$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c \
		    -I$(SQLITE4_INC) $(ODBC_FLAGS) \
		    $(SQLITE4_FLAGS) sqlite4odbc.c

$(SQLITE3_A10N_O):	$(SQLITE3_A10N_C)
		$(LIBTOOL) --mode=compile $(CC) -c $(CFLAGS) $(SQLITE3_A10N_C)

$(SQLITE4_A10N_O):	$(SQLITE4_A10N_C)
		$(LIBTOOL) --mode=compile $(CC) -c $(CFLAGS) \
		     $(SQLITE4_A10N_C)

blobtoxy.lo:	blobtoxy.c
		$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c \
		    -I$(SQLITE3_INC) \
		    $(SQLITE3_FLAGS) blobtoxy.c

impexp.lo:	impexp.c
		$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c \
		    -I$(SQLITE3_INC) \
		    $(SQLITE3_FLAGS) impexp.c

csvtable.lo:	csvtable.c
		$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c \
		    -I$(SQLITE3_INC) \
		    $(SQLITE3_FLAGS) csvtable.c

zipfile.lo:	zipfile.c
		$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c \
		    -I$(SQLITE3_INC) \
		    $(SQLITE3_FLAGS) zipfile.c

xpath.lo:	xpath.c
		$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c \
		    -I$(SQLITE3_INC) \
		    $(SQLITE3_FLAGS) $(XML2_FLAGS) xpath.c

install-2:	libsqliteodbc.la
		$(LIBTOOL) --mode=install $(INSTALL) \
		    libsqliteodbc.la $(DESTDIR)$(drvdir)

drvinst-2:	install-2
		sh drvdsninst.sh SQLITE "SQLite Datasource" \
			libsqliteodbc.la $(drvdir)

drvuninst-2:	uninstall-2
		sh drvdsnuninst.sh SQLITE "SQLite Datasource"

uninstall-2:
		$(LIBTOOL) --mode=uninstall \
		    $(RM) $(DESTDIR)$(drvdir)/libsqliteodbc.la

install-3:	libsqlite3odbc.la libsqlite3_mod_blobtoxy.la libsqlite3_mod_impexp.la \
		libsqlite3_mod_csvtable.la libsqlite3_mod_zipfile.la 
		$(LIBTOOL) --mode=install $(INSTALL) \
		    libsqlite3odbc.la $(DESTDIR)$(drvdir)
		test -z "libsqlite3_mod_blobtoxy.la" || \
		    $(LIBTOOL) --mode=install $(INSTALL) \
		    libsqlite3_mod_blobtoxy.la $(DESTDIR)$(drvdir)
		test -z "libsqlite3_mod_impexp.la" || \
		    $(LIBTOOL) --mode=install $(INSTALL) \
		    libsqlite3_mod_impexp.la $(DESTDIR)$(drvdir)
		test -z "libsqlite3_mod_csvtable.la" || \
		    $(LIBTOOL) --mode=install $(INSTALL) \
		    libsqlite3_mod_csvtable.la $(DESTDIR)$(drvdir)
		test -z "libsqlite3_mod_zipfile.la" || \
		    $(LIBTOOL) --mode=install $(INSTALL) \
		    libsqlite3_mod_zipfile.la $(DESTDIR)$(drvdir)
		test -z "" || \
		    $(LIBTOOL) --mode=install $(INSTALL) \
		     $(DESTDIR)$(drvdir)

uninstall-3:
		$(LIBTOOL) --mode=uninstall \
		    $(RM) $(DESTDIR)$(drvdir)/libsqlite3odbc.la
		test -z "libsqlite3_mod_blobtoxy.la" || \
		    $(LIBTOOL) --mode=uninstall \
		        $(RM) $(DESTDIR)$(drvdir)/libsqlite3_mod_blobtoxy.la
		test -z "libsqlite3_mod_impexp.la" || \
		    $(LIBTOOL) --mode=uninstall \
		        $(RM) $(DESTDIR)$(drvdir)/libsqlite3_mod_impexp.la
		test -z "libsqlite3_mod_csvtable.la" || \
		    $(LIBTOOL) --mode=uninstall \
		        $(RM) $(DESTDIR)$(drvdir)/libsqlite3_mod_csvtable.la
		test -z "libsqlite3_mod_zipfile.la" || \
		    $(LIBTOOL) --mode=uninstall \
		        $(RM) $(DESTDIR)$(drvdir)/libsqlite3_mod_zipfile.la
		test -z "" || \
		    $(LIBTOOL) --mode=uninstall \
		        $(RM) $(DESTDIR)$(drvdir)/

drvinst-3:	install-3
		sh drvdsninst.sh SQLITE3 "SQLite3 Datasource" \
			libsqlite3odbc.la $(drvdir)

drvuninst-3:	uninstall-3
		sh drvdsnuninst.sh SQLITE3 "SQLite3 Datasource"


install-4:	libsqlite4odbc.la
		$(LIBTOOL) --mode=install $(INSTALL) \
		    libsqlite4odbc.la $(DESTDIR)$(drvdir)

uninstall-4:
		$(LIBTOOL) --mode=uninstall \
		    $(RM) $(DESTDIR)$(drvdir)/libsqlite4odbc.la

drvinst-4:	install-4
		sh drvdsninst.sh SQLITE4 "SQLite4 Datasource" \
			libsqlite4odbc.la $(drvdir)

drvuninst-4:	uninstall-4
		sh drvdsnuninst.sh SQLITE4 "SQLite4 Datasource"

install:	 install-3

uninstall:	 uninstall-3

drvinst:	 drvinst-3

drvuninst:	 drvuninst-3

doxy:
		doxygen doxygen.conf

deb:
		dpkg-buildpackage $(DPKG_BUILD_OPTS) -tc

clean:
		rm -f *.lo *.la libsqliteodbc.la libsqlite3odbc.la *~ core
		rm -f *.o
		rm -rf .libs .deps

distclean:	clean
		rm -f config.status config.log config.cache Makefile
