pkglib_LTLIBRARIES = omhttp.la

omhttp_la_SOURCES = omhttp.c omhttp_sender.c
omhttp_la_CPPFLAGS = $(RSRT_CFLAGS) $(PTHREADS_CFLAGS) $(CURL_CFLAGS) $(APU_CFLAGS)
omhttp_la_LDFLAGS = -module -avoid-version
omhttp_la_LIBADD =  $(CURL_LIBS) $(LIBM) $(APU_LIBS)

EXTRA_DIST = 
