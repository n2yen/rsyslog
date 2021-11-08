#ifndef OMHTTP_SENDER_H_INCLUDED
#define OMHTTP_SENDER_H_INCLUDED

#include "rsyslog.h"
#include <errno.h>
#include <sys/queue.h>
#include <zlib.h>
#include <curl/curl.h>
#include <apr_queue.h>

typedef struct omhttpBatch_s omhttpBatch_t;
typedef struct omhttpCompressCtx_s omhttpCompressCtx_t;

struct omhttpBatch_s {
	uchar **data;		/* array of strings, this will be batched up lazily */
	uchar *restPath;	/* Helper for restpath in batch mode */
	size_t sizeBytes;	/* total length of this batch in bytes */
	size_t nmemb;		/* number of messages in batch (for statistics counting) */
};

struct omhttpCompressCtx_s {
		uchar *buf;
		size_t curLen;
		size_t len;
};


typedef struct omhttpRequestData_s {
	CURL *curl;
	struct curl_slist *curlHeader;	/* json POST request info */
	omhttpBatch_t batchData;
	uchar* postData; // we can use this in case we want to manage the memory here. // may not be necessary.
	size_t postLen; // we can use this in case we want to manage the memory here. // may not be necessary.
	/* track the reply */
	int replyLen;
	char *reply;
	long statusCode;
	uchar *restUrl;
	char errbuf[CURL_ERROR_SIZE];
} omhttpRequestData_t;

// TODO: determine if we should leverage the BEGINinterface macros
/* callback interfaces */
typedef rsRetVal (*curlPostSetupCb)(CURL* curl, omhttpRequestData_t *pRequestData, void *privateData);
typedef rsRetVal (*curlPostCompleteCb)(CURL* curl, CURLcode result, void *privateData);
typedef rsRetVal (*curlPostSetOptsCb)(CURL* curl, omhttpRequestData_t *pRequestData, z_stream *zstrm,
		omhttpCompressCtx_t *compressCtx, void *privateData);
// set up post for send
typedef rsRetVal (*compressHttpPayloadCb)(z_stream* zstrm, int compressionLevel,
		omhttpCompressCtx_t *compressCtx, uchar *message, unsigned len);
typedef rsRetVal (*buildCurlHeadersCb)(CURL* curl, z_stream *ztrm, omhttpCompressCtx_t *compressCtx);

typedef struct sender_req_s {
	STAILQ_ENTRY(sender_req_s) link;
	omhttpRequestData_t *pRequestData;
} sender_req_t;

typedef struct sender_q_s {
	STAILQ_HEAD(senderQ_s, sender_req_s) head;
	int capacity;
	int size; //current q size
	pthread_mutex_t mut;
	pthread_cond_t wakeup_worker; // needed to implement forced wake up from another thread
	pthread_cond_t cond_has_space;
	int active_connections;
	// enable later
#if 0
	STATSCOUNTER_DEF(ctrEnq, mutCtrEnq);
	statsobj_t *stats;
#endif
	omhttpCompressCtx_t compressCtx;
} sender_q_t;

typedef struct sender_s sender_t;

struct sender_s {
	pthread_t tid;
	uchar *name;
	CURLM *curlm;
	CURL **curlHandles;
	size_t curlHandlesCount;
	size_t curlHandlesCapacity;
	pthread_mutex_t mut;
	sbool bShutdownWorker;
	apr_queue_t *request_q;
	apr_pool_t *_pool;
	// private data provided by owner of the instance
	// actually not needed, perhaps this is just something
	// that can be guarded on the owner's side. doing that will
	// require that owner is guarding everything with a mutex
	// This would complicate code much more with a bunch of
	// locks just to support this.
	void *privateData;

	// Supported interfaces
	curlPostSetupCb curlPostSetup;
	curlPostCompleteCb curlPostComplete;
	curlPostSetOptsCb curlPostSetOpts;
	// end Interfaces

	/* compression related */
	z_stream zstrm; /* zip stream to use for gzip http compression */
	omhttpCompressCtx_t compressCtx;
};

/* compress context */
void ATTR_NONNULL()
_initCompressCtx(omhttpCompressCtx_t *compressCtx);

void ATTR_NONNULL()
_freeCompressCtx(omhttpCompressCtx_t *compressCtx);
/* end compress context */

rsRetVal
omhttpSenderInit(sender_t *sender, size_t capacity, uchar *name,
		curlPostSetupCb curlPostSetup, curlPostCompleteCb curlPostComplete, curlPostSetOptsCb curlPostSetOpts,
		void *privateData);
void omhttpSenderExit(sender_t *sender);
#if 0
void start_send_worker(sender_t *sender);
void stop_send_worker(sender_t *sender);
#endif
rsRetVal enqueueSendReq(const sender_t *sender, omhttpRequestData_t *pRequestData);

#endif
