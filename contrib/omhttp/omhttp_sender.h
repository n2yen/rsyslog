#ifndef OMHTTP_SENDER_H_INCLUDED
#define OMHTTP_SENDER_H_INCLUDED

#include "rsyslog.h"
#include <errno.h>
#include <sys/queue.h>
#include <curl/curl.h>

typedef struct omhttp_batch_s omhttp_batch_t;

struct omhttp_batch_s {
	uchar **data;		/* array of strings, this will be batched up lazily */
	uchar *restPath;	/* Helper for restpath in batch mode */
	size_t sizeBytes;	/* total length of this batch in bytes */
	size_t nmemb;		/* number of messages in batch (for statistics counting) */
};

// TODO: determine if we should leverage the BEGINinterface macros
/* callback interfaces */
typedef rsRetVal (*curl_complete_cb)(CURL* curl);

typedef struct sender_req_s {
	STAILQ_ENTRY(sender_req_s) link;
	CURL *curl_h; 	/* curl handle should have all appropriate curl options set already */
	const void* private_data;
	const char* buffer;
	size_t bufsize;
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
} sender_q_t;

typedef struct sender_s sender_t;

struct sender_s {
	pthread_t tid;
	CURLM *curlm;
	CURL **curl_handles;
	size_t n_curl_handles;
	sender_q_t sender_q;
	const void *inst_data;
	int runstate;
	curl_complete_cb curl_complete;
};

void init_sender(sender_t *sender, size_t capacity, curl_complete_cb cb);
void start_send_worker(sender_t *sender);
void stop_send_worker(sender_t *sender);
rsRetVal enqueueSendReq(sender_q_t *sender_q, CURL* curl, const void* private_data);



#endif
