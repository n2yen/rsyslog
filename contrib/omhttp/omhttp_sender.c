#include "rsyslog.h"
#include "errmsg.h"
#include <errno.h>
#include <sys/queue.h>
#include <curl/curl.h>
#include "omhttp_sender.h"
#include "module-template.h"
#include <apr_pools.h>

/* forward declarations */
static sbool shutdownWorker(sender_t *sender);

rsRetVal
enqueueSendReq(const sender_t *sender, omhttpRequestData_t *pRequestData)
{
	DEFiRet;
	apr_status_t apr_rv = APR_SUCCESS;
	int i = 0,
		max_tries = 1;

	while (i++ < max_tries) {
		apr_rv = apr_queue_push(sender->request_q, (void*)pRequestData);
		if (apr_rv == APR_SUCCESS) {
			break;
		} else if (apr_rv == APR_EINTR) {
			// retry
			continue;
		} else if (apr_rv == APR_EOF) {
			fprintf(stderr, "omhttp_sender: enqueueSendReq - queue was terminated: %d\n", apr_rv);
			ABORT_FINALIZE(RS_RET_SUSPENDED);
			break;
		} else {
			ABORT_FINALIZE(RS_RET_SUSPENDED);
		}
	}
finalize_it:
	if (apr_rv != APR_SUCCESS) {
		char buf[256];
		fprintf(stderr, "omhttp_sender: enqueue failed - error: %d, %s\n",
			apr_rv, apr_strerror(apr_rv, buf, sizeof(buf)));
		iRet = RS_RET_OUT_OF_MEMORY;
		assert(0);
	}
	RETiRet;
}

static rsRetVal dequeueSendReq(sender_t *sender, omhttpRequestData_t **pRequestDataOut, int tryonly)
{
	apr_status_t apr_rv;
	omhttpRequestData_t *pdata;
	DEFiRet;

	apr_queue_t *queue = sender->request_q;
	int i = 0,
		max_tries = 1;

	while (i++ < max_tries) {
		if (tryonly) {
			apr_rv = apr_queue_trypop(queue, (void*)&pdata);
		} else {
			apr_rv = apr_queue_pop(queue, (void*)&pdata);
		}
		if (apr_rv == APR_SUCCESS) {
			*pRequestDataOut = pdata;
			break;
		} else if (apr_rv == APR_EINTR) {
			// retry
			continue;
		} else if (apr_rv == APR_EOF) {
			ABORT_FINALIZE(RS_RET_SENDER_GONE_AWAY);
			break;
		} else {
			// unexpected
			ABORT_FINALIZE(RS_RET_ERR);
		}
	}
finalize_it:
	RETiRet;
}

#define WAITMS(x) \
    struct timeval wait = { 0, (x)*1000 }; \
    (void)select(0, NULL, NULL, NULL, &wait);

static __attribute__((noreturn)) void *senderTask(void *data)
{
	sender_t* me = (sender_t*) data;
	int numfds = 0;
	int still_running = 0;

	fprintf(stderr, "omhttp_sender: thread (%p) started.\n", (const void*)me->tid);
	assert(me->tid == pthread_self());
	int repeats = 0;
	size_t num_easy = 0;
	CURLMcode mcode;
	while (1)
	{
		while (apr_queue_size(me->request_q) && num_easy < me->curlHandlesCapacity) {
			omhttpRequestData_t *pRequestData = NULL;

			dequeueSendReq(me, &pRequestData, 0);
			if (pRequestData) {
				CURL *curl = me->curlHandles[num_easy];
				curl_easy_reset(curl);
				assert(curl != NULL);
				if (me->curlPostSetup) {
					me->curlPostSetup(curl, pRequestData, me->privateData);
				}
				if (me->curlPostSetOpts) {
					me->curlPostSetOpts(curl, pRequestData, &me->zstrm, &me->compressCtx, me->privateData);
				}
				mcode = curl_multi_add_handle(me->curlm, curl);
				if (mcode == CURLM_OK) {
					num_easy++;
				} else {
					LogError(0, RS_RET_ERR,
									 "omhttp_sender: error curl_multi_add_handle ret- %d:%s\n",
									 mcode, curl_multi_strerror(mcode));
					assert(0);
				}
			} else {
				break;
			}
		}

#if 0
			mcode = curl_multi_perform(me->curlm, &still_running);

			if (still_running != prev_still_running) {
				prev_still_running = still_running;
			}
#endif
		repeats = 0;
		do {
			numfds = 0;
			mcode = curl_multi_perform(me->curlm, &still_running);
			if (mcode != CURLM_OK) {
				fprintf(stderr, "omhttp_sender: curl_multi failed, code %d\n", mcode);
				break;
			}

			mcode = curl_multi_wait(me->curlm, NULL, 0, 500, &numfds);
			if (mcode != CURLM_OK) {
				fprintf(stderr, "omhttp_sender: curl_multi_wait error: %d\n", mcode);
				break;
			}

			if (!numfds) {
				repeats++;
				if (repeats > 1) {
					WAITMS(500);
				}
			} else {
				repeats = 0;
			}
		} while (still_running);

		int msgs_left = 0;
		CURLMsg *msg = NULL;
		CURL *curl = NULL;
		while ((msg = curl_multi_info_read(me->curlm, &msgs_left))) {
			CURLcode curlResult = 0;
			if (msg->msg == CURLMSG_DONE) {
				curl = msg->easy_handle;
				curlResult = msg->data.result;
				if (curlResult != CURLE_OK) {
					LogError(0, RS_RET_ERR,
							"omhttp_sender: %s() - curl handle: %p, error code: %d:%s\n",
							__FUNCTION__, (void *)curl, curlResult,
							curl_multi_strerror(curlResult));
				}
				me->curlPostComplete(curl, curlResult, me->privateData);
				mcode = curl_multi_remove_handle(me->curlm, curl);
				if (mcode == CURLM_OK) {
					num_easy--;
					me->processedRequests++;
					curl_easy_reset(curl);
				} else {
					LogError(0, RS_RET_ERR,
									 "omhttp_sender: error curl_multi_remove_handle ret- %d:%s\n",
									 mcode, curl_multi_strerror(mcode));
					assert(0);
				}
			}
		}

		if (shutdownWorker(me)) {
			break;
		}
	}
	fprintf(stderr, "omhttp_sender: total successfully processed requests: %ld\n", me->processedRequests);
	fprintf(stderr, "omhttp_sender: exiting thread.\n");
	pthread_exit(NULL);
}

static void omhttpAprInit(sender_t *sender)
{
	if (apr_initialize() != APR_SUCCESS) {
		abort();
	}

	apr_pool_create(&sender->_pool, NULL);
	apr_pool_tag(sender->_pool, "apr-util omhttp pool");
}

static void omhttpAprExit(sender_t *sender)
{
	apr_pool_destroy(sender->_pool);
	apr_terminate();
}

static void startWorker(sender_t *sender)
{
	fprintf(stderr, "omhttp_sender: starting worker thread: '%s'\n", sender->name);
	pthread_create(&sender->tid, NULL, senderTask, sender);
}

static sbool shutdownWorker(sender_t *sender)
{
	sbool bShutdown = 0;
	pthread_mutex_lock(&sender->mut);
	bShutdown = sender->bShutdownWorker;
	pthread_mutex_unlock(&sender->mut);
	return bShutdown;
}

static sbool setShutdownState(sender_t *sender, sbool state)
{
	sbool bShutdown = 0;
	pthread_mutex_lock(&sender->mut);
	sender->bShutdownWorker = state;
	pthread_mutex_unlock(&sender->mut);
	return bShutdown;
}

static void stopWorker(sender_t *sender)
{
	fprintf(stderr, "omhttp_sender: stopping worker thread: %s !!!!\n", sender->name);
	setShutdownState(sender, 1);
	fprintf(stderr, "omhttp_sender: apr queue size: %d\n", apr_queue_size(sender->request_q));
	apr_queue_interrupt_all(sender->request_q);
	fprintf(stderr, "omhttp_sender: apr_queue_interrupt_all done.\n");
	apr_queue_term(sender->request_q);
	fprintf(stderr, "omhttp_sender: joining thread!!!!\n");
	pthread_join(sender->tid, NULL);
	fprintf(stderr, "omhttp_sender: worker stopped.\n");
}

rsRetVal
omhttpSenderInit(sender_t *sender, size_t capacity, uchar *name,
		curlPostSetupCb curlPostSetup, curlPostCompleteCb curlPostComplete, curlPostSetOptsCb curlPostSetOpts,
		void *privateData)
{
	sender->name = (uchar*)strdup(name ? (char*)name : (char*)"");
	sender->curlm = curl_multi_init();
	sender->curlHandlesCount = 0;
	sender->curlHandles = calloc(capacity, sizeof(CURL*));
	sender->curlHandlesCapacity = capacity;
	sender->processedRequests = 0;
	sender->bShutdownWorker = 0;
	sender->curlPostSetup = curlPostSetup;
	sender->curlPostComplete = curlPostComplete;
	sender->curlPostSetOpts = curlPostSetOpts;
	sender->privateData = privateData;
	pthread_mutex_init(&sender->mut, NULL);

	_initCompressCtx(&sender->compressCtx);

	omhttpAprInit(sender);
	apr_status_t apr_rv = apr_queue_create(&sender->request_q, capacity, sender->_pool);
	assert(apr_rv == APR_SUCCESS);

	for (size_t i = 0; i < sender->curlHandlesCapacity; ++i) {
		sender->curlHandles[i] = curl_easy_init();
	}

	startWorker(sender);

	return RS_RET_OK;
}

void omhttpSenderExit(sender_t *sender)
{
	stopWorker(sender);
	for (size_t i = 0; i < sender->curlHandlesCapacity; ++i) {
		curl_easy_cleanup(sender->curlHandles[i]);
	}
	_freeCompressCtx(&sender->compressCtx);
	free(sender->name);
	free(sender->curlHandles);
	curl_multi_cleanup(sender->curlm);
	omhttpAprExit(sender);
}

// End new multi-threaded sender interface
