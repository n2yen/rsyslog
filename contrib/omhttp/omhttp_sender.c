#include "rsyslog.h"
#include "errmsg.h"
#include <errno.h>
#include <sys/queue.h>
#include <curl/curl.h>
#include "omhttp_sender.h"
#include "module-template.h"
#include "glbl.h"
#include <apr_pools.h>

DEFobjCurrIf(glbl)

/* forward declarations */
static sbool getShutdownState(sender_t *sender);

#if 0
static rsRetVal
initIoQ(sender_q_t *sender_q, size_t capacity)
{
	DEFiRet;
	CHKiConcCtrl(pthread_mutex_init(&sender_q->mut, NULL));
	CHKiConcCtrl(pthread_cond_init(&sender_q->wakeup_worker, NULL));
	CHKiConcCtrl(pthread_cond_init(&sender_q->cond_has_space, NULL));
	STAILQ_INIT(&sender_q->head);
	sender_q->size = 0;
	sender_q->capacity = capacity; /* TODO: discuss this and fix potential concurrent read/write issues */
finalize_it:
	RETiRet;
}

static void destroyIoQ(sender_q_t *sender_q)
{
	sender_req_t *req;
#if 0
	if (io_q.stats != NULL) {
		statsobj.Destruct(&io_q.stats);
	}
#endif
	pthread_mutex_lock(&sender_q->mut);
	while (!STAILQ_EMPTY(&sender_q->head)) {
		req = STAILQ_FIRST(&sender_q->head);
		STAILQ_REMOVE_HEAD(&sender_q->head, link);
#if 0
		LogError(0, RS_RET_INTERNAL_ERROR, "imptcp: discarded enqueued io-work to allow shutdown "
								"- ignored");
#else
		printf("omhttp-sender: discarded enqueued io-work to allow shutdown "
				"- ignored\n");

#endif
		free(req);
	}
	sender_q->size = 0;
	pthread_mutex_unlock(&sender_q->mut);
	pthread_cond_destroy(&sender_q->wakeup_worker);
	pthread_cond_destroy(&sender_q->cond_has_space);
	pthread_mutex_destroy(&sender_q->mut);
}
#endif

rsRetVal
enqueueSendReq(const sender_t *sender, omhttpRequestData_t *pRequestData)
{
	DEFiRet;
	apr_status_t apr_rv = APR_SUCCESS;
	int i = 0,
		max_tries = 10;

	while (i++ < max_tries) {
		apr_rv = apr_queue_push(sender->request_q, (void*)pRequestData);
		if (apr_rv == APR_SUCCESS) {
			//printf("pushed into queue postdata: %s\n", pRequestData->postData);
			break;
		} else if (apr_rv == APR_EINTR) {
			// retry
			//printf("enqueue EINTR: %d\n", apr_rv);
			continue;
		} else if (apr_rv == APR_EOF) {
			// queue is terminated we should be shutting down.
			//printf("enqueue EOF: %d\n", apr_rv);
			ABORT_FINALIZE(RS_RET_SUSPENDED);
			break;
		} else {
			ABORT_FINALIZE(RS_RET_SUSPENDED);
		}
	}
	assert(i < max_tries);

finalize_it:
	if (apr_rv != APR_SUCCESS) {
		char buf[256];
		//printf("omhttp-sender: enqueue failed - iterations: %d, error: %d, %s\n",
		//		i, apr_rv, apr_strerror(apr_rv, buf, sizeof(buf)));
		iRet = RS_RET_OUT_OF_MEMORY;
		assert(0);
	}
	RETiRet;
}

static rsRetVal dequeueSendReq(sender_t *sender, omhttpRequestData_t **pRequestDataOut, int tryonly)
{
	DEFiRet;
	apr_status_t apr_rv;
	int i = 0,
		max_tries = 1;
	apr_queue_t *queue = sender->request_q;
	omhttpRequestData_t *pdata;

	while (i++ < max_tries) {
#if 0
		apr_rv = apr_queue_trypop(queue, (void*)&pdata);
#else
		if (tryonly) {
			apr_rv = apr_queue_trypop(queue, (void*)&pdata);
		} else {
			//printf("doing a normal apr_queue_pop - queue size: %d\n.", apr_queue_size(queue));
			apr_rv = apr_queue_pop(queue, (void*)&pdata);
		}
#endif
		if (apr_rv == APR_SUCCESS) {
			// done
			*pRequestDataOut = pdata;
			//printf("dequeued postdata: %s\n", (*pRequestDataOut)->postData);
			break;
		} else if (apr_rv == APR_EINTR) {
			// retry
			//printf("apr_queue_pop was interrupted.\n");
			continue;
		} else if (apr_rv == APR_EOF) {
			// queue shutdown
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

#if 0
static consumeRemainingRequests()
{
		if (me->runstate == 1 )
		{
			pthread_mutex_lock(&me->sender_q.mut);
			size_t cur_size = me->sender_q.size;
			pthread_mutex_unlock(&me->sender_q.mut);
			printf(">>>> queue is shutting down!!! - items left: %d\n", cur_size);
			if (cur_size > 0) {
				// consume the rest
				for (int i = 0; i < cur_size; ++i) {
					req = dequeueSendReq(&me->sender_q);
					if (req) {
						printf("request taken: requestData: %p\n", (void*)req->pRequestData);
					}
				}
			}
		}
}
#endif

#define WAITMS(x) \
    struct timeval wait = { 0, (x)*1000 }; \
    (void)select(0, NULL, NULL, NULL, &wait);

static __attribute__((noreturn)) void *sender_task(void *data)
{
	sender_t* me = (sender_t*) data;
	int i = 0;
	int numfds = 0;
	int still_running = 0;
	int prev_still_running = 0;

	printf("thread: (%p) started.\n", (const void*)me->tid);
	assert(me->tid == pthread_self());
	int count= 0;
	int repeats = 0;
	size_t num_easy = 0;
	CURLMcode mcode;
	while (1)
	{
		if (getShutdownState(me)) {
			break;
		}
		//fprintf(stderr, "current number of easy handles: %ld\n", num_easy);
		while (apr_queue_size(me->request_q) && num_easy < me->curlHandlesCapacity) {
			omhttpRequestData_t *pRequestData = NULL;

			//fprintf(stderr, "About to dequeue, num_easy connections: %d.\n", num_easy);
			dequeueSendReq(me, &pRequestData, num_easy != 0);
			if (pRequestData) {
#define USE_CURL_RESET
#ifndef USE_CURL_RESET
				CURL *curl = curl_easy_init();
#else
				CURL *curl = me->curlHandles[num_easy];
				curl_easy_reset(curl);
#endif
				assert(curl != NULL);
				/* TODO: this needs to be moved to connection initialization */
				if (me->curlPostSetup) {
					me->curlPostSetup(curl, pRequestData, me->privateData);
				}
				if (me->curlPostSetOpts) {
					me->curlPostSetOpts(curl, pRequestData, &me->zstrm, &me->compressCtx, me->privateData);
				}
				//printf("request taken: requestData: %p\n", (void *)pRequestData);
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
				//printf("empty request.\n");
				break;
			}
		}

		{
			mcode = curl_multi_perform(me->curlm, &still_running);

			if (still_running != prev_still_running) {
				//printf("still_running = %d\n", still_running);
				prev_still_running = still_running;
			}

			repeats = 0;
			//do {
			do {
			//printf("calling curl_multi_wait()...\n");
			numfds = 0;
			mcode = curl_multi_wait(me->curlm, NULL, 0, 500, &numfds);
			//printf("woke up.\n");
			if (mcode != CURLM_OK) {
				fprintf(stderr, "error: curl_multi_wait() returned %d\n", mcode);
				break;
			}
			mcode = curl_multi_perform(me->curlm, &still_running);
			if (mcode != CURLM_OK) {
				fprintf(stderr, "curl_multi failed, code %d\n", mcode);
				break;
			}
			} while (still_running);
			//} while (still_running);

			int msgs_left = 0;
			CURLMsg *msg = NULL;
			CURL *pCurl = NULL;
			int rc = 0;

			while ((msg = curl_multi_info_read(me->curlm, &msgs_left))) {
				if (msg->msg == CURLMSG_DONE) {
					pCurl = msg->easy_handle;
					rc = msg->data.result;
					// TODO: should we send the result as well?
					me->curlPostComplete(pCurl, rc, me->privateData);
					// TODO: verify this can be removed here.
#if 0
					if (rc != CURLE_OK) {
						LogError(0, RS_RET_ERR,
								"omhttp: %s() - curl handle: %p, error code: %d:%s\n",
								__FUNCTION__, (void *)pCurl, rc,
								curl_multi_strerror(rc));
						// assert(0);
						continue;
					}
#endif

					mcode = curl_multi_remove_handle(me->curlm, pCurl);
					if (mcode == CURLM_OK) {
						num_easy--;
						// TODO: remove
						count++;
#ifndef USE_CURL_RESET
						curl_easy_cleanup(pCurl);
#else
						curl_easy_reset(pCurl);
#endif
					} else {
						LogError(0, RS_RET_ERR,
								"omhttp_sender: error curl_multi_remove_handle ret- %d:%s\n",
								mcode, curl_multi_strerror(mcode));
						assert(0);
					}
				}
			}

			if (!numfds) {
				repeats++;
				if (repeats > 1) {
					WAITMS(500);
				}
			} else {
				repeats = 0;
			}
		}

		//printf("iteration %d, 2xx responses: %d\n", ++i, count);
	}
	fprintf(stderr, "total successful 200s responses: %d\n", count);
	fprintf(stderr, "exiting thread.\n");

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

static void start_send_worker(sender_t *sender)
{
	// TODO: sender->tid needs to be needs to be locked
	fprintf(stderr, "!!!! starting worker thread: %s...\n", sender->name);
	pthread_create(&sender->tid, NULL, sender_task, sender);
}

static sbool getShutdownState(sender_t *sender)
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

static void stop_send_worker(sender_t *sender)
{
	fprintf(stderr, "!!!! stopping worker thread: %s !!!!\n", sender->name);
	setShutdownState(sender, 1);
	fprintf(stderr, "apr_queue_interrupt_all called...\n");
	apr_queue_interrupt_all(sender->request_q);
	fprintf(stderr, "apr_queue_interrupt_all done.\n");
	apr_queue_term(sender->request_q);
#if 1
	void *res;
	fprintf(stderr, "apr queue size: %d\n", apr_queue_size(sender->request_q));
	fprintf(stderr, "joining thread!!!!\n");
	int s = pthread_join(sender->tid, &res);
	fprintf(stderr, "thread joined returned: %d\n", s);
	//if (s != 0)
	//	handle_error_en(s, "pthread_join");
#if 0
	fprintf(stderr, "Joined with thread %d; returned value was %s\n",
			sender->tid, (char *) res);
#endif
	free(res);      /* Free memory allocated by thread */
#endif
}

rsRetVal
omhttpSenderInit(sender_t *sender, size_t capacity, uchar *name,
		curlPostSetupCb curlPostSetup, curlPostCompleteCb curlPostComplete, curlPostSetOptsCb curlPostSetOpts,
		void *privateData)
{
	sender->tid = -1;
	sender->name = (uchar*)strdup(name ? (char*)name : (char*)"");
	sender->curlm = curl_multi_init();
	sender->curlHandlesCount = 0;
	sender->curlHandles = calloc(capacity, sizeof(CURL*));
	sender->curlHandlesCapacity = capacity;

	sender->bShutdownWorker = 0;
	sender->curlPostSetup = curlPostSetup;
	sender->curlPostComplete = curlPostComplete;
	sender->curlPostSetOpts = curlPostSetOpts;
	sender->privateData = privateData;
	pthread_mutex_init(&sender->mut, NULL);

	_initCompressCtx(&sender->compressCtx);

	/* apr related stuff */
	omhttpAprInit(sender);
	apr_status_t apr_rv = apr_queue_create(&sender->request_q, capacity, sender->_pool);
	assert(apr_rv == APR_SUCCESS);

	for (size_t i = 0; i < sender->curlHandlesCapacity; ++i) {
		sender->curlHandles[i] = curl_easy_init();
	}

		// start worker
	start_send_worker(sender);

finalize_it:
	return RS_RET_OK;
}

void omhttpSenderExit(sender_t *sender)
{
	stop_send_worker(sender);
	_freeCompressCtx(&sender->compressCtx);

	for (size_t i = 0; i < sender->curlHandlesCapacity; ++i) {
		curl_easy_cleanup(sender->curlHandles[i]);
	}
	free(sender->name);
	free(sender->curlHandles);
	curl_multi_cleanup(sender->curlm);
	omhttpAprExit(sender);
}

// End new multi-threaded sender interface
