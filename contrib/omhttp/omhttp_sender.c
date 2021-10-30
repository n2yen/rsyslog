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
			printf("pushed into queue postdata: %s\n", pRequestData->postData);
			break;
		} else if (apr_rv == APR_EINTR) {
			// retry
			printf("enqueue EINTR: %d\n", apr_rv);
			continue;
		} else if (apr_rv == APR_EOF) {
			// queue is terminated we should be shutting down.
			printf("enqueue EOF: %d\n", apr_rv);
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
		printf("omhttp-sender: enqueue failed - iterations: %d, error: %d, %s\n",
				i, apr_rv, apr_strerror(apr_rv, buf, sizeof(buf)));
		iRet = RS_RET_OUT_OF_MEMORY;
		//assert(0);
	}
	RETiRet;
}

static rsRetVal dequeueSendReq(sender_t *sender, omhttpRequestData_t **pRequestDataOut)
{
	DEFiRet;
	apr_status_t apr_rv;
	int i = 0,
		max_tries = 3;
	apr_queue_t *queue = sender->request_q;
	omhttpRequestData_t *pdata;

	while (i++ < max_tries) {
		apr_rv = apr_queue_trypop(queue, (void*)&pdata);
		if (apr_rv == APR_SUCCESS) {
			// done
			*pRequestDataOut = pdata;
			printf("dequeued postdata: %s\n", (*pRequestDataOut)->postData);
			break;
		} else if (apr_rv == APR_EINTR) {
			// retry
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
	while (me->runstate != 1)
	{
		printf("current number of easy handles: %ld\n", num_easy);
		while (num_easy < me->n_curl_handles) {
			omhttpRequestData_t *pRequestData = NULL;

			dequeueSendReq(me, &pRequestData);
			if (pRequestData) {
#if 1
				CURL *curl = curl_easy_init();
#else
				CURL *curl = me->curl_handles[i];
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
				printf("empty request.\n");
				break;
			}
		}

		{
			mcode = curl_multi_perform(me->curlm, &still_running);

			if (still_running != prev_still_running) {
				printf("still_running = %d\n", still_running);
				prev_still_running = still_running;
			}

			repeats = 0;
			//do {
			printf("calling curl_multi_wait()...\n");
			mcode = curl_multi_wait(me->curlm, NULL, 0, 500, &numfds);
			printf("woke up.\n");
			if (mcode != CURLM_OK) {
				fprintf(stderr, "error: curl_multi_wait() returned %d\n", mcode);
				break;
			}
			mcode = curl_multi_perform(me->curlm, &still_running);
			if (mcode != CURLM_OK) {
				fprintf(stderr, "curl_multi failed, code %d\n", mcode);
				break;
			}
			//} while (still_running);

				int msgs_left = 0;
				CURLMsg *msg = NULL;
				CURL *pCurl = NULL;
				int rc = 0;

				while ((msg = curl_multi_info_read(me->curlm, &msgs_left))) {
					if (msg->msg == CURLMSG_DONE) {
						pCurl = msg->easy_handle;
						rc = msg->data.result;
						if (rc != CURLE_OK) {
							LogError(0, RS_RET_ERR,
									"omhttp: %s() - curl handle: %p, error code: %d:%s\n",
									__FUNCTION__, (void *)pCurl, rc,
									curl_multi_strerror(rc));
							// assert(0);
							continue;
						}

						// TODO: should we send the result as well?
						me->curlPostComplete(pCurl, rc, me->privateData);

						mcode = curl_multi_remove_handle(me->curlm, pCurl);
						if (mcode == CURLM_OK) {
							num_easy--;
							// TODO: remove
							count++;
#if 1
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
						WAITMS(100);
					}
				} else {
					repeats = 0;
				}
			}

		printf("iteration %d, 2xx responses: %d\n", ++i, count);
	}
	destroyIoQ(&me->sender_q);

	printf("total successful 200s responses: %d\n", count);
	printf("exiting thread.\n");
	pthread_exit(0);
}

static void init_apr(sender_t *sender) {
	if (apr_initialize() != APR_SUCCESS) {
		abort();
	}
	atexit(apr_terminate);

	apr_pool_create(&sender->_pool, NULL);
	apr_pool_tag(sender->_pool, "apr-util omhttp pool");
}

rsRetVal
omhttpSenderInit(sender_t *sender, size_t capacity,
		curlPostSetupCb curlPostSetup, curlPostCompleteCb curlPostComplete, curlPostSetOptsCb curlPostSetOpts,
		void *privateData)
{
	sender->tid = -1;
	sender->curlm = curl_multi_init();
	sender->curl_handles = NULL;
	sender->n_curl_handles = 0;
	sender->curl_handles = calloc(capacity, sizeof(CURL*));
	sender->n_curl_handles = capacity;

	sender->runstate = 0;
	sender->curlPostSetup = curlPostSetup;
	sender->curlPostComplete = curlPostComplete;
	sender->curlPostSetOpts = curlPostSetOpts;

	sender->privateData = privateData;

	_initCompressCtx(&sender->compressCtx);

	initIoQ(&sender->sender_q, capacity);
	/* apr related stuff */
	init_apr(sender);
	apr_status_t apr_rv = apr_queue_create(&sender->request_q, capacity, sender->_pool);
	assert(apr_rv == APR_SUCCESS);

	for (size_t i = 0; i < sender->n_curl_handles; ++i) {
		sender->curl_handles[i] = curl_easy_init();
	}
	return RS_RET_OK;
}

void start_send_worker(sender_t *sender) {
	printf("!!!! starting worker thread: %s...\n", sender->name);
	pthread_create(&sender->tid, NULL, sender_task, sender);
}

void stop_send_worker(sender_t *sender) {
	printf("!!!! stopping worker thread: %s !!!!\n", sender->name);
	sender->runstate = 1;
	apr_queue_term(sender->request_q);
}
// End new multi-threaded sender interface
