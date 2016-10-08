import requests
import json
import time
import threading
import hashlib
import random
import queue
from collections import deque
from .ngramstream import NgramBase, NgramSources, NgramStream

class ElasticUtility(object):
    def __init__(self, database_url, language, version):

        database_url = 'http://localhost:9200' if database_url is None else database_url
        if database_url[-1] == '/':
            database_url = database_url[:-1]
        language = 'english' if language is None else language
        version = 2 if version is None else version

        ngram_info = NgramBase()

        if language in ngram_info.languages.keys():
            self.language = ngram_info.languages[language]
        else:
            raise ValueError('Invalid language argument. Acceptable values are:\n  -{}'.format(
                '\n  -'.join(sorted([str(x) for x in ngram_info.languages.keys()]))
            ))

        if version in {1,2}:
            self.version = ngram_info.versions[version]
        else:
            raise ValueError('Invalid version number. Use version 1 or 2, not the version date.')

        try:
            db_ping = requests.get(database_url)
            if db_ping.status_code == requests.codes.ok:
                self.database_url = database_url
        except:
            raise RuntimeError('Could not contact elasticsearch database at {}'.format(database_url))

        self.mapping = '''{
            "mappings": {
                "source": {
                    "properties": {
                        "ngram": {
                            "type": "integer"
                        },
                        "letters": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "size": {
                            "type": "long",
                            "index": "yes"
                        },
                        "url": {
                            "type": "string",
                            "index": "no"
                        }
                    }
                },
                "ngram": {
                    "properties": {
                        "n": {
                            "type": "short"
                        },
                        "ngram_full": {
                            "type": "string",
                            "index": "no"
                        },
                        "token_1": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "token_2": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "token_3": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "token_4": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "token_5": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "total_count": {
                            "type": "integer"
                        },
                        "volumes_count": {
                            "type": "integer"
                        },
                        "year_counts": {
                            "type": "object",
                            "dynamic": "true"
                        },
                        "year_vols": {
                            "type": "object",
                            "dynamic": "true"
                        },
                        "partial_aggregate_min": {
                            "type": "short"
                        },
                        "partial_aggregate_max": {
                            "type": "short"
                        },
                        "partial_aggregate": {
                            "type": "integer"
                        },
                        "partial_aggregate_vols": {
                            "type": "integer"
                        },
                        "max_year": {
                            "type": "short"
                        },
                        "min_year": {
                            "type": "short"
                        }
                    }
                },
                "total": {
                    "properties": {
                        "year": {
                            "type": "short"
                        },
                        "counts": {
                            "type": "long",
                            "index": "yes"
                        },
                        "pages": {
                            "type": "long",
                            "index": "yes"
                        },
                        "volumes": {
                            "type": "long",
                            "index": "yes"
                        }
                    }
                }
            }
        }'''

        self.index = 'ngrams-{}-{}'.format(
            self.language,
            self.version
        )
        self.index_url = '{}/ngrams-{}-{}'.format(
            self.database_url,
            self.language,
            self.version
        )

        self.sources = NgramSources(self.language, self.version)

    def put_mappings(self):
        resp = requests.get(self.index_url)
        if resp.status_code != requests.codes.ok:
            resp = requests.put(self.index_url, data=self.mapping)
        else:
            if json.loads(self.mapping)['mappings'] != resp.json()['ngrams-{}-{}'.format(self.language, self.version)]['mappings']:
                raise RuntimeError('Index at {} already exists and does not have correct mappings. Cannot continue.'.format(self.index_url))

    def get_totals(self):
        totals = self.sources.get_totals()
        bulk_string = []
        for total in totals:
            create_string = {
                "index": {
                    "_index": "{}".format(self.index),
                    "_type": "total",
                    "_id": "{}".format(total['year'])
                }
            }
            bulk_string.append(json.dumps(create_string).replace('\n', ' '))
            bulk_string.append(json.dumps(total, ensure_ascii=False).replace('\n', ' '))
            # resp = requests.put('{}/total/{}'.format(self.index_url, total['year']), data=json.dumps(total))
            # if resp.status_code not in {requests.codes.created, requests.codes.ok}:
            #     print(resp.status_code, resp.text)

        bulk_string.append(' ')
        bulk_string = bytearray('\n'.join(bulk_string), 'utf-8')
        resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
        if resp.status_code not in {requests.codes.created, requests.codes.ok}:
            print(resp.status_code, resp.text)

        time.sleep(2)
        totals_count = requests.get('{}/total/_count'.format(self.index_url))
        totals_count = totals_count.json()['count']

        if totals_count != len(totals):
            raise RuntimeError('Number of totals in database ({}) does not match total submitted ({})'.format(totals_count, len(totals)))

    def get_sizes(self):
        self.sources.get_sizes()

        bulk_string = []
        total_sources = 0
        time.sleep(2)
        while(True):

            size_data = next(self.sources)
            if not size_data and self.sources.num_threads == 0:
                break
            if not size_data:
                time.sleep(1)
                continue

            create_string = {
                "index": {
                    "_index": "{}".format(self.index),
                    "_type": "source",
                    "_id": "{}gram-{}".format(size_data['ngram'], size_data['letters'])
                }
            }
            bulk_string.append(json.dumps(create_string).replace('\n', ' '))
            bulk_string.append(json.dumps(size_data, ensure_ascii=False).replace('\n', ' '))
            total_sources += 1

            if len(bulk_string) % 1000 == 0:
                bulk_string.append(' ')
                bulk_string = bytearray('\n'.join(bulk_string), 'utf-8')
                resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
                if resp.status_code not in {requests.codes.created, requests.codes.ok}:
                    print(resp.status_code, resp.text)

                bulk_string = []

        if len(bulk_string) > 0:
            bulk_string.append(' ')
            bulk_string = bytearray('\n'.join(bulk_string), 'utf-8')
            resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
            if resp.status_code not in {requests.codes.created, requests.codes.ok}:
                print(resp.status_code, resp.text)

            bulk_string = []

        time.sleep(2)
        sources_count = requests.get('{}/source/_count'.format(self.index_url))
        sources_count = sources_count.json()['count']

        if sources_count != total_sources:
            raise RuntimeError('Number of sources in database ({}) does not match total submitted ({})'.format(sources_count, total_sources))

class ElasticInterface(ElasticUtility):
    def __init__(self, database_url, language, version,
                min_year=None, max_year=None, volume_count=True, aggregate_count=False,
                partial_aggregate=None, agg_min_year=None, agg_max_year=None):

        ElasticUtility.__init__(self, database_url, language, version)

        self.min_year       = min_year
        self.max_year       = max_year
        self.volume_count   = volume_count
        self.aggregate_count    = aggregate_count
        self.partial_aggregate  = partial_aggregate
        self.agg_min_year   = agg_min_year
        self.agg_max_year   = agg_max_year

    def create_index(self):
        self.put_mappings()
        self.get_totals()
        self.get_sizes()

    def _update_unprocessed(self):
        unprocessed_query = {
            "sort": {"size": {"order": "desc"} },
            "query": {
                "constant_score": {
                    "filter": {
                        "missing": { "field": "downloaded" }
                    }
                }
            }
        }
        unprocessed_count = requests.post(
            '{}/source/_count'.format(self.index_url),
            data=json.dumps(unprocessed_query)
        )
        unprocessed_count = unprocessed_count.json()['count']

        unprocessed_query['size'] = unprocessed_count
        unprocessed_sources = requests.post(
            '{}/source/_search'.format(self.index_url),
            data=json.dumps(unprocessed_query)
        )

        unprocessed_sources = deque(unprocessed_sources.json()['hits']['hits'])
        random.shuffle(unprocessed_sources)

        self.unprocessed_sources = unprocessed_sources

    def download_ngrams(self):

        self.stream_threads = []
        self.stream_count = 0
        self.downloaded_ngrams = deque()
        self._update_unprocessed()

        for i in range(1):
            self.stream_threads.append(
                threading.Thread(target=self._download_thread, args=(i,))
            )
            self.stream_threads[-1].start()
            time.sleep(0.1)

        self.download_counter = 0
        self.upload_threads = []
        for i in range(3):
            self.upload_threads.append(
                threading.Thread(target=self._upload_thread)
            )
            self.upload_threads[-1].start()
            time.sleep(10)

        for thread in self.upload_threads:
            thread.join()

    def _upload_thread(self):
        bulk_string = deque()
        while self.stream_count > 0:
            if len(self.downloaded_ngrams) > 0:
                try:
                    next_ngram = self.downloaded_ngrams.popleft()
                    if len(self.downloaded_ngrams) > 5000:
                        time.sleep((len(self.downloaded_ngrams)**2) * 0.0000003)
                except IndexError:
                    continue

                create_string = '''{{"index": {{"_index": "{}","_type": "ngram","_id": "{}{}"}}}}'''.format(
                    self.index,
                    next_ngram['n'],
                    hashlib.md5(bytes(next_ngram['ngram_full'], 'utf-8')).hexdigest()
                )
                bulk_string.append(bytes(create_string, 'utf-8'))
                bulk_string.append(bytes(json.dumps(next_ngram, ensure_ascii=False).replace('\n', ' '), 'utf-8'))
                self.download_counter += 1

                # if self.download_counter % 50000 == 0:
                #     print('Upload thread download counter: {} ngrams'.format(self.download_counter))

                if (len(bulk_string)/2) % 7500 == 0 and len(bulk_string) > 0:
                    print('Uploading {} on thread {} | {} in download queue'.format(int(len(bulk_string)/2), threading.get_ident(), len(self.downloaded_ngrams)))
                    # print('Total downloaded: {}'.format(self.download_counter))
                    # encoding_start = time.perf_counter()
                    bulk_string.append(b' ')
                    bulk_string = b'\n'.join(bulk_string)
                    # print('Finished encoding on thread {} in {}s'.format(threading.get_ident(), int(time.perf_counter() - encoding_start)))
                    # upload_start = time.perf_counter()
                    resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
                    if resp.status_code not in {requests.codes.created, requests.codes.ok}:
                        print(resp.status_code, resp.text)
                    # else:
                    #     print('Upload on thread {} completed in {}s'.format(threading.get_ident(), int(time.perf_counter() - upload_start)))

                    # print("{} threads running".format(threading.active_count()))

                    bulk_string = deque()

        bulk_string.append(' ')
        bulk_string = bytearray('\n'.join(bulk_string), 'utf-8')
        resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
        if resp.status_code not in {requests.codes.created, requests.codes.ok}:
            print(resp.status_code, resp.text)

        print('\n\nUpload thread {} is exiting...\n\n'.format(threading.get_ident()))

    def _download_thread(self, thread_index=0):
        self.stream_count += 1
        while(len(self.unprocessed_sources) > 0):
            start_time = time.perf_counter()

            if thread_index == 0:
                self._update_unprocessed()
                print('Shuffling unprocessed sources...')

            ngram_count = 0
            source = self.unprocessed_sources.pop()

            ngram_info = source['_source']
            print('Processing {}gram-{}...'.format(ngram_info['ngram'], ngram_info['letters']))
            stream = NgramStream(
                ngram       = ngram_info['ngram'],
                letters     = ngram_info['letters'],
                language    = self.language,
                version     = self.version,
                min_year    = self.min_year,
                max_year    = self.max_year,
                volume_count    = self.volume_count,
                aggregate_count = self.aggregate_count,
                partial_aggregate   = self.partial_aggregate,
                agg_min_year    = self.agg_min_year,
                agg_max_year    = self.agg_max_year
            )

            failure_signal = queue.Queue(maxsize=1)
            stream.download(signal=failure_signal)

            next_ngram = next(stream)
            while(stream.thread_live or next_ngram):
                if next_ngram is not False:
                    self.downloaded_ngrams.append(next_ngram)
                    ngram_count += 1

                    if ngram_count % 10000 == 0:
                        download_time = int(time.perf_counter() - start_time) + 0.00001
                        print('Downloaded {} ngrams from {}gram-{} at {}/sec | {} in download queue'.format(ngram_count, ngram_info['ngram'], ngram_info['letters'], (ngram_count/download_time), len(self.downloaded_ngrams)))

                    if len(self.downloaded_ngrams) > 15000:
                        time.sleep(len(self.downloaded_ngrams) * 0.0000001)

                next_ngram = next(stream)

            if failure_signal.empty():

                download_time = int(time.perf_counter() - start_time) + 0.00001
                print('Downloaded {} from {}gram-{} at {}/sec'.format(ngram_count, ngram_info['ngram'], ngram_info['letters'], (ngram_count/download_time)))
                mark_downloaded = requests.post(
                    '{}/source/{}/_update'.format(self.index_url, source['_id']),
                    data=json.dumps({
                        "doc": {
                            "downloaded": "true",
                            "download_count": ngram_count,
                            "download_duration": download_time
                        }
                    })
                )
            else:
                exc_type, exc_value, exc_traceback = failure_signal.get()
                if exc_type is RuntimeError:
                    print('Failure to download {}gram-{}:'.format(ngram_info['ngram'], ngram_info['letters']))
                    print('\t - RuntimeError: {}'.format(exc_value))

            ngram_count = 0

        self.stream_count -= 1
        return
