import requests
import json
import time
import threading
import hashlib
import random
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

        for i in range(2):
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
            else:
                continue

            if len(bulk_string) % 25000 == 0 and len(bulk_string) > 0:
                print('Downloaded {} on thread {}'.format(len(bulk_string), threading.get_ident()))
            if len(bulk_string) % 25000 == 0 and len(bulk_string) > 0:
                bulk_string.append(b' ')
                bulk_string = b'\n'.join(bulk_string)
                resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
                if resp.status_code not in {requests.codes.created, requests.codes.ok}:
                    print(resp.status_code, resp.text)

                # print("{} threads running".format(threading.active_count()))

                bulk_string = deque()

        bulk_string.append(' ')
        bulk_string = bytearray('\n'.join(bulk_string), 'utf-8')
        resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
        if resp.status_code not in {requests.codes.created, requests.codes.ok}:
            print(resp.status_code, resp.text)

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

            stream.download()
            next_ngram = next(stream)
            while(stream.thread_live or next_ngram):
                if next_ngram is not False:
                    self.downloaded_ngrams.append(next_ngram)
                    ngram_count += 1

                next_ngram = next(stream)

            download_time = int(time.perf_counter() - start_time) + 0.01
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

            ngram_count = 0

        self.stream_count -= 1
        return