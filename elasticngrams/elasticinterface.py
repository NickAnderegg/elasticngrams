import requests
import json
import time
import threading
import hashlib, base64
import random
import sys

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

        self.sources_mapping = '''{
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

        self.ngrams_mapping = '''{
            "mappings": {
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
                }
            }
        }'''

        self.sources_index = 'ngrams-{}-{}-sources'.format(
            self.language,
            self.version
        )

        self.ngrams_index = 'ngrams-{}-{}-ngrams'.format(
            self.language,
            self.version
        )

        self.sources_index_url = '{}/{}'.format(
            self.database_url,
            self.sources_index
        )

        self.ngrams_index_url = '{}/{}'.format(
            self.database_url,
            self.ngrams_index
        )

        self.sources = NgramSources(self.language, self.version)

    def put_mappings(self):
        resp = requests.get(self.sources_index_url)
        if resp.status_code != requests.codes.ok:
            resp = requests.put(self.sources_index_url, data=self.sources_mapping)
        else:
            if json.loads(self.sources_mapping)['mappings'] != resp.json()['ngrams-{}-{}-sources'.format(self.language, self.version)]['mappings']:
                raise RuntimeError('Index at {} already exists and does not have correct mappings. Cannot continue.'.format(self.sources_index_url))

        resp = requests.get(self.ngrams_index_url)
        if resp.status_code != requests.codes.ok:
            resp = requests.put(self.ngrams_index_url, data=self.ngrams_mapping)
        else:
            if json.loads(self.ngrams_mapping)['mappings'] != resp.json()['ngrams-{}-{}-ngrams'.format(self.language, self.version)]['mappings']:
                raise RuntimeError('Index at {} already exists and does not have correct mappings. Cannot continue.'.format(self.ngrams_index_url))

    def get_totals(self):
        totals = self.sources.get_totals()
        bulk_string = []
        for total in totals:
            create_string = {
                "index": {
                    "_index": "{}".format(self.sources_index),
                    "_type": "total",
                    "_id": "{}".format(total['year'])
                }
            }
            bulk_string.append(json.dumps(create_string).replace('\n', ' '))
            bulk_string.append(json.dumps(total, ensure_ascii=False).replace('\n', ' '))

        bulk_string.append(' ')
        bulk_string = bytearray('\n'.join(bulk_string), 'utf-8')
        resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
        if resp.status_code not in {requests.codes.created, requests.codes.ok}:
            print(resp.status_code, resp.text)

        time.sleep(2)
        totals_count = requests.get('{}/total/_count'.format(self.sources_index_url))
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

            if size_data['size'] < 51200:
                continue

            create_string = {
                "index": {
                    "_index": "{}".format(self.sources_index),
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
        sources_count = requests.get('{}/source/_count'.format(self.sources_index_url))
        sources_count = sources_count.json()['count']

        if sources_count != total_sources:
            raise RuntimeError('Number of sources in database ({}) does not match total submitted ({})'.format(sources_count, total_sources))

class NgramDownloader(ElasticUtility):
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
            '{}/source/_count'.format(self.sources_index_url),
            data=json.dumps(unprocessed_query)
        )
        unprocessed_count = unprocessed_count.json()['count']

        unprocessed_query['size'] = unprocessed_count
        unprocessed_sources_raw = requests.post(
            '{}/source/_search'.format(self.sources_index_url),
            data=json.dumps(unprocessed_query)
        ).json()['hits']['hits']

        unprocessed_sources = deque()
        curr_time = time.time()
        for source in unprocessed_sources_raw:
            if 'last_processed' in source['_source']:
                if curr_time - source['_source']['last_processed'] > 600:
                    unprocessed_sources.append(source)
            else:
                unprocessed_sources.append(source)

        # unprocessed_sources = unprocessed_sources[:int(len(unprocessed_sources)/5)+1]
        # random.shuffle(unprocessed_sources)

        self.unprocessed_sources = unprocessed_sources

    def download_ngrams(self):

        self.run_event = threading.Event()
        self.run_event.set()

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

        # self.download_counter = 0
        try:
            self.upload_threads = []
            for i in range(2):
                self.upload_threads.append(
                    threading.Thread(target=self._upload_thread)
                )
                self.upload_threads[-1].start()
                time.sleep(1)

            for thread in self.upload_threads:
                thread.join()
        except KeyboardInterrupt:
            self.run_event.clear()
            print('Attempting to close threads...')
            for thread in self.upload_threads:
                thread.join()
            print('Shutdown successful!')

    def ngram_id(self, n, letters, full_ngram):
        ngram_hash = ''
        for tok in full_ngram.split():
            ngram_hash += hex(ord(tok[0]))[2:].rjust(3, '0')[:3]
        ngram_hash = ngram_hash.rjust(15, '0')

        return '{}-{}-{}-{}'.format(
            n,
            bytes(letters, 'utf-8').hex()[:4].rjust(4, '0'),
            ngram_hash,
            base64.b64encode(hashlib.md5(bytes(full_ngram, 'utf-8')).digest()).decode('utf-8')[:16]
        )

    def _upload_thread(self):
        bulk_string = deque()
        while self.stream_count > 0 and self.run_event.is_set():
            if len(self.downloaded_ngrams) > 0:
                try:
                    next_ngram = self.downloaded_ngrams.popleft()
                except IndexError:
                    # print('Index error...')
                    time.sleep(0.1)
                    continue

                create_string = '''{{"index": {{"_index": "{}","_type": "ngram","_id": "{}"}}}}'''.format(
                    self.ngrams_index,
                    self.ngram_id(next_ngram['n'], next_ngram['letters'], next_ngram['ngram_full'])
                )
                bulk_string.append(bytes(create_string, 'utf-8'))
                bulk_string.append(bytes(json.dumps(next_ngram, ensure_ascii=False).replace('\n', ' '), 'utf-8'))
                # self.download_counter += 1

                # if self.download_counter % 50000 == 0:
                #     print('Upload thread download counter: {} ngrams'.format(self.download_counter))

                if (len(bulk_string)/2) % 15000 == 0 and len(bulk_string) > 0:
                    # print('Total downloaded: {}'.format(self.download_counter))
                    # encoding_start = time.perf_counter()
                    bulk_string.append(b' ')
                    bulk_string = b'\n'.join(bulk_string)
                    print('Uploading 15000 on thread {} | {} in download queue'.format(threading.get_ident(), len(self.downloaded_ngrams)))
                    # print('Finished encoding on thread {} in {}s'.format(threading.get_ident(), int(time.perf_counter() - encoding_start)))
                    upload_start = time.perf_counter()
                    resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
                    if resp.status_code not in {requests.codes.created, requests.codes.ok}:
                        print(resp.status_code, resp.text)
                    else:
                        print('Uploaded 15000 on thread {} in {:.2f}s | {} in download queue'.format(threading.get_ident(), (time.perf_counter() - upload_start), len(self.downloaded_ngrams)))
                    # else:
                    #     print('Upload on thread {} completed in {}s'.format(threading.get_ident(), int(time.perf_counter() - upload_start)))

                    # print("{} threads running".format(threading.active_count()))

                    bulk_string = deque()

        if self.run_event.is_set():
            bulk_string.append(' ')
            bulk_string = bytearray('\n'.join(bulk_string), 'utf-8')
            resp = requests.post('{}/_bulk'.format(self.database_url), data=bulk_string)
            if resp.status_code not in {requests.codes.created, requests.codes.ok}:
                print(resp.status_code, resp.text)

        print('\n\nUpload thread {} is exiting...\n\n'.format(threading.get_ident()))

    def _download_thread(self, thread_index=0):
        self.stream_count += 1
        while len(self.unprocessed_sources) > 0 and self.run_event.is_set():
            start_time = time.perf_counter()

            ngram_count = 0
            self._update_unprocessed()
            if len(self.unprocessed_sources) > 0:
                source = self.unprocessed_sources.pop()
            else:
                continue

            updated_processed = requests.post(
                '{}/source/{}/_update'.format(self.sources_index_url, source['_id']),
                data=json.dumps({
                    "doc": {
                        "last_processed": int(time.time())
                    }
                })
            )

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
            stream.download(signal=failure_signal, run_event=self.run_event)

            download_time = time.perf_counter()
            next_ngram = next(stream)
            while stream.thread_live or next_ngram:
                if not self.run_event.is_set():
                    break
                if next_ngram is not False:
                    self.downloaded_ngrams.append(next_ngram)
                    ngram_count += 1

                    if ngram_count % 10000 == 0:
                        download_time = max((time.perf_counter() - download_time), 1)
                        avg_time = max(int(time.perf_counter() - start_time), 1)
                        print('Downloaded {} ngrams from {}gram-{} in {:.1f} at {:.1f}/sec (avg. {:.1f}/sec) | {} in download queue'.format(ngram_count, ngram_info['ngram'], ngram_info['letters'], download_time, (10000/download_time), (ngram_count/avg_time), len(self.downloaded_ngrams)))
                        download_time = time.perf_counter()

                        updated_processed = requests.post(
                            '{}/source/{}/_update'.format(self.sources_index_url, source['_id']),
                            data=json.dumps({
                                "doc": {
                                    "last_processed": int(time.time())
                                }
                            })
                        )

                    if len(self.downloaded_ngrams) > 15000:
                        time.sleep((len(self.downloaded_ngrams)**2) * 0.0000003)

                next_ngram = next(stream)

            if failure_signal.empty() and self.run_event.is_set():
                avg_time = max(int(time.perf_counter() - start_time), 1)
                print('Downloaded {} from {}gram-{} at {:.1f}/sec'.format(ngram_count, ngram_info['ngram'], ngram_info['letters'], (ngram_count/avg_time)))
                mark_downloaded = requests.post(
                    '{}/source/{}/_update'.format(self.sources_index_url, source['_id']),
                    data=json.dumps({
                        "doc": {
                            "downloaded": "true",
                            "download_count": ngram_count,
                            "download_duration": avg_time,
                            "line_count": stream.line_count,
                            "extracted_count": stream.extracted_count
                        }
                    })
                )
            elif self.run_event.is_set():
                exc_type, exc_value, exc_traceback = failure_signal.get()
                print('Failure to download {}gram-{}:'.format(ngram_info['ngram'], ngram_info['letters']))
                print('\t - Exception: {}'.format(exc_value))
                print('\t - Message: {}'.format(exc_type))
                print('\t - Traceback: {}'.format(exc_traceback))
            else:
                stream.download_thread.join()

            ngram_count = 0

        print('\n\nDownload thread {} is exiting...\n\n'.format(threading.get_ident()))
        self.stream_count -= 1
        return

class SourceInterface(ElasticUtility):
    def __init__(self, database_url, language, version):
        import numpy as np
        import scipy.stats as stats
        import math

        ElasticUtility.__init__(self, database_url, language, version)

    def get_sources(self):
        query_all_sources = '''
            {
                "sort": {"size": {"order": "asc"} },
                "size": 10000,
                "query": {
                    "match_all": {}
                }
            }
        '''
        resp = requests.post(
            '{}/source/_search'.format(self.sources_index_url),
            data=query_all_sources
        )

        if resp.status_code != requests.codes.ok:
            print(resp.request.body)
            raise RuntimeError('Ngram sources response failed: response code {}'.format(resp.status_code))

        resp_all_sources = resp.json()['hits']['hits']

        sources = dict()
        for source in resp_all_sources:
            source_id = source['_id']
            source = source['_source']
            sources[source_id] = {}
            keys = (
                'ngram', 'letters', 'size',
                'downloaded', 'download_duration', 'download_count',
                'prev_ratio'
            )

            for key in keys:
                if key in source:
                    # if key == 'download_duration':
                    #     sources[source_id][key] = int(source[key])
                    if key == 'downloaded':
                        sources[source_id][key] = bool(source[key])
                    else:
                        sources[source_id][key] = source[key]
                elif key == 'downloaded':
                    sources[source_id]['downloaded'] = False

        return sources

    def get_ratio_outliers(self):
        sources = self.get_sources()
        rgb = ['#4286f4', '#f442ee', '#f44242', '#f4eb42', '#42f450']
        population = [[],[],[],[],[]]
        source_ratio = [[],[],[],[],[]]
        for key, value in sources.items():
            if value['downloaded'] and value['size'] > 1048576:
                ratio = value['size']/value['download_count']
                source_ratio[value['ngram']-1].append((ratio, key))
                if ratio < 400:
                    population[value['ngram']-1].append(ratio)

        out_pop = []
        for ix, row in enumerate(source_ratio):
            # print('{}gram:'.format(ix+1))
            pop_mean, pop_std = stats.hmean(population[ix]), np.std(population[ix])
            # print('Mean:', pop_mean, 'STD:', pop_std)

            for ratio, key in row:
                if 'prev_ratio' in sources[key]:
                    ratio_diff = min(ratio, sources[key]['prev_ratio'])/max(ratio, sources[key]['prev_ratio'])
                    if ratio_diff > 0.95:
                        continue

                if (pop_mean - (pop_std * 5)) < ratio < (pop_mean + (pop_std * 5)):
                    continue

                statistic, pvalue = stats.ttest_ind_from_stats(
                    pop_mean, pop_std, len(population),
                    ratio, 0, 1
                )

                if pvalue < 0.10:
                    # out_pop.append(('Out of pop:', key, 'Value:', str(ratio), 'T=', str(statistic), 'p=', str(pvalue)))
                    # print(' '.join(out_pop[-1]))
                    out_pop.append((key, ratio))

        return out_pop

    def void_ratio_outliers(self):
        outliers = self.get_ratio_outliers()

        for outlier in outliers:
            key, ratio = outlier

            update_script = json.dumps({
                'script': '{}'.format(' '.join([
                    'ctx._source.remove(\"downloaded\");',
                    'ctx._source.remove(\"download_duration\");',
                    'ctx._source.remove(\"download_count\");',
                    'ctx._source.remove(\"last_processed\");',
                    'ctx._source.prev_ratio = {:.2f};'.format(ratio)
                ]))
            })

            update = requests.post(
                '{}/source/{}/_update'.format(self.sources_index_url, key),
                data=update_script
            )

            # print(update.request.url)
            # print(update.request.body)
            # print(update.json())


class NgramInterface(ElasticUtility):
    def __init__(self):
        pass
