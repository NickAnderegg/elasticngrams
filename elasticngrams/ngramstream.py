from string import ascii_lowercase, digits
from contextlib import closing
from collections import deque
import itertools
import requests
import gzip
import io
import json
import time
import threading
import pathlib
import csv
import queue
import sys

class NgramBase(object):
    def __init__(self):
        self.base_url = 'http://storage.googleapis.com/books/ngrams/books/googlebooks'
        self.languages = {
            'chinese_simplified': 'chi-sim-all',
            'english': 'eng-all',
            'english_million': 'eng-1M',
            'english_american': 'eng-US-all',
            'english_british': 'eng-gb-all',
            'english_fiction': 'eng-fiction-all'
        }
        self.versions = {
            2: '20120701',
            1: '20090715'
        }
        self.speech_parts = (
            '_ADJ_',    '_ADP_',    '_ADV_',    '_CONJ_',   '_DET_',
            '_NOUN_',   '_NUM_',    '_PRON_',   '_PRT_',    '_VERB_'
        )

        self.onegram_standalones = tuple(digits + ascii_lowercase) + (
            'other', 'pos', 'punctuation'
        )

        self.multigram_standalones = tuple(digits) + self.speech_parts
        self.multigram_combos = tuple(
            ''.join(combo) for combo in itertools.product(ascii_lowercase, '_'+ascii_lowercase)
        )

class NgramSources(NgramBase):
    def __init__(self, language=None, version=None):
        NgramBase.__init__(self)

        language = 'english' if language is None else language
        if language in self.languages.keys():
            self.language = self.languages[language]
        elif language in self.languages.values():
            self.language = language

        version = 2 if version is None else version
        if version in {1,2}:
            self.version = self.versions[version]
        elif version in self.versions.values():
            self.version = version

    def get_totals(self):
        totals_url = (
            'http://storage.googleapis.com/books/ngrams/books/googlebooks-'
            '{}-totalcounts-{}.txt'.format(
                self.language,
                self.version
            )
        )

        resp = requests.get(totals_url)
        totals_file = resp.text
        totals_file = totals_file.split('\t')[1:-1]

        totals = []
        for total in totals_file:
            total = total.split(',')
            totals.append({
                'year': int(total[0]),
                'counts': int(total[1]),
                'pages': int(total[2]),
                'volumes': int(total[3])
            })

        return totals

    def get_sizes(self):
        if not hasattr(self, 'sizing_threads'):

            self.source_sizes = deque()
            self.sizing_threads = []
            self.num_threads = 0

            for i in range(1, 6):
                for chunk in range(5):
                    self.sizing_threads.append(threading.Thread(target=self._get_sizes_thread, args=(i,5,chunk)))
                    self.sizing_threads[-1].start()

        return self.source_sizes

    def _get_sizes_thread(self, n, divisor=None, chunk=None):

        self.num_threads += 1
        if n == 1:
            sources_list = self.onegram_standalones
        else:
            sources_list = (self.multigram_standalones + self.multigram_combos)

        if divisor is not None and chunk is not None:
            chunk_start = int(len(sources_list)/divisor) * chunk
            if chunk == divisor-1:
                chunk_end   = len(sources_list)
            else:
                chunk_end   = int(len(sources_list)/divisor) * chunk + int(len(sources_list)/divisor)

            # print(n, len(sources_list), chunk_start, chunk_end)
            sources_list = sources_list[chunk_start:chunk_end]

        for source in sources_list:
            source_url = self._generate_source_url(n, source)
            resp = requests.head(source_url)
            size = int(resp.headers['content-length'])#/1000000
            source_info = {
                # 'language': self.languages[self.language],
                # 'version': self.versions[self.version],
                'ngram': n,
                'letters': source,
                'size': size,
                'url': source_url
            }

            self.source_sizes.appendleft(source_info)

        self.num_threads -= 1
        return

    def __len__(self):
        return len(self.source_sizes)

    def __next__(self):
        try:
            return self.source_sizes.pop()
        except IndexError:
            return False

    def _generate_source_url(self, ngram, letters):

        return (
            'http://storage.googleapis.com/books/ngrams/books/googlebooks-'
            '{}-{}gram-{}-{}.gz'.format(
                self.language,
                ngram,
                self.version,
                letters
            )
        )

class NgramStream(NgramBase):

    def __init__(self, ngram=None, letters=None, language=None, version=None,
                 min_year=None, max_year=None, volume_count=True, aggregate_count=False,
                 partial_aggregate=None, agg_min_year=None, agg_max_year=None):

        # Set class variables
        # self.ngram_stats    = NgramStats()
        # self.base_url       = self.ngram_stats.base_url
        # self.languages      = self.ngram_stats.languages
        # self.versions       = self.ngram_stats.versions
        # self.speech_parts   = self.ngram_stats.speech_parts
        #
        # self.onegram_standalones    = self.ngram_stats.onegram_standalones
        # self.multigram_standalones  = self.ngram_stats.multigram_standalones
        # self.multigtam_combos       = self.ngram_stats.multigtam_combos

        NgramBase.__init__(self)

        # Set default values
        ngram = 1 if ngram is None else ngram
        letters = '0' if letters is None else letters
        language = 'english' if language is None else language
        version = 2 if version is None else version
        min_year = False if min_year is None else min_year
        max_year = False if max_year is None else max_year
        volume_count = True if max_year is None else volume_count
        aggregate_count = False if aggregate_count is None else aggregate_count
        partial_aggregate = True if partial_aggregate is None else partial_aggregate
        agg_min_year = 1980 if agg_min_year is None else agg_min_year
        agg_max_year = 2012 if agg_max_year is None else agg_max_year

        # Set ngram instance variable
        if ngram in range(1,6):
            self.ngram = ngram
        else:
            raise ValueError('Invalid ngram argument. Acceptable values are 1-5.')

        # Set letters instance variable
        if self.ngram == 1:
            if letters in self.onegram_standalones:
                self.letters = letters
            else:
                raise ValueError('Letters argument is invalid for 1gram.')
        elif self.ngram in range(2,6):
            if letters in (self.multigram_standalones + self.multigram_combos):
                self.letters = letters
            else:
                raise ValueError('Letters argument is invalid for {}gram'.format(self.ngram))

        # Set language instance variable
        if language in self.languages.keys():
            self.language = self.languages[language]
        elif language in self.languages.values():
            self.language = language
        else:
            raise ValueError('Invalid language argument. Acceptable values are:\n  -{}'.format(
                '\n  -'.join(sorted([str(x) for x in self.languages.keys()]))
            ))

        # Set version instance variable
        if version in {1,2}:
            self.version = self.versions[version]
        elif version in self.versions.values():
            self.version = version
        else:
            raise ValueError('Invalid version number. Use version 1 or 2, not the version date.')

        # Set parsing-related instance variables
        self.min_year = min_year
        self.max_year = max_year
        self.volume_count = volume_count
        self.aggregate_count = aggregate_count
        self.partial_aggregate = partial_aggregate
        self.agg_min_year = agg_min_year
        self.agg_max_year = agg_max_year

        self.resource_url = self._generate_url()

        self.ngram_stream = deque()
        self.failure_signal = None

    def _generate_url(self):

        return '{}-{}-{}-{}-{}.gz'.format(
            self.base_url,
            self.language,
            '{}gram'.format(self.ngram),
            self.version,
            self.letters
        )

    def __next__(self):
        try:
            return self.ngram_stream.pop()
        except IndexError:
            return False

    def __len__(self):
        return len(self.ngram_stream)

    def download(self, signal=None):
        self.failure_signal = signal
        self.download_thread = threading.Thread(target=self._download_thread)
        self.download_thread.start()

        return

    def _download_thread(self):
        self.thread_live = True
        def next_line(retry_counter=0):
            try:
                return self.zipped.readline()
            except ValueError:
                self.thread_live = False
                return False
            except:
                if retry_counter < 5:
                    retry_counter += 1
                    return next_line(retry_counter)
                else:
                    if self.failure_signal is not None:
                        self.failure_signal.put(sys.exc_info())
                        self.thread_live = False
                        return False
                    else:
                        raise RuntimeError('Failed to get next line from ngram stream')

        with closing(requests.get(self.resource_url, stream=True)) as r:

            resource_size = int(r.headers['content-length'])
            if  resource_size == 0:
                print('No such resource: {}'.format(self.resource_url))
                self.thread_live = False
                return

            file_buffer = 65536
            if resource_size <= file_buffer:
                file_buffer = int(resource_size / 2)

            self.buff = io.BufferedReader(r.raw, buffer_size=file_buffer)
            self.zipped = gzip.GzipFile(mode='rb', fileobj=self.buff)

            try:
                line = self.zipped.readline()
            except OSError:
                # print('Skipped {}'.format(self.resource_url))
                # file_loc = pathlib.Path('downloaded/{}grams/condensed-{}.csv'.format(xgram,file_ref))
                # file_loc.touch()
                self.thread_live = False
                return

            timestamp = time.perf_counter()
            total_time = 0

            # extracted_ngrams = []
            current_ngram_text = ''
            current_ngram = {}

            line_no     = 0
            added       = 0
            chunk       = 0
            while line:
                line_no += 1

                if line_no % 1000000 == 0:
                    curr = time.perf_counter() - timestamp
                    total_time += curr
                    print('Extracted {} lines in {:.2f}s ({}/s) | Processed: {} lines in {:.1f}s ({}/s)'.format(chunk, curr, int(chunk/curr), line_no, total_time, int(line_no/total_time)))
                    timestamp = time.perf_counter()
                    chunk = 0

                line_full   = line.decode().split('\t')
                ngram_year  = int(line_full[1])

                if self.min_year and ngram_year < self.min_year:
                    line = next_line()
                    continue
                if self.max_year and ngram_year > self.max_year:
                    line = next_line()
                    continue

                ngram_full  = line_full[0]
                ngram_split = ngram_full.split(' ')
                ngram_count = int(line_full[2])
                ngram_vols  = int(line_full[3])

                chunk += 1
                added += 1
                if ngram_full != current_ngram_text:
                    if current_ngram != {}:
                        self.ngram_stream.appendleft(current_ngram)
                    current_ngram_text = ngram_full
                    current_ngram = {
                        'n': len(ngram_split),
                        'ngram_full': ngram_full
                    }

                    if self.max_year:
                        current_ngram['max_year'] = self.max_year
                    if self.min_year:
                        current_ngram['min_year'] = self.min_year

                    for i in range(0, self.ngram):
                        current_ngram['token_{}'.format(i+1)] = ngram_split[i]

                    current_ngram['total_count'] = ngram_count
                    if self.volume_count:
                        current_ngram['volumes_count'] = ngram_vols
                    if not self.aggregate_count:
                        current_ngram['year_counts'] = {ngram_year: ngram_count}
                        if self.volume_count:
                            current_ngram['year_vols'] = {ngram_year: ngram_vols}

                        if (self.partial_aggregate
                          and self.agg_min_year is not None
                          and self.agg_max_year is not None):
                            current_ngram['partial_aggregate_min'] = self.agg_min_year
                            current_ngram['partial_aggregate_max'] = self.agg_max_year
                            current_ngram['partial_aggregate'] = 0

                            if self.volume_count:
                                current_ngram['partial_aggregate_vols'] = 0

                            if ngram_year >= self.agg_min_year and ngram_year <= self.agg_max_year:
                                current_ngram['partial_aggregate'] = ngram_count

                                if self.volume_count:
                                    current_ngram['partial_aggregate_vols'] = ngram_vols
                else:
                    current_ngram['total_count'] += ngram_count
                    if self.volume_count:
                        current_ngram['volumes_count'] += ngram_vols
                    if not self.aggregate_count:
                        current_ngram['year_counts'][ngram_year] = ngram_count
                        if self.volume_count:
                            current_ngram['year_vols'][ngram_year] = ngram_count

                        if (self.partial_aggregate and
                          self.agg_min_year is not None and
                          self.agg_max_year is not None):
                            if ngram_year >= self.agg_min_year and ngram_year <= self.agg_max_year:
                                current_ngram['partial_aggregate'] += ngram_count

                                if self.volume_count:
                                    current_ngram['partial_aggregate_vols'] += ngram_vols


                line = next_line()

        self.thread_live = False
        return

                # if line_no % 100000 == 0:
                #     print(json.dumps(self.next_ngram(),indent=2))
                # if line_no > 10000:
                #     return

                # if line_no % 1000000 == 0:
                #     curr = time.perf_counter() - timestamp
                #     total_time += curr
                #     print('Extracted {} in {:.2f}s ({}/s) | Processed: {} in {:.1f}s ({}/s)'.format(chunk, curr, int(chunk/curr), line_no, total_time, int(line_no/total_time)))
                #     timestamp = time.perf_counter()
                #     chunk = 0
