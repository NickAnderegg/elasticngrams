#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__version__ = "0.1.0"


PROGRAMNAME="elasticngrams"
VERSION=__version__
COPYRIGHT="(C) 2016 Nick Anderegg"

from elasticngrams import elasticinterface

def main():
    ob = elasticinterface.NgramDownloader(
        'http://192.168.1.150:9200/',
        'chinese_simplified', 2,
        min_year=1980, aggregate_count=True
    )
    ob.download_ngrams()

if __name__ == "__main__":
    main()
