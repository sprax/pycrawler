import PyCrawler

fc = PyCrawler.FetchClient()
fc.set_config({
        "hostbins": hostbins,
        "frac_to_fetch": frac_to_fetch,
        "fetcher_options": {
            "DOWNLOAD_TIMEOUT":  options.download_timeout,
            "FETCHER_TIMEOUT":   options.fetcher_timeout,
            "NUM_FETCHERS":      options.num_fetchers,
            "CRAWLER_NAME":      options.name,
            "CRAWLER_HOMEPAGE":  options.homepage,
            }
        })

