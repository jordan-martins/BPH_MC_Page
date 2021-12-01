import sys
import os
import json
import traceback
import logging
import time

try:
    import cookielib
except ImportError:
    import http.cookiejar as cookielib

try:
    import urllib2 as urllib
    from urllib2 import HTTPError as HTTPError
except ImportError:
    import urllib.request as urllib
    from urllib.error import HTTPError as HTTPError


class Stats2:
    def __init__(self, debug=False, cookie=None):
        self.host = 'cms-pdmv.cern.ch'
        self.server = 'https://' + self.host + '/stats'
        # Set up logging
        if debug:
            logging_level = logging.DEBUG
        else:
            logging_level = logging.INFO

        if cookie:
            self.cookie = cookie
        else:
            home = os.getenv('HOME')
            self.cookie = '%s/private/stats2-cookie.txt' % (home)

        # Set up logging
        logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging_level)
        self.logger = logging.getLogger()
        # Create opener
        self.__connect()
        # Request retries
        self.max_retries = 3

    def __connect(self):
        if not os.path.isfile(self.cookie):
            self.logger.info('SSO cookie file is absent. Will try to make one for you...')
            self.__generate_cookie()
            if not os.path.isfile(self.cookie):
                self.logger.error('Missing cookie file %s, quitting', self.cookie)
                sys.exit(1)
        else:
            self.logger.info('Using SSO cookie file %s' % (self.cookie))

        cookie_jar = cookielib.MozillaCookieJar(self.cookie)
        cookie_jar.load()
        for cookie in cookie_jar:
            self.logger.debug('Cookie %s', cookie)

        self.opener = urllib.build_opener(urllib.HTTPCookieProcessor(cookie_jar))

    def __generate_cookie(self):
        # use env to have a clean environment
        command = 'env -i KRB5CCNAME="$KRB5CCNAME" cern-get-sso-cookie -u %s -o %s --reprocess --krb' % (self.server, self.cookie)
        self.logger.debug(command)
        output = os.popen(command).read()
        self.logger.debug(output)
        if not os.path.isfile(self.cookie):
            self.logger.error('Could not generate SSO cookie.\n%s', output)

    def __http_request(self, url, parse_json=True):
        url = self.server + url
        self.logger.debug('%s', url)
        headers = {'User-Agent': 'Stats2 Scripting'}
        retries = 0
        response = None
        while retries < self.max_retries:
            request = urllib.Request(url, headers=headers)
            try:
                retries += 1
                response = self.opener.open(request)
                response = response.read()
                response = response.decode('utf-8')
                self.logger.debug('Response from %s length %s', url, len(response))
                if parse_json:
                    return json.loads(response)
                else:
                    return response

            except (ValueError, HTTPError) as some_error:
                # If it is not 3xx, reraise the error
                if isinstance(some_error, HTTPError) and not (300 <= some_error.code <= 399):
                    raise some_error

                wait_time = retries ** 3
                self.logger.warning('Most likely SSO cookie is expired, will remake it after %s seconds',
                                    wait_time)
                time.sleep(wait_time)
                self.__generate_cookie()
                self.__connect()

        self.logger.error('Error while making a request to %s. Response: %s',
                          url,
                          response)
        return None

    def get_workflow(self, workflow_name):
        url = '/api/get_json/%s' % (workflow_name)
        return self.__http_request(url)

    def get_prepid(self, prepid):
        url = '/api/fetch?prepid=%s' % (prepid)
        return self.__http_request(url)

    def get_input_dataset(self, input_dataset):
        url = '/api/fetch?input_dataset=%s' % (input_dataset)
        return self.__http_request(url)

    def get_output_dataset(self, output_dataset):
        url = '/api/fetch?output_dataset=%s' % (output_dataset)
        return self.__http_request(url)
    
    def get_request(self, prepid):
        url = '/api/fetch?request=%s' % (prepid)
        return self.__http_request(url)
