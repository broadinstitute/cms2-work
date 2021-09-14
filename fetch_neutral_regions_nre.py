#!/usr/bin/env python3

"""Command-line interface to the Neutral Regions Explorer webserver.


"""

# import webdriver
# from selenium import webdriver
# import chromedriver_binary
  
# # create webdriver object
# driver = webdriver.Chrome()
  
# # get geeksforgeeks.org
# driver.get("http://nre.cb.bscb.cornell.edu/nre/run.html")

import platform

if not tuple(map(int, platform.python_version_tuple())) >= (3,8):
    raise RuntimeError('Python >=3.8 required')

import argparse
import logging
import time

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_binary

_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--nre-url', default='http://nre.cb.bscb.cornell.edu/nre/run.html')
    parser.add_argument('--nre-timeout-seconds', type=float, default=7200)
    parser.add_argument('--nre-poll-frequency-seconds', type=float, default=30)

    return parser.parse_args()

def submit_neutral_region_explorer_job(args):

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")

    driver = webdriver.Chrome(options=options)

    #driver.get('http://www.google.com')
    #print(driver.title)

    driver.get(args.nre_url)

    def find_submit_button():
        for e in driver.find_elements_by_tag_name('input'):
            print(e)
            #print(dir(e))
            if e.get_attribute('type') == 'submit':
                return e

    current_url = driver.current_url

    find_submit_button().click()
    #time.sleep(2)
    #driver.refresh()
    # some work on current page, code omitted

    # save current page url

    _log.debug(f'waiting for {current_url=} to change')

    # initiate page transition, e.g.:
    #input_element.send_keys(post_number)
    #input_element.submit()

    # wait for URL to change with 15 seconds timeout
    WebDriverWait(driver, timeout=args.nre_timeout_seconds, poll_frequency=args.nre_poll_frequency_seconds).\
        until(EC.url_changes(current_url))

    # print new URL
    new_url = driver.current_url
    _log.info(f'{new_url=}')
# end: def submit_neutral_region_explorer_job(nre_url)



  
# get element 
#element = driver.find_element_by_id("gsc-i-id2")
  
# send keys 
#element.send_keys("Arrays")
  
# submit contents
#element.submit()

if __name__ == '__main__':
    submit_neutral_region_explorer_job(parse_args())
