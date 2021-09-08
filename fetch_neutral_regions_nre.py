#!/usr/bin/env python3

# import webdriver
# from selenium import webdriver
# import chromedriver_binary
  
# # create webdriver object
# driver = webdriver.Chrome()
  
# # get geeksforgeeks.org
# driver.get("http://nre.cb.bscb.cornell.edu/nre/run.html")

from selenium import webdriver
import chromedriver_binary

options = webdriver.ChromeOptions()
options.add_argument("--headless")

driver = webdriver.Chrome(options=options)

#driver.get('http://www.google.com')
#print(driver.title)

driver.get('http://nre.cb.bscb.cornell.edu/nre/run.html')


  
# get element 
#element = driver.find_element_by_id("gsc-i-id2")
  
# send keys 
#element.send_keys("Arrays")
  
# submit contents
#element.submit()
