import csv
import string   
import urllib2 
from HTMLParser import HTMLParser  
import mechanize 
import cookielib

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
import time



class MyHTMLParser(HTMLParser):

  def __init__(self):
    HTMLParser.__init__(self)
    self.recording = 0 
    self.data = []

  def handle_starttag(self, tag, attrs):
    if tag == 'div':
      print("pring ==========================================================================")
      print(attrs)
      print("ending ==========================================================================")
      for name, value in attrs:
        #if name == 'somename' and value == 'somevale':
        if name == 'id' and value == 'your-score':
          print(name, value)
          print ("Encountered the beginning of a %s tag" % tag) 
          self.recording = 1 


  def handle_endtag(self, tag):
    if tag == 'div':
      self.recording -=1 
      print ("Encountered the end of a %s tag" % tag )

  def handle_data(self, data):
    if self.recording:
      self.data.append(data)

 #p = MyHTMLParser()
 #f = urllib2.urlopen('http://www.someurl.com')
 #html = f.read()
 #p.feed(html)
 #print p.data
 #p.close()

      
addressReader = csv.reader(open("E:\\webSpider\\webs.csv", "rt"), delimiter=",")
address_l=[] 
for row in addressReader:
   colnum=0
   for  col in row:
      col1=col.rstrip(" ")
      col1=col1.replace(" ","-")
      if colnum==1 :
        address_l.append("http://www.walkscore.com/score/"+col1)
      print(col1)
      colnum=colnum+1 
print(address_l[0])
p = MyHTMLParser()
f = urllib2.urlopen(address_l[1])
html = f.read()
print(html)
#p.feed(html)
#print p.data

