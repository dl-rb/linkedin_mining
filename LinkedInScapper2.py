import datetime
import json
import math
import os
import time
import urllib.parse
import urllib.request
import urllib.error
from bs4 import BeautifulSoup
import warnings
import re
import multiprocessing


class LinkedInScrapper2:
    def __init__(self):
        self.__linkedIn_url = 'https://www.linkedin.com/jobs/search/?'
        self.__job_links = set()
        self.__job_infos = []
        self.__job_info_c = 0
        self.__job_links_writer = None
        self.__job_info_writer = None

    @staticmethod
    def __createParamsDict(keywords, page=0, location='United States', job_type=None, day_posted_ago=0):
        params = {'keywords': keywords,
                  'location': location,
                  'start': page}

        if day_posted_ago > 0:
            params['f_TPR'] = 'r' + str(day_posted_ago * 24 * 60 * 60)

        if job_type is not None:
            params['f_JT'] = job_type  # F for full time, C for contract
        return params

    @staticmethod
    def requestURL(url):
        '''
        get response from url request
        :param url: destination url
        :return: None if cannot get response due to any HTTPError, otherwise return response
        '''

        try:
            res = urllib.request.urlopen(url, timeout=5)
            return res
        except urllib.error.HTTPError as e:
            warnings.warn('\ngetRes error {}'.format(e.reason))
            return None

    @staticmethod
    def requestURLWithRetry(based_url, params=None, n_retry=None):
        '''
        get url response with n times retry before giving up
        :param based_url:
        :param params:
        :param n_retry: number of retries
        :return: response
        '''

        url = based_url
        if params is not None:
            url += urllib.parse.urlencode(params, quote_via=urllib.parse.quote)

        res = LinkedInScrapper2.requestURL(based_url)
        if res is not None or n_retry is None:
            return res

        c = 0
        sleep_time = 5
        while True:
            warnings.warn('\nRetry {}, wait for {} secs\n{}'.format(c + 1, sleep_time, url))
            time.sleep(sleep_time)
            res = LinkedInScrapper2.requestURL(url)
            sleep_time += 2
            c += 1
            if c >= n_retry or res is not None:
                break
        return res

    @staticmethod
    def parseContent(response):
        return BeautifulSoup(response.read(), 'html.parser')

    @staticmethod
    def getJobLinksInPage(linkedIn_url, params):
        '''
        by default each of request via LinkedIn search page will return a page with list of 25 jobs. This func will
        extract all job links from returned page
        :param linkedIn_url: LinkedIn search Url
        :param params: searching parameters
        :return: list of job urls
        '''

        url = linkedIn_url + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)

        res = LinkedInScrapper2.requestURLWithRetry(url, n_retry=5)
        links = set()
        if res is None:
            return links

        soup = LinkedInScrapper2.parseContent(res)
        tags = soup.find_all('a', {'class': 'result-card__full-card-link'})

        for t in tags:
            job_url = t.attrs['href']
            links.add(job_url)
        return links

    def __getJobLinksCallBack(self, return_val):
        '''
        callback func for multiprocessing Pool async function
        :param return_val:
        :return:
        '''

        for l in return_val:
            self.__job_info_writer.write(l + '\n')
        self.__job_links.update(return_val)
        print('\r Got {} links'.format(len(self.__job_links)), end='')

    def getJobLinksAsync(self, n_job, keywords, location='United States', job_type=None, day_posted_ago=0,
                         write2file=True, fname=None, save_dir='data'):
        '''
        get number of job links by scrapping LinkedIn search page
        :param n_job:
        :param keywords: searching keywords
        :param location: job location
        :param job_type: full time, part time, contract ... Default is all type
        :param day_posted_ago:
        :param write2file: if true, result will be written to file
        :param fname: filename to write result to if 'write2file' is set
        :return: list of job links
        '''

        self.__job_links.clear()
        pool = multiprocessing.Pool()
        if write2file and fname is None:
            current_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            fname = os.path.join(save_dir, keywords + '_' + current_time + '.txt')
            if not os.path.exists(save_dir):
                os.mkdir(save_dir)

        self.__job_info_writer = open(fname, 'w')

        n_page = math.ceil(n_job / 25)

        for page in range(n_page):
            pool.apply_async(self.getJobLinksInPage,
                             (self.__linkedIn_url, self.__createParamsDict(keywords,
                                                                           page,
                                                                           location,
                                                                           job_type,
                                                                           day_posted_ago)),
                             callback=self.__getJobLinksCallBack)
        pool.close()
        pool.join()
        self.__job_info_writer.close()
        print('\nTotal links scrapped {}'.format(len(self.__job_links)))
        if write2file:
            print(fname)
        return self.__job_links

    def getJobLinks(self, n_job, keywords, location='United States', job_type=None, day_posted_ago=0, write2file=True,
                    fname=None, save_dir='data'):

        self.__job_links.clear()
        if write2file and fname is None:
            current_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            fname = os.path.join(save_dir, keywords + '_' + current_time + '.txt')
            if not os.path.exists(save_dir):
                os.mkdir(save_dir)

        self.__job_info_writer = open(fname, 'w')

        n_page = math.ceil(n_job / 25)
        for page in range(n_page):
            result = self.getJobLinksInPage(self.__linkedIn_url, self.__createParamsDict(keywords,
                                                                                         page,
                                                                                         location,
                                                                                         job_type,
                                                                                         day_posted_ago))
            for l in result:
                self.__job_info_writer.write(l + '\n')
            self.__job_links.update(result)
            print('\r Got {} links'.format(len(self.__job_links)), end='')

        self.__job_info_writer.close()

        print('\nTotal links scrapped {}'.format(len(self.__job_links)))
        if write2file:
            print(fname)
        return self.__job_links

    @staticmethod
    def getPostDate(soup, current_date):
        '''
        get date job was posted based on LinkedIn date posted: 2 days ago, or 1 month ago...
        :param soup:
        :param current_date: today date
        :return: date job was posted
        '''

        result = soup.find('span', {'class': re.compile(r'posted-time-ago')}, text=True)
        if result is None:
            return ''
        text = result.text
        w = 0
        d = 0
        h = 0
        i = int(text.split(' ')[0])
        if 'month' in text:
            d = 30 * i
        if 'week' in text:
            w = i
        if 'day' in text:
            d = i
        if 'hour' in text:
            h = i
        date = str((current_date - datetime.timedelta(weeks=w, days=d, hours=h)).date())
        return date

    @staticmethod
    def getJobInfo(job_url, current_date):
        '''
        From job url, get all info about job: company, link, description...
        :param job_url:
        :param current_date:
        :return: dict: {'job link': ,'company name': , 'company link':, 'description': , 'posted date': }
        '''

        res = LinkedInScrapper2.requestURLWithRetry(job_url, n_retry=5)
        if res is None:
            return None

        soup = LinkedInScrapper2.parseContent(res)
        info = {'job link': job_url}

        company_tag = soup.find('a', {'class': re.compile(r'org-name-link')})
        if company_tag is not None:
            info['company name'] = company_tag.get_text()
            linkedin_link = company_tag.attrs['href']
            info['company link'] = linkedin_link[:linkedin_link.find('?')]

        tags = soup.find_all('div', {'class': 'description__text description__text--rich'})
        info['description'] = ''
        for t in tags:
            info['description'] += ' '.join(t.find_all(text=True))

        info['posted date'] = LinkedInScrapper2.getPostDate(soup, current_date)
        return info

    def __getJobInfosCallBack(self, result):
        if result is not None:
            json.dump(result, self.__job_info_writer)
            self.__job_info_writer.write('\n')
            self.__job_info_c += 1
            print('\r Got {} infos'.format(self.__job_info_c), end='')

    def getJobInfosAsync(self, keywords, job_links, write2file=True, fname=None, save_dir='data'):
        '''
        from list of job links, scrape job page and extract all job infos
        :param keywords:
        :param job_links:
        :param write2file:
        :param fname:
        :return:
        '''
        current_date = datetime.datetime.now()
        self.__job_infos.clear()
        self.__job_info_c = 0

        if write2file and fname is None:
            current_time = current_date.strftime("%Y%m%d-%H%M%S")
            if not os.path.exists(save_dir):
                os.mkdir(save_dir)
            fname = os.path.join(save_dir, keywords + '_' + current_time + '.json')
        self.__job_info_writer = open(fname, 'w')

        pool = multiprocessing.Pool()
        for link in job_links:
            pool.apply_async(self.getJobInfo, (link, current_date), callback=self.__getJobInfosCallBack)

        pool.close()
        pool.join()
        self.__job_info_writer.close()
        print('\nDone')
        return fname

    def getJobInfos(self, keywords, job_links, write2file=True, fname=None, save_dir='data'):
        current_date = datetime.datetime.now()

        if write2file and fname is None:
            current_time = current_date.strftime("%Y%m%d-%H%M%S")
            if not os.path.exists(save_dir):
                os.mkdir(save_dir)
            fname = os.path.join(save_dir, keywords + '_' + current_time + '.json')
        self.__job_info_writer = open(fname, 'w')

        for link in job_links:
            result = LinkedInScrapper2.getJobInfo(link, current_date)

            self.__job_infos.append(result)
            json.dump(result, self.__job_info_writer)
            self.__job_info_writer.write('\n')

        self.__job_info_writer.close()
        return fname
