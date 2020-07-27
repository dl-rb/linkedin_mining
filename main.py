import json
import re
from pyspark import SparkContext
import LinkedInScapper2 as lis
import time


def analyze(json_fname):
    def readLine(line):
        data = json.loads(line)
        description = data['description'].lower()
        word_list = re.sub(r'\W', " ", description).split(' ')
        l = set()
        # get 2-grams
        for i in range(0, len(word_list) - 1):
            if len(word_list[i]) > 2 and len(word_list[i + 1]) > 2:
                l.add(word_list[i] + ' ' + word_list[i + 1])

        for w in word_list:
            if len(w) > 2:
                l.add(w)

        # l.extend(word_list)
        return l

    sc = SparkContext('local[*]', 'LinkedIn')
    sc.setLogLevel('ERROR')
    rdd = sc.textFile(json_fname)
    d = rdd.flatMap(readLine).map(lambda term: (term, 1)).reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False).collectAsMap()

    sc.stop()


def find_job_with_kw(json_fname, kw):
    '''
    find jobs that contain kw
    :param kw: list of keywords
    :return: list of job links
    '''

    def get_descp(line):
        data = json.loads(line)
        return data['description'].lower(), data['job link']

    def match_kw(x):
        desc = x[0]
        word_set = set(re.sub(r'[^a-z0-9_+\-]', ' ', desc).split(' '))
        r = []
        for w in kw:
            if w not in word_set:
                return []
        r.append(x[1])
        return r

    kw = [i.lower() for i in kw]

    sc = SparkContext()
    sc.setLogLevel('ERROR')
    rdd = sc.textFile(json_fname)
    d = rdd.map(get_descp).reduceByKey(lambda v1, v2: v1).flatMap(match_kw).collect()
    sc.stop()
    return d


def scrape_linkedIn(kw):
    scapper = lis.LinkedInScrapper2()
    # kw = 'robotics software engineer'
    n_link = 100

    print('Getting {} links...'.format(n_link))
    t = time.time()
    links = scapper.getJobLinksAsync(n_link, kw, job_type='F,C', day_posted_ago=20)
    print(time.time() - t)

    print('Getting job info...')
    t = time.time()
    fname = scapper.getJobInfosAsync(kw, links)
    print(fname)
    print(time.time() - t)

    return fname


def main():
    json_fname = scrape_linkedIn('robotics software engineer')
    # json_fname = 'data/software engineer_20200713-122413.json'
    d = find_job_with_kw(json_fname, ['C++', 'robotics'])
    for i in d:
        print(i)
    print(len(d))


if __name__ == '__main__':
    main()
    # test()
