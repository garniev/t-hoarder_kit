#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2015 Mariluz Congosto
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

import argparse
import codecs
import datetime
import re
import sys
import time
import unicodedata


def strip_accents(s):
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))


# # A dinamic matrix
# # This matrix is a dict whit only cells it nedeed
# # Column and row numbers start with 1
#  
class Matrix(object):
    def __init__(self, rows, cols):
        self.matrix = {}
        self.cols = int(cols)
        self.rows = int(rows)

    def setitem(self, row, col, v):
        self.matrix[row - 1, col - 1] = v
        return

    def getitem(self, row, col):
        if (row - 1, col - 1) not in self.matrix:
            return 0
        else:
            return self.matrix[row - 1, col - 1]

    def __iter__(self):
        for row in range(self.rows):
            for col in range(self.cols):
                yield (self.matrix, row, col)

    def __repr__(self):
        outStr = ""
        for i in range(self.rows):
            for j in range(self.cols):
                if (i, j) not in self.matrix:
                    outStr += '0.00,'
                else:
                    outStr += '%.2f,' % (self.matrix[i, j])
            outStr += '\n'
        return outStr


class Rank(object):
    def __init__(self):
        self.rank = {}

    def set_item(self, item, value):
        if item not in self.rank:
            self.rank[item] = value
        else:
            self.rank[item] = self.rank[item] + value
        return

    def get_item(self, item):
        if item in self.rank:
            return self.rank[item]
        else:
            return 0


def get_communities(file_communities, col_user, col_community):
    dict_user_community = {}
    try:
        f_communities = codecs.open(file_communities, 'rU', encoding='utf-8')
    except:
        print 'Can not open file', file_communities
        exit(1)
    head = True
    for line in f_communities:
        if head:
            head = False
        else:
            line = line.strip("\n\r")
            data = line.split(",")
            user = data[col_user].lower()
            community = int(data[col_community])
            dict_user_community[user] = community
    return dict_user_community


def get_number(item):
    number = 0
    match = (re.search(r"\d+", item))
    if match:
        number = int(match.group(0))
    return number


def get_tweet(tweet):
    data = tweet.split('\t')
    try:
        id_tweet = data[0]
        timestamp = data[1]
        # print timestamp
        date_hour = re.findall(r'(\d\d\d\d)-(\d\d)-(\d\d)\s(\d\d):(\d\d):(\d\d)', timestamp, re.U)
        # date_hour =re.findall(r'(\d+)/(\d+)/(\d+)\s(\d+):(\d+)',timestamp,re.U)
        (year, month, day, hour, minutes, seconds) = date_hour[0]
        # (day,month,year,hour,minutes) = date_hour[0]
        seconds = 0
        author = data[2]
        text = data[3]
        app = data[4]
        user_id = data[6]
        followers = get_number(data[6])
        following = get_number(data[7])
        statuses = get_number(data[8])
        loc = data[9]
        return (
        year, month, day, hour, minutes, seconds, author, text, app, user_id, followers, following, statuses, loc)
    except:
        print ' tweet not match', tweet
        return None


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# main
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def main():
    # intit data

    first_tweet = True
    tweets = 0
    num_tweets = 0
    dir_in = ''
    dir_out = ''
    head = True

    reload(sys)
    sys.setdefaultencoding('utf-8')
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
    start = datetime.datetime.fromtimestamp(time.time())

    parser = argparse.ArgumentParser(description='This script extracts the most spread tweets in global and by day ')
    parser.add_argument('file_in', type=str, help='file with raw tweets')
    parser.add_argument('file_communities', type=str, help='file users communities')
    parser.add_argument('col_user', type=str, help='col_user commnunity')
    parser.add_argument('col_community', type=str, help='col commnunity')
    parser.add_argument('--dir_in', type=str, default='./', help='Dir data input')
    parser.add_argument('--dir_out', type=str, default='./', help='Dir data output')
    args = parser.parse_args()

    file_in = args.file_in
    file_communities = args.file_communities
    col_user = int(args.col_user) - 1
    col_community = int(args.col_community) - 1
    dir_in = args.dir_in
    dir_out = args.dir_out

    filename = re.search(r"([\w-]+)\.([\w\.]*)", file_in)
    print 'file name', file_in
    if not filename:
        print "bad filename", file_in, ' Must be an extension'
        exit(1)
    name = filename.group(0)
    prefix = filename.group(1)
    extension = filename.group(2)
    print extension
    try:
        f_in = codecs.open(dir_in + file_in, 'rU', encoding='utf-8')
        print 'open as unicode'
    except:
        print 'Can not open file', dir_in + file_in
        exit(1)
    f_out = codecs.open(dir_out + prefix + '_with_communities.txt', 'w', encoding='utf-8')
    dict_user_community = get_communities(file_communities, col_user, col_community)
    for line in f_in:
        num_tweets += 1
        if num_tweets % 10000 == 0:
            print num_tweets
        line = line.strip('\n\r')
        if head:
            line = '%s\tcommunity\n' % line
            f_out.write(line)
            head = False
        else:
            tweet_flat = get_tweet(line)
            if tweet_flat == None:
                print 'not match ', line
            else:
                (year, month, day, hour, minutes, seconds, author, text, app, user_id, followers, following, statuses,
                 loc) = tweet_flat
                match = re.match(r'^(\d+)\t', line, re.U)
                if match:
                    id_tweet = match.group(1)
                else:
                    id_tweet = 0
                author = author.lower()
                if author in dict_user_community:
                    community = dict_user_community[author]
                else:
                    community = None
                line = '%s\t%s\n' % (line, community)
                f_out.write(line)
    exit(0)


if __name__ == '__main__':
    main()
