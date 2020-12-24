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
import csv
import logging
import re
import time
import os
from datetime import datetime
from api_secrets import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET

import tweepy
import unicodecsv as csv

OUTPUT_FILE_PATH = 'output/'


class OauthKeys(object):
    def __init__(self):
        self.matrix = {}
        self.app_keys = []
        self.user_keys = []
        self.dict_rate_limit = {}

    def get_access(self):
        try:
            auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
            auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
            return tweepy.API(auth)
        except:
            print 'Error in oauth authentication.'
            exit(83)

    def get_rate_limits(self, api, type_resource, method, wait):
        try:
            result = api.rate_limit_status(resources=type_resource)
            resources = result['resources']
            resource = resources[type_resource]
            rate_limit = resource[method]
            limit = int(rate_limit['limit'])
            remaining_hits = int(rate_limit['remaining'])
            self.dict_rate_limit[(type_resource, method)] = remaining_hits
            while remaining_hits < 1:
                print 'waiting for 5 minutes ->' + str(datetime.now())
                time.sleep(wait / 3)
                result = api.rate_limit_status(resources=type_resource)
                resources = result['resources']
                resource = resources[type_resource]
                rate_limit = resource[method]
                limit = int(rate_limit['limit'])
                remaining_hits = int(rate_limit['remaining'])
                self.dict_rate_limit[(type_resource, method)] = remaining_hits
                print 'remaining hits', remaining_hits
        except:
            print 'exception checking rate limit, waiting for 15 minutes ->' + str(datetime.now())
            print 'If same error occurred after 15 minutes, please abort the command and check the app-user access keys'
            time.sleep(wait)
        return

    def check_rate_limits(self, api, type_resource, method, wait):
        if (type_resource, method) not in self.dict_rate_limit:
            self.get_rate_limits(api, type_resource, method, wait)
        else:
            self.dict_rate_limit[(type_resource, method)] -= 1
            print 'remaining hits', self.dict_rate_limit[(type_resource, method)]
            if self.dict_rate_limit[(type_resource, method)] < 1:
                self.get_rate_limits(api, type_resource, method, wait)
        return


def tweet_search(user_keys, api, full_path_file_out, query, format):
    head = False
    try:
        f = codecs.open(full_path_file_out, 'ru', encoding='utf-8', errors='ignore')
    except:
        head = True
    f = codecs.open(full_path_file_out, 'a', encoding='utf-8', errors='ignore')
    print 'results in %s\n' % full_path_file_out

    f_log = open(full_path_file_out + '.log', 'a')
    f_log.write('%s\t' % datetime.now())
    n_tweets = 0
    recent_tweet = 0
    first_tweet = True
    f_log.write('First time\t\t')

    if format == 'text':
        print 'generate file txt'
    if format == 'csv':
        print 'generate file csv'
        writer = csv.writer(f, delimiter=';')

    if head:
        if format == 'text':
            f.write('id tweet\tdate\tauthor\ttext\tapp\tid user\tfollowers\tfollowing\tstatuses\tlocation\turls\t'
                    'geolocation\tname\tdescription\turl_media\ttype media\tquoted\trelation\treplied_id\t'
                    'user replied\tretweeted_id\tuser retweeted\tquoted_id\tuser quoted\tfirst HT\tlang\tcreated_at\t'
                    'verified\tavatar\tlink\trts\tfavs\n')

        if format == 'csv':
            title = ['id tweet', 'date', 'author', 'text', 'app', 'id user', 'followers', 'following', 'statuses',
                     'location', 'urls', 'geolocation', 'name', 'description', 'url_media', 'type media', 'quoted',
                     'relation', 'replied_id', 'user replied', 'retweeted_id', 'user retweeted', 'quoted_id',
                     'user quoted', 'first HT', 'lang', 'created at', 'verified', 'avatar', 'link', 'rts', 'favs']
            writer.writerow(title)

    while True:
        page = []
        error = False

        try:
            OauthKeys.check_rate_limits(user_keys, api, 'search', '/search/tweets', 900)
            error = False
            if first_tweet:
                # print 'since_id', recent_tweet
                page = api.search(query, since_id=recent_tweet, include_entities=True, result_type='recent', count=100,
                                  tweet_mode='extended')
                first_tweet = False
            else:
                # print 'max_id', recent_tweet-1
                page = api.search(query, max_id=recent_tweet - 1, include_entities=True, result_type='recent',
                                  count=100, tweet_mode='extended')
        except KeyboardInterrupt:
            print '\nGoodbye!'
            exit(0)
        except tweepy.TweepError as e:
            text_error = '---------------->Tweepy error tweet at %s %s\n' % (time.asctime(), e)
            f_log.write(text_error)
            error = True
        if len(page) == 0:
            break

        if not error:
            print 'collected', n_tweets
            for statuses in page:
                recent_tweet = statuses.id
                statuse_quoted_text = None
                geoloc = None
                url_expanded = None
                url_media = None
                type_media = None
                location = None
                description = None
                name = None
                relation = None
                quoted_id = None
                replied_id = None
                retweeted_id = None
                user_replied = None
                user_quoted = None
                user_retweeted = None
                first_HT = None
                rt_count = 0
                fav_count = 0

                # get interactions Ids
                try:
                    id_tweet = statuses.id_str
                    if statuses.in_reply_to_status_id_str is not None:
                        relation = 'reply'
                        replied_id = statuses.in_reply_to_status_id_str
                        user_replied = '@' + statuses.in_reply_to_screen_name
                    if hasattr(statuses, 'quoted_status'):
                        relation = 'quote'
                        quoted_id = statuses.quoted_status_id_str
                        user_quoted = '@' + statuses.quoted_status['user']['screen_name']
                    elif hasattr(statuses, 'retweeted_status'):
                        relation = 'RT'
                        retweeted_id = statuses.retweeted_status.id_str
                        user_retweeted = '@' + statuses.retweeted_status.user.screen_name
                        if hasattr(statuses.retweeted_status, 'quoted_status'):
                            quoted_id = statuses.retweeted_status.quoted_status['id_str']
                            user_quoted = '@' + statuses.retweeted_status.quoted_status['user']['screen_name']
                except:
                    text_error = '---------------->Warning (tweet not discarded): bad interactions ids, id tweet %s at %s \n' % (
                        id_tweet, time.asctime())
                    f_log.write(text_error)

                # get quote
                if hasattr(statuses, 'quoted_status'):
                    try:
                        statuse_quoted_text = statuses.quoted_status['full_text']
                        statuse_quoted_text = re.sub('[\r\n\t]+', ' ', statuse_quoted_text)
                    except:
                        text_error = '---------------->Warning (tweet not discarded): bad quoted, id tweet %s at %s\n' % (
                            id_tweet, time.asctime())
                        f_log.write(text_error)
                elif hasattr(statuses, 'retweeted_status'):
                    try:
                        if hasattr(statuses.retweeted_status, 'quoted_status'):
                            statuse_quoted_text = statuses.retweeted_status.quoted_status['full_text']
                            statuse_quoted_text = re.sub('[\r\n\t]+', ' ', statuse_quoted_text)
                    except:
                        text_error = '---------------->Warning (tweet not discarded): bad quoted into a RT, id tweet %s at %s\n' % (
                            id_tweet, time.asctime())
                        f_log.write(text_error)

                # get geolocation
                if hasattr(statuses, 'coordinates'):
                    coordinates = statuses.coordinates
                    if coordinates != None:
                        try:
                            if 'coordinates' in coordinates:
                                list_geoloc = coordinates['coordinates']
                                print list_geoloc
                                geoloc = '%s, %s' % (list_geoloc[0], list_geoloc[1])
                        except:
                            text_error = '---------------->Warning (tweet not discarded): bad coordinates, id tweet %s at %s\n' % (
                                id_tweet, time.asctime())
                            f_log.write(text_error)

                # get entities
                entities = None
                if hasattr(statuses, 'entities'):
                    entities = statuses.entities
                if hasattr(statuses, 'retweeted_status'):
                    if hasattr(statuses.retweeted_status, 'entities'):
                        entities = statuses.retweeted_status.entities
                if entities is not None:
                    try:
                        urls = entities['urls']
                        if len(urls) > 0:
                            url_expanded = urls[0]['expanded_url']
                    except:
                        text_error = '---------------->Warning (tweet not discarded):  bad entity urls, id tweet %s at %s\n' % (
                            id_tweet, time.asctime())
                        f_log.write(text_error)
                    try:
                        if 'media' in entities:
                            list_media = entities['media']
                            if len(list_media) > 0:
                                url_media = list_media[0]['media_url']
                                type_media = list_media[0]['type']
                    except:
                        text_error = '---------------->Warning (tweet not discarded): bad entity Media, id tweet %s at %s\n' % (
                            id_tweet, time.asctime())
                        f_log.write(text_error)
                    try:
                        if 'hashtags' in entities:
                            HTs = entities['hashtags']
                            if len(HTs) > 0:
                                first_HT = HTs[0]['text']
                    except:
                        text_error = '---------------->Warning (tweet not discarded): bad entity HT, id tweet %s at %s\n' % (
                            id_tweet, time.asctime())
                        f_log.write(text_error)

                # get text
                if hasattr(statuses, 'full_text'):
                    try:
                        text = re.sub('[\r\n\t]+', ' ', statuses.full_text)
                    except:
                        text_error = '---------------->Warning (tweet not discarded): bad tweet text,  at %s id tweet %s \n' % (
                            time.asctime(), id_tweet)
                        f_log.write(text_error)

                if hasattr(statuses, 'retweeted_status'):
                    if hasattr(statuses.retweeted_status, 'full_text'):
                        try:
                            RT_expand = re.sub('[\r\n\t]+', ' ', statuses.retweeted_status.full_text)
                            RT = re.match(r'(^RT @\w+: )', text)
                            if RT:
                                text = RT.group(1) + RT_expand
                        except:
                            text_error = '---------------->Warning (tweet not discarded): bad tweet text into a RT,  at %s id tweet %s \n' % (
                                time.asctime(), id_tweet)
                            f_log.write(text_error)
                try:
                    location = re.sub('[\r\n\t]+', ' ', statuses.user.location, re.UNICODE)
                except:
                    pass
                try:
                    description = re.sub('[\r\n\t]+', ' ', statuses.user.description, re.UNICODE)
                except:
                    pass
                try:
                    name = re.sub('[\r\n\t]+', ' ', statuses.user.name, re.UNICODE)
                except:
                    pass

                # RTs and FAVs
                if hasattr(statuses, 'retweet_count'):
                    rt_count = int(statuses.retweet_count)
                if hasattr(statuses, 'favorite_count'):
                    fav_count = int(statuses.favorite_count)

                try:
                    link_tweet = 'https://twitter.com/%s/status/%s' % (statuses.user.screen_name, statuses.id)

                    if format == 'text':
                        tweet = '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
                                '\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (
                                    statuses.id,
                                    statuses.created_at,
                                    '@' + statuses.user.screen_name,
                                    text,
                                    statuses.source,
                                    statuses.user.id,
                                    statuses.user.followers_count,
                                    statuses.user.friends_count,
                                    statuses.user.statuses_count,
                                    location,
                                    url_expanded,
                                    geoloc,
                                    name,
                                    description,
                                    url_media,
                                    type_media,
                                    statuse_quoted_text,
                                    relation,
                                    replied_id,
                                    user_replied,
                                    retweeted_id,
                                    user_retweeted,
                                    quoted_id,
                                    user_quoted,
                                    first_HT,
                                    statuses.lang,
                                    statuses.user.created_at,
                                    statuses.user.verified,
                                    statuses.user.profile_image_url_https,
                                    link_tweet,
                                    rt_count,
                                    fav_count)

                        f.write(tweet)
                    if format == 'csv':
                        row = [statuses.id, statuses.created_at, '@' + statuses.user.screen_name, text, statuses.source,
                               statuses.user.id, statuses.user.followers_count, statuses.user.friends_count,
                               statuses.user.statuses_count, location, url_expanded, geoloc, name, description,
                               url_media, type_media, statuse_quoted_text, relation, replied_id, user_replied,
                               retweeted_id, user_retweeted, quoted_id, user_quoted, first_HT, statuses.lang,
                               statuses.user.created_at, statuses.user.verified, statuses.user.profile_image_url_https,
                               link_tweet, rt_count, fav_count]

                        writer.writerow(row)
                    n_tweets = n_tweets + 1
                except:
                    text_error = '---------------->bad format,  at %s id tweet %s \n' % (time.asctime(), id_tweet)
                    f_log.write(text_error)

    # write log file
    f_log.write('wrote %s tweets\t' % n_tweets)
    f_log.write('recent tweet Id %s \n' % recent_tweet)
    f_log.close()


def main():
    parser = argparse.ArgumentParser(description='Examples usage Twitter API REST, search method')
    parser.add_argument('--query', help='query')
    parser.add_argument('--file_out', default='tweet_store.txt', help='name file out')
    parser.add_argument('--format', default='text', help='name file out')

    args = parser.parse_args()
    query = args.query
    file_out = args.file_out
    format = args.format

    file_name = re.search(r"[\.]*[\w/-]+\.[\w]*", file_out)
    if not file_name:
        print "bad filename", file_out
        exit(1)

    script_dir = os.path.dirname(__file__)
    full_path_file_out = os.path.join(script_dir, OUTPUT_FILE_PATH + file_out)

    user_keys = OauthKeys()
    api = OauthKeys.get_access(user_keys)

    tweet_search(user_keys, api, full_path_file_out, query, format)
    exit(0)


if __name__ == '__main__':
    try:
        logging.basicConfig()
        main()
    except KeyboardInterrupt:
        print '\nGoodbye!'
        exit(0)
