# --*-- coding:utf-8 --*--
"""




"""
from __future__ import absolute_import
import sys
import re
import random
import json
from collections import OrderedDict
sys.path.insert(0,'/Users/moonmoonbird/Documents/ms/crawl')
import scrapy
from mydb.db import DB


class ZodiacSpider(scrapy.Spider):
    name = "zodiac"
    index = 0
    even_index = 1
    def __init__(self):
        super(ZodiacSpider, self).__init__()
        self.db = DB()
        self.astrology_table = {
            'table_name': 'astrology',
            'sql': """
                CREATE TABLE  `astrology` (
                id INT AUTO_INCREMENT COMMENT "自增主键",
                name  VARCHAR(255) NOT NULL COMMENT "星座名字",
                dateofbirthday VARCHAR(255) NOT NULL COMMENT "星座所在日期范围",
                start_date VARCHAR(255) NOT NULL COMMENT "星座开始日期",
                end_date VARCHAR(255) NOT NULL COMMENT "星座结束日期",
                strength VARCHAR(255) NOT NULL COMMENT "星座赋予的力量",
                weakness VARCHAR(255) NOT NULL COMMENT "星座的弱点",
                symbol VARCHAR(255) NOT NULL COMMENT "星座符号",
                element VARCHAR(255) NOT NULL COMMENT "星座元素",
                signruler VARCHAR(255) NOT NULL COMMENT "星座统治者",
                luckycolor VARCHAR(255) NOT NULL COMMENT "幸运颜色",
                luckynumber INT COMMENT "幸运数字",
                jewelry VARCHAR(255) NOT NULL COMMENT "星座对应的珠宝",
                bestmatch VARCHAR(255) NOT NULL COMMENT "最匹配的星座",
                celebrities VARCHAR(255) NOT NULL COMMENT "此星座的名人",
                characters TEXT DEFAULT NULL COMMENT "特性",
                personality TEXT DEFAULT NULL COMMENT "个性分析",
                hobbies TEXT DEFAULT NULL COMMENT "爱好",
                love TEXT DEFAULT NULL COMMENT "爱情分析",
                friendfamily TEXT DEFAULT NULL COMMENT "亲友分析",
                careermoney TEXT DEFAULT NULL COMMENT "财运事业",
                PRIMARY KEY (id)
                )
             """
        }
        self.match_table = {
            'table_name':'compatibility',
            'sql': """
                CREATE TABLE  `compatibility` (
                id INT AUTO_INCREMENT COMMENT "自增主键",
                issuer_sign VARCHAR(225) NOT NULL COMMENT "发起匹配星座",
                receive_sign VARCHAR(225) NOT NULL COMMENT "目标匹配星座",
                sexual_intimacy_pct INT DEFAULT 0 COMMENT "性关系,亲密关系匹配百分比",
                trust_pct INT DEFAULT 0 COMMENT "互相信任百分比",
                communication_intellect_pct INT DEFAULT 0 COMMENT "交流和知性匹配百分比",
                emotions_pct INT DEFAULT 0 COMMENT "情绪情感匹配百分比",
                values_pct INT DEFAULT 0 COMMENT "价值观匹配百分比",
                sharedactivities_per INT DEFAULT 0 COMMENT "共同进行活动匹配百分批",
                total_per INT DEFAULT 0 COMMENT "总的百分比",
                PRIMARY KEY (id)
                )
             """
        }
        self.sign = [
            'aries','taurus','gemini','cancer','leo','virgo',
            'libra','scorpio','sagittarius','capricorn','aquarius','pisces'
        ]
        self.date_table = {
            'january': 1,
            'february': 2,
            'march': 3,
            'april': 4,
            'may': 5,
            'june': 6,
            'july': 7,
            'august': 8,
            'september': 9,
            'october': 10,
            'november': 11,
            'december': 12

        }
        self.basic_table = {
            'love': 'love',
            'friendship': 'friendfamily',
            'career-money':'careermoney',
        }
        self.type = [
            'love',
            'friendship',
            'career-money'
        ]
        self.type_secondary = [
            'love',
            'friends',
            'career'
        ]
        self.type_secondary_table = {
            'love':'love',
            'friends':'friendfamily',
            'career': 'careermoney'
        }
        self.db.create_table(self.astrology_table.get('table_name'), self.astrology_table.get('sql'))
        self.db.create_table(self.match_table.get('table_name'), self.match_table.get('sql'))
        self.personality_url = 'https://www.horoscope.com/zodiac-signs/{sign}/{sign}-personality.html'
        self.personality_url_secondary = 'https://www.horoscope.com/us/profiles/zodiac/profile-zodiac-sign-{sign}.aspx'
        self.love_friendfamily_careermoney = 'https://www.horoscope.com/zodiac-signs/{sign}/{sign}-{type}.html'
        self.match_url = 'http://www.astrology-zodiac-signs.com/compatibility/{source_sign}-{target_sign}/'

    def odd(self):
        self.index += 1
        return not self.index%2

    def even(self):
        self.even_index += 1
        return self.even_index%2

    def start_requests(self):
        urls = [
            'https://www.travelchinaguide.com/intro/astrology/western-zodiac/',

        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        dealing with basic zodiac infomation
        :param response:
        :return:
        """
        #get all table/tr element
        cons = response.xpath('//table//tr')
        zodiac_name_dict = {}
        zodiac_basic_dict = {}

        #split up according to odd and even number
        for c in cons[1:]:
            if not self.odd():
                zodiac_name = c.xpath('td[1]//h2//text()').extract()
                zodiac_name_dict[self.index] = zodiac_name

            if self.even():
                # print( self.even_index)
                zodiac_basic = []
                for data in c.xpath('td[2]//text()').extract():
                    zodiac_basic.append(data.strip())
                zodiac_basic_dict[self.index] = zodiac_basic

        #make final data structure
        final_data = OrderedDict()
        for key, value in zodiac_name_dict.items():
            tmp_basic = {}
            basic = zodiac_basic_dict.get(key+1)
            for bs in basic:
                tmp = bs.split(':')
                k = re.sub(r'\s+','', tmp[0])
                v = tmp[1]
                tmp_basic.update({k.lower():v.lower()})
            final_data[value[0].lower()] = tmp_basic
        # print (zodiac_basic_dict)

        #generate sql and store into database
        for name, values in final_data.items():
            sql = 'INSERT INTO `astrology` (`name`, `dateofbirthday`, `start_date`, `end_date` ,`strength`, `weakness`, `symbol`, `element`,' \
                  ' `signruler`,`luckycolor`, `luckynumber`,`jewelry`,`bestmatch`,`celebrities`) ' \
                  'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

            self.db.insert(sql, (name,
                                 values.get('dateofbirth'),
                                 str(self.date_table.get((values.get('dateofbirth').split('-')[0].strip().split(' ')[0])))+'/'+values.get('dateofbirth').split('-')[0].strip().split(' ')[1],
                                 str(self.date_table.get((values.get('dateofbirth').split('-')[1].strip().split(' ')[0])))+'/'+values.get('dateofbirth').split('-')[1].strip().split(' ')[1],
                                 values.get('strength'),
                                 values.get('weakness'),
                                 values.get('symbol'),
                                 values.get('element'),
                                 values.get('signruler'),
                                 values.get('luckycolors') or values.get('luckycolor'),
                                 int(values.get('luckynumber')),
                                 values.get('jewelry'),
                                 values.get('bestmatch'),
                                 values.get('celebrities')))
        self.db.connection.commit()


        #characters
        yield response.follow('https://astrologybay.com/what-does-your-zodiac-sign-say-about-your-hobbies',
                                  callback=self.parse_character)
        # goon = True
    def parse_character(self, response):
        """
        dealing with zodiac characteristics
        :param response:
        :return:
        """
        cons = response.css('div.bz-basic-card')
        for c in cons[1:14]:
            title_d = c.css('div.bz-card-title span.bz-text::text').extract()
            title = ''.join(''.join(title_d[:1]).split(' ')[:1]).lower()
            if title in self.sign:
                character = c.css('div.bz-card-text div.bz-text')
                character_dict = character.xpath('string(.)').extract()
                sql = 'UPDATE `{table_name}` SET characters = "{text}" WHERE name = "{name}"'.format(table_name='astrology',
                                                                                                 text=character_dict[0].split(':')[1] ,
                                                                                                 name=title)
                # print (sql)
                self.db.update(sql)
        self.db.connection.commit()

        yield response.follow('http://trendyfeeds.com/12-zodiac-signs-and-their-distinct-hobbies/',
                                  callback=self.parse_hobbies)

    def parse_hobbies(self, response):
        """
        dealing with zodiac hobbies
        :param response:
        :return:
        """
        hobbies_parent = response.xpath('//div[@class="entry-content"]')
        # hobbies = response.xpath('//div[@class="entry-content"]/h3/following-sibling::p[3]/text()').extract()

        titles = hobbies_parent.xpath('//h3')

        for index,title in enumerate(titles[:-2]):
            sql = 'UPDATE `{table_name}` SET hobbies = "{text}" ' \
                  'WHERE name = "{name}"'.format(table_name='astrology',
                                                 text=title.xpath('//h3['+str(index+1)+']/following-sibling::p[3]/text()').extract()[0].split(':')[1].replace('-','').strip(),
                                                 name=title.xpath('text()').extract()[0].split(' ')[1].lower().strip())

            self.db.update(sql)
        self.db.connection.commit()

        for sign in self.sign:
            url = "https://en.wikipedia.org/wiki/{sign}_(constellation)".format(sign=sign)
            yield response.follow(url,callback=self.parse_mathology)

    def parse_mathology(self, response):
        """
        dealing with zodiac mathology
        :return:
        """
        # mythologies = response.xpath("//span[@id='History_and_mythology']//").extract()
        # if not mythologies:
        #     mythologies = response.xpath("//span[@id='Mythology']/p").extract()
        # print (mythologies, '--------', response.url)

        for sign in self.sign:
            url = self.personality_url.format(sign=sign)
            yield response.follow(url,callback=self.parse_personality)

    def parse_personality(self, response):
        personalities = response.xpath('//ul[@class="list-inline list-unstyled profile-tabs"]/following-sibling::div[2]//text()[not(ancestor::a)]').extract()
        request_url = response.url
        sign = request_url.split('/')[-2]
        if response.status == 200:
            sql = 'UPDATE `{table_name}` SET personality = "{text}" ' \
                      'WHERE name = "{name}"'.format(table_name='astrology',
                                                     text=''.join(personalities).replace('"',"'"),
                                                     name=sign)
            self.db.update(sql)
            self.db.connection.commit()
        elif response.status == 404:
            url = self.personality_url_secondary.format(sign=sign)+'?'+sign
            yield response.follow(url,callback=self.parse_personality_secondary)

    def parse_personality_secondary(self, response):
        personalities = response.xpath('//div[@id="personality"]/h2/following-sibling::p//text()').extract()
        request_url = response.url
        sign = request_url.split('?')[1]
        sql = 'UPDATE `{table_name}` SET personality = "{text}" ' \
                      'WHERE name = "{name}"'.format(table_name='astrology',
                                                     text=''.join(personalities).replace('"',"'"),
                                                     name=sign)
        self.db.update(sql)
        self.db.connection.commit()

        for sign in self.sign:
            for type in self.type:
                url = self.love_friendfamily_careermoney.format(sign=sign,type=type)+'?'+sign+'?'+type
                yield response.follow(url,callback=self.parse_love_friendfamily_careermoney)

    def parse_love_friendfamily_careermoney(self, response):
        datas = response.xpath("//div[@class='profile-banner']/following-sibling::div[1]//text()").extract()

        request_url = response.url
        sign = request_url.split('?')[1]
        type = request_url.split('?')[2]
        if response.status == 200:
            sql = 'UPDATE `{table_name}` SET {type} = "{text}" ' \
                          'WHERE name = "{name}"'.format(table_name='astrology',
                                                         text=''.join(datas).replace('"',"'"),
                                                         type=self.basic_table.get(type),
                                                         name=sign)
            self.db.update(sql)
            self.db.connection.commit()
        elif response.status == 404:

            sign = request_url.split('?')[1]
            url = self.personality_url_secondary.format(sign=sign) + "?" + sign + '&'+str(random.randint(1,100000))
            yield response.follow(url,callback=self.parse_love_friendfamily_careermoney_secondary)


    def parse_love_friendfamily_careermoney_secondary(self, response):
        request_url = response.url
        sign = request_url.split('?')[1].split('&')[0]
        for type in self.type_secondary:
            datas = response.xpath("//div[@id='"+type+"']/p//text()").extract()
            sql = 'UPDATE `{table_name}` SET {type} = "{text}" ' \
                          'WHERE name = "{name}"'.format(table_name='astrology',
                                                         text=''.join(datas).replace('"',"'"),
                                                         type=self.type_secondary_table.get(type),
                                                         name=sign)
            self.db.update(sql)
            self.db.connection.commit()
        for sign_one in self.sign:
            for sign_two in self.sign:
                url = self.match_url.format(source_sign=sign_one, target_sign=sign_two)
                yield response.follow(url,callback=self.parse_match)

    def parse_match(self, response):
        data = response.xpath("//div[@class='skills div inpage']//div[@class='skills-div-block']/div/@style").extract()
        url = response.url
        source_sign = url.split('/')[-2].split('-')[0]
        target_sign = url.split('/')[-2].split('-')[1]

        tmp = []
        for pct in data:
            actual_pct = pct.split(';')[0].split(':')[1].strip(' %')
            tmp.append(actual_pct)
        tmp.insert(0, '"'+source_sign+'"')
        tmp.insert(1, '"'+target_sign+'"')
        tmp_str = ','.join(tmp)
        sql = 'INSERT INTO `'+self.match_table.get('table_name')+'` (`issuer_sign`, `receive_sign`, ' \
                                                                 '`sexual_intimacy_pct`, `trust_pct` ,' \
                                                                 '`communication_intellect_pct`, `emotions_pct`, ' \
                                                                 '`values_pct`, `sharedactivities_per`,`total_per`)' \
                                                                 ' VALUES ('+tmp_str+')'
        print (sql, 'tmp_str.......................')
        self.db.update(sql)
        self.db.connection.commit()
