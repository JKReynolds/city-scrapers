import re
from datetime import datetime
import scrapy
import string
from city_scrapers_core.constants import COMMISSION
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import CityScrapersSpider

import json

class ChiNorthwestHomeEquitySpider(CityScrapersSpider):
    name = "chi_northwest_home_equity"
    agency = "Chicago Northwest Home Equity Assurance Program"
    timezone = "America/Chicago"
    # allowed_domains = ["nwheap.com"]
    start_urls = [
        "https://nwheap.com/category/meet-minutes-and-agendas/",
        ]

    # used to convert month to number month, help with mm/dd/yyyy formatting
    calendar = {
        "january": "01",
        "february": "02",
        "march": "03",
        "april": "04",
        "may": "05",
        "june": "06",
        "july": "07",
        "august": "08",
        "september": "09",
        "october": "10",
        "november": "11",
        "december": "12"
    }

    # default location dictionary
    locationDict = {
        "name": "TBD",
        "address": "",
    }

    # dictionary used to store the results
    # key = date (mm/dd/yyyy)
    # value = Meeting object
    meetings = {

    }

    datetimeFormat = "%m/%d/%Y %I:%M%p"
    defaultStartTimeString = "12:00AM"

    def parse(self, response):
        """
        `parse` should always `yield` Meeting items.

        Change the `_parse_title`, `_parse_start`, etc methods to fit your scraping
        needs.
        """
        
        # meetMinutesAgendas = response.xpath("//div[@class='post-loop-content']")
        minutePostLinks = response.xpath(".//h2[@class='entry-title']/a/@href").getall()
        # print(minutePostLinks)

        # loop through the minute links
        for link in minutePostLinks:
            yield scrapy.Request(link, callback=self._parse_minute_page)

        # loop through the upcoming sidebar items
        upcoming_events = response.xpath("//aside[@id='em_widget-5']/ul/li[not(@class)]")
        for item in upcoming_events:
            
            # get the date string
            dateString = item.xpath("./ul/li/text()").get()
            datetimeString = dateString +  " " + self.defaultStartTimeString
            startDatetime = datetime.strptime(datetimeString, self.datetimeFormat)

            # many of these values are defaulted
            # default values should be filled in when scraping board meeting page
            meeting = Meeting(
                title=item.xpath("./a/text()").get(),
                description="",
                classification=self._parse_classification(item),
                start=startDatetime,
                end=None,
                all_day=self._parse_all_day(response),
                time_notes=self._parse_time_notes(response),
                location=self.locationDict,
                links=[],
                source=self._parse_source(response),
            )

            meeting["status"] = self._get_status(meeting)
            meeting["id"] = self._get_id(meeting)

            self.meetings[dateString] = meeting
        
        # print(self.meetings[dateString])

    # callback for parsing each minute post
    def _parse_minute_page(self, response):
        
        #get the date
        dateString = self._look_for_date(response)
        # print(dateString)
        
        #get the description
        description = self._parse_minute_description(response)

        meeting = Meeting(
            title=self._parse_minute_title(response),
            description=description,
            classification=self._parse_classification(response),
            start=self._parse_minute_start(response, description, dateString),
            end=self._parse_minute_end(response, description, dateString),
            all_day=self._parse_all_day(response),
            time_notes=self._parse_time_notes(response),
            location=self.locationDict,
            links=self._parse_minute_links(response),
            source=self._parse_source(response),
        )

        meeting["status"] = self._get_status(meeting)
        meeting["id"] = self._get_id(meeting)

        self.meetings[dateString] = meeting

        # return None
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """ Overridden `from_crawler` to connect `spider_idle` signal. """
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=scrapy.signals.spider_idle)
        return spider

    def spider_idle(self):
        """ React to `spider_idle` signal by starting JSON parsing after _parse_minutes."""
        self.crawler.signals.disconnect(self.spider_idle, signal=scrapy.signals.spider_idle)
        self.crawler.engine.crawl(scrapy.Request("https://nwheap.com/events/", callback=self._parse_all_events), self)
        raise scrapy.exceptions.DontCloseSpider
    
    def _parse_all_events(self, response):
        # for each meeting use the key to search for the row
        # include in more detail
        for date in self.meetings:
            queryString = ".//tr[contains(td,'" + date + "')]"
            tableRow = response.xpath(queryString)
            
            if tableRow:
                # get the link to the meeting
                meetingLink = tableRow.xpath(".//a/@href").get()

                cb_kwargsInput = {
                    "dateString":date
                }

                yield scrapy.Request(meetingLink, callback=self._parse_meeting_page, cb_kwargs=cb_kwargsInput)

            # else:
            #     yield self.meetings[date]
            # yield self.meetings[date]
        # print(self.meetings)
        # print("$$$$$$$$$$$$$$$$$$")
        # print(len(self.meetings))
        # print(self.meetings)


    def _parse_meeting_page(self, response, dateString):
        cb_kwargsInput = {
            "dateString":dateString,
        }

        self.meetings[dateString]["title"]=self._parse_meeting_title(response)
        # self.meetings[dateString]["description"]=self._parse_meeting_description(response)
        self.meetings[dateString]["start"]=self._parse_meeting_start(response, dateString)
        self.meetings[dateString]["end"]=self._parse_meeting_end(response, dateString)

        # get the location link
        locationLink = response.xpath(".//p[strong='Location']/a/@href").get()
        
        yield scrapy.Request(locationLink, callback=self._parse_meeting_location, cb_kwargs=cb_kwargsInput)


    # function is used to look for the date in title or description
    def _look_for_date(self, item):
        title = item.xpath(".//h1[@class='entry-title']/text()").get()
        description = item.xpath(".//div[@class='entry-content']").css('div *::text').getall()
        #remove the punctuations and create array
        title = re.sub(r'[^\w\s]',' ',title).split()
        if description:
            description = " ".join(description)
            description = description.replace("\xa0","")
            description = re.sub(r'[^\w\s]',' ',description).split()

        dateString = ""

        # search month in title
        for i in range(len(title)):
            if self.calendar.get(title[i].lower()):
                dateString = self.calendar.get(title[i].lower())
                #if the day and year the next 2 string
                if title[i+1].isdigit() and title[i+2].isdigit():
                    day = title[i+1]
                    year = title[i+2]
                    if len(day) < 2:
                        dateString += "/0" + day + "/" + year
                    else:
                        dateString += "/" + day + "/" + year
                    return dateString

        # search month in description
        for i in range(len(description)):
            if self.calendar.get(description[i].lower()):
                dateString = self.calendar.get(description[i].lower())
                #if the day and year the next 2 string
                if description[i+1].isdigit() and description[i+2].isdigit():
                    day = description[i+1]
                    year = description[i+2]
                    if len(day) < 2:
                        dateString += "/0" + day + "/" + year
                    else:
                        dateString += "/" + day + "/" + year
                    return dateString

    # used to parse minute title
    def _parse_minute_title(self, item):
        return item.xpath(".//h1[@class='entry-title']/text()").get()

    # used to parse minute description
    def _parse_minute_description(self, item):
        description = item.xpath(".//div[@class='entry-content']").css('div *::text').getall()
        if description:
            description = " ".join(description)
            description = description.replace("\xa0","")
            # description = description.translate({ord(c): " " for c in string.whitespace})
            return description
        else:
            return ""

    def _parse_classification(self, item):
        """Parse or generate classification from allowed options."""
        # Northwest Home Equity Assurance Program is governing commission appointed by the mayor
        return COMMISSION

    # parse the minutes for the start time
    def _parse_minute_start(self, item, description, dateString):
        startTimeString = self.defaultStartTimeString
        
        # check to see if time is mentioned in description
        timeStringList = None
        if description:
            timeStringList = re.findall(r'\d{1,2}(?:(?:am|pm)|(?::\d{1,2})(?:am|pm)?)', description)
        # if the time string exists then get the first time
        if timeStringList:
            startTimeString = timeStringList[0].upper()

        return datetime.strptime(dateString +  " " + startTimeString, self.datetimeFormat)

    # used to parse the minutes for the end time
    def _parse_minute_end(self, item, description, dateString):
        endTimeString = None

        # check to see if time is mentioned in description
        timeStringList = None
        if description:
            timeStringList = re.findall(r'\d{1,2}(?:(?:am|pm)|(?::\d{1,2})(?:am|pm)?)', description)
        # if the time string exists
        # if it contains two times 
        if timeStringList and len(timeStringList) > 1:
            endTimeString = timeStringList[1].upper()

        if endTimeString:
            # print("$$$$$$$$$$$")
            # print(str(endTimeString))
            return datetime.strptime(dateString +  " " + endTimeString, self.datetimeFormat)
        else:
            return None

    def _parse_time_notes(self, item):
        """Parse any additional notes on the timing of the meeting"""
        # there are no examples of notes about meeting times for any of the meetings on this site
        return ""

    def _parse_all_day(self, item):
        """Parse or generate all-day status. Defaults to False."""
        # none of the meetings were marked as all day, no way to know how they would mark it as such
        return False

    # most of the minutes don't have location
    # will have to get the location from the All Event page
    def _parse_minute_location(self, item):
        d = {
            "name": "TBD",
            "address": ""
        }
        return d

    # callback for following the minute link
    # used to parse for the pdf link
    def _parse_minute_links(self, response):
        linkList = []
        linkItems = response.xpath(".//a[contains(@href,'.pdf') or contains(@href, '.docx')]")

        # for each item get the link text and url
        for item in linkItems:
            linkDict = {
                "title":item.xpath("./text()").get(),
                "href":item.xpath("./@href").get(),
            }
            linkList.append(linkDict)

        return linkList

    # parse used on meeting page to get title
    def _parse_meeting_title(self, response):
        return response.xpath(".//h1[@class='entry-title']/text()").get()

    # parse used on meeting page to get description
    def _parse_meeting_description(self, response):
        # get the description and eliminate whitespaces
        description = " ".join(response.xpath(".//p[strong='Categories']/text()").getall())
        return " ".join(description.split())

    def _parse_meeting_start(self, response, dateString):
        timeString = response.xpath(".//p/i/text()").get()
        if timeString:
            timeString = timeString.replace("-","")
            timeString = timeString.split()
            startTimeString = timeString[0] + timeString[1].upper()
            return datetime.strptime(dateString + " " + startTimeString, self.datetimeFormat)
        else:
            return datetime.strptime(dateString + " " + self.defaultStartTimeString, self.datetimeFormat)

    def _parse_meeting_end(self, response, dateString):
        timeString = response.xpath(".//p/i/text()").get()
        if timeString:
            timeString = timeString.replace("-","")
            timeString = timeString.split()
            endTimeString = timeString[2] + timeString[3].upper()
            return datetime.strptime(dateString +  " " + endTimeString, self.datetimeFormat)
        else:
            return None

    def _parse_meeting_location(self, response, dateString):
        location_name = response.xpath(".//h1[@class='entry-title']/text()").get()
        addressList = response.xpath(".//p[strong='Address']/text()").getall()
        address = ", ".join(addressList)
        address = address.replace("\r", "")
        address = address.replace("\n", "")
        address = address.replace("\t", "")

        self.meetings[dateString]["location"]["name"] = location_name
        self.meetings[dateString]["location"]["name"] = address

        # yield self.meetings[dateString]
        self._return_items()

    def _parse_source(self, response):
        """Parse or generate source."""
        return response.url

    def _return_items(self):
        for key, item in self.meetings.items():
            yield item