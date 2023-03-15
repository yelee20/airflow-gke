from typing import List
from operators.property_sourcing_base import PropertySourcingBaseOperator
from bs4 import BeautifulSoup
from utils.string import multiple_replace
import re

class SquareFootSourcingOperator(PropertySourcingBaseOperator):

    def get_html_source_(self, driver, scroll_to_end: bool = True):
        import time
        self.log.info("------- Getting HTML source - STEP 1 -------")

        driver.implicitly_wait(3)
        driver.get(self.base_url)

        time.sleep(5)

        self.log.info("------- Getting HTML source - SCROLLING -------")

        if scroll_to_end:
            return self.scroll_to_end(driver)
        else:
            return self.scroll(driver)

    def get_html_source(self, driver):
        from bs4 import BeautifulSoup

        whole_source = ""
        next_url = self.base_url
        
        while next_url:
            html_source = self.get_html_source_(driver=driver, scroll_to_end=False)
            whole_source += html_source
            tmp_soup = BeautifulSoup(html_source, 'html.parser')

            next_url = tmp_soup.find_all("div", class_="ui borderless menu pagination")[-1].find("a", {"href": True, "rel": "next"})["href"]
        
        return whole_source


    def get_property_info(self, html_source):
        import pandas as pd
        soup = BeautifulSoup(html_source, 'html.parser')

        rooms = []
        rents = soup.find_all("div", class_="item property_item")
        
        for rent in rents:
            titles = rent.find("div", class_="header cat").get_text()

            tmp_title = re.sub("[\n\t]+", "@#", titles.strip())
            title_list = tmp_title.strip().split("@#")

            location = title_list[0]
            title = title_list[-1].strip()

            details = rent.find_all("div", class_="header")
            details = details[-1].get_text().strip()
            detail_list = details.split("\n")

            replacements = ("ft²", ""), (",", "")

            sfa = int(multiple_replace(detail_list[0].strip(), *replacements))
            bed_room_num = int(detail_list[1].strip())
            bath_room_num = int(detail_list[2].strip())
            mon_price = int(rent.find("span", class_="priceDesc rentDesc").get_text()[4:].replace(",",""))
            features = rent.find("div", class_="description").get_text().strip()
            url = rent.find('img', class_="desktop_myimage detail_page")["href"]

            room_idx = int(url.split("property-")[-1])

            room_info = {"room_idx": room_idx, "title": title, "bed_room_num": bed_room_num,"bath_room_num": bath_room_num,
                                    "sfa": sfa, "mon_price": mon_price, "location": location, "features_combined": features, "url": url}

            if room_info not in rooms:
                rooms.append(room_info)

        self.log.info("------- Property Info -------")
        self.log.info(rooms)
        self.log.info(f"# of properties: {len(rooms)}")
        return rooms