from typing import List
from operators.property_sourcing_base import PropertySourcingBaseOperator
from bs4 import BeautifulSoup


class MidLandRealitySourcingOperator(PropertySourcingBaseOperator):
    @staticmethod
    def get_sfa_gfa(space: List):
        if len(space) > 1:
            sfa, gfa = space[0].get_text().replace("SFA", "").replace("ft²", "").replace("\xa0", ""), \
                       space[1].get_text().replace("GFA", "").replace("ft²", "").replace("\xa0", "")
            return sfa, gfa
        elif len(space) == 1:
            return space[0].get_text().replace("SFA", "").replace("ft²", "").replace("\xa0", ""), None
        else:
            return None, None

    def get_property_info(self, html_source):
        import pandas as pd
        soup = BeautifulSoup(html_source, 'html.parser')
        
        rooms = []
        rents = soup.find_all("div", class_="sc-1r1odlb-23 etCoIy")

        for rent in rents:
            titles = rent.find("div", class_="sc-wivooq-1 hCnCJl").get_text()
            title_list = titles.strip().split("\n")
            title = title_list[0].strip()

            if len(title_list) < 3:
                sub_title = None
            else:
                sub_title = title_list[2].strip()

            space = rent.find_all("div", class_="sc-gqqyk9-1 kYfBEV")
            space_element = self.get_sfa_gfa(space)
            mon_price = rent.find("span", class_="sc-hlnw2x-6 kktEPG").get_text()[1:]
            location = rent.find("span", class_="sc-1r1odlb-9 dHhWAt").get_text()
            features = rent.find_all("div", class_="sc-1r1odlb-16 gopLNA")
            features_combined = ""

            for i in range(len(features) // 2):
                features_combined += features[i].get_text() + "&&"

            age = rent.find("div", class_="sc-w2gv6f-0 eMkKmr")

            if age:
                age = age.get_text()
            else:
                age = None

            url = rent.find('a', href=True)['href']
            room_idx = url.split("-")[-1]

            room_info = {"date": self.execution_date, "room_idx": room_idx, "title": title, "sub_title": sub_title,
                         "sfa": space_element[0], "gfa": space_element[1], "mon_price": mon_price, "age": age,
                         "location": location, "features_combined": features_combined, "url": url}

            if room_info not in rooms:
                rooms.append(room_info)

        self.log.info("------- Property Info -------")
        self.log.info(rooms)

        self.log.info(f"# of properties: {len(rooms)}")
        return rooms
