from typing import List
from operators.property_sourcing_base import PropertySourcingBaseOperator


class HKPropertySourcingOperator(PropertySourcingBaseOperator):
    @staticmethod
    def get_sfa_gfa(space_info: List):
        if len(space_info) < 2:
            return None, None
        elif len(space_info) < 7:
            return space_info[1], None
        else:
            return space_info[1], space_info[5]

    def get_property_info(self, html_source):
        import re
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_source, 'html.parser')

        rooms = []
        rents = soup.find_all("div", class_="sc-u3x3v7-25 hBypJX")

        for rent in rents:
            titles = rent.find("div", class_="sc-hs8n9o-1 iaNTuL").get_text()
            title_list = titles.strip().split("\n")
            title = title_list[0].strip()

            if len(title_list) < 3:
                sub_title = None
            else:
                sub_title = title_list[2].strip()

            space = rent.find("div", class_="sc-16di5lh-5 cxijRp").get_text().replace("ft²", "")
            space_element = re.split(" |GFA|\\xa0", space)

            sfa, gfa = self.get_sfa_gfa(space_element)
            mon_price = rent.find("span", class_="sc-1fa9gj4-4 iPsFLJ").get_text()[1:]
            location = rent.find("span", class_="sc-u3x3v7-11 iPCCQr").get_text()
            features = rent.find_all("div", class_="sc-u3x3v7-18 gwyDpM")
            features_combined = ""

            for i in range(len(features) // 2):
                features_combined += features[i].get_text() + "\\ "

            age = rent.find("div", class_="sc-1prre98-0 jNGTki")

            if age:
                age = age.get_text()
            else:
                age = None

            url = rent.find('a', href=True)['href']
            room_idx = url.split("-")[-1]

            room_info = {"date": self.execution_date, "room_idx": room_idx, "title": title, "sub_title": sub_title,
                         "sfa": sfa, "gfa": gfa, "mon_price": mon_price, "age": age, "location": location,
                         "features_combined": features_combined, "url": url}

            if room_info not in rooms:
                rooms.append(room_info)

        

        self.log.info("------- Property Info -------")
        self.log.info(rooms)
        self.log.info(f"# of properties: {len(rooms)}")
        return rooms
        
