from typing import Optional, List
from airflow.utils.context import Context
from airflow.models import BaseOperator
from constants.data_category import DataCategory

from typing import Any


class PropertySourcingBaseOperator(BaseOperator):
    template_fields = (
        "provider",
        "data_category",
        "execution_date",
        "base_url",
    )

    def __init__(
            self,
            provider: str,
            data_category: str,
            execution_date: str,
            base_url: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.provider = provider
        self.execution_date = execution_date
        self.data_category = data_category
        self.base_url = base_url

    def get_chrome_driver(self):
        from selenium import webdriver
        from selenium.webdriver.firefox.options import Options as FirefoxOptions

        options = FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("--width=700")
        options.add_argument("--height=700")
        driver = webdriver.Firefox(options=options)
        self.log.info("------- driver -------")

        return driver
    

    def scroll(self, driver):
        import time
        self.log.info("------- Scrolling -------")

        last_page_height = driver.execute_script(
            "return document.documentElement.scrollHeight"
        )

        sub_height = last_page_height

        for i in range(10):
            driver.execute_script(
                f"window.scrollTo(0, {sub_height / 10 * i});"
            )
            time.sleep(0.5)
        time.sleep(4)

        return driver.page_source

    def scroll_to_end(self, driver, stand_height=0, retry_count=0):
        self.log.info("------- Scrolling -------")

        import time

        last_page_height = driver.execute_script(
            "return document.documentElement.scrollHeight"
        )
        sub_height = last_page_height

        while True:
            if retry_count >= 5:
                break

            current_height = driver.execute_script(
                "return document.documentElement.scrollHeight"
            )

            for i in range(10):
                driver.execute_script(
                    f"window.scrollTo(0, {stand_height + (sub_height / 10 * i)});"
                )
                time.sleep(0.5)
            time.sleep(4)

            new_page_height = driver.execute_script(
                "return document.documentElement.scrollHeight"
            )
            stand_height = last_page_height
            sub_height = new_page_height - last_page_height

            if new_page_height == current_height:
                self.scroll_to_end(driver, current_height, retry_count + 1)
                break
            last_page_height = new_page_height
            time.sleep(1)

            self.log.info(f"last_page_height: {last_page_height}")
            
        return driver.page_source
    
    def get_html_source(self, driver, scroll_to_end: bool = True):
        import time
        from bs4 import BeautifulSoup
        self.log.info("------- Getting HTML source - STEP 1 -------")

        driver.implicitly_wait(3)
        driver.get(self.base_url)

        time.sleep(5)

        self.log.info("------- Getting HTML source - SCROLLING -------")

        if scroll_to_end:
            return self.scroll_to_end(driver)
        else:
            return self.scroll(driver)

    @staticmethod
    def get_sfa_gfa(space: List):
        pass

    def get_property_info(self, html_source):
        raise NotImplementedError()
    
    def upload_property_info_to_gcs(self, json_data) -> None:

        from utils.aws.s3 import get_sourcing_path
        from constants.constants import GCP_CONN_ID, GCS_BUCKET_NAME
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        from os import path
        import tempfile

        import pandas as pd

        self.log.info("------- Uploading property info to AWS S3 -------")

        sourcing_path = get_sourcing_path(provider=self.provider,
                                          data_category=self.data_category,
                                          execution_date=self.execution_date)
        df_data = pd.DataFrame(json_data)
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "sourcing.csv")
            df_data.to_csv(tmp_path, index=False)

            # Upload file to GCS.
            gcs_hook = GCSHook(GCP_CONN_ID)
            gcs_hook.upload(
                bucket_name=GCS_BUCKET_NAME,
                object_name=f"{sourcing_path}.csv",
                filename=tmp_path,
            )

        self.log.info("------- PROPERTY INFO UPLOADED -------")

    def execute(self, context: Context) -> None:
        driver = self.get_chrome_driver()
        html_source = self.get_html_source(driver)
        results = self.get_property_info(html_source)
        self.upload_property_info_to_gcs(results)