import os
import json
import re
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from functools import wraps
from typing import List, Union
from dotenv import load_dotenv

import numpy as np
import pandas as pd
import shooju
from dateutil.relativedelta import relativedelta
from deepdiff import DeepDiff
from shooju import Connection, Point, RemoteJob
from shooju.points_serializers import pd_series

from .logger import Logger


class ShoojuTools(object):
    """A class that makes your life easier with Shooju"""

    logger = Logger("Shooju Tools").logger

    null_value_list = ['nan', 'None', "NaT"]

    def __init__(self, job_batch_size: int = 4000, scroll_batch_size: int = 4000, silence_alerts: bool = False):
        """
        Creates shooju connection and sets batch_size

        Args:
            job_batch_size: The batch_size used for when you create a job.
            scroll_batch_size: The batch_size used for a scroll.
            silence_alerts: When True, stops alerting any info logs from the ShoojuTools class.
        """
        self._get_variables()
        self._environment_variables()
        self.sj = Connection(server=self.shooju_api_server,
                             user=self.shooju_api_user_name,
                             api_key=self.shooju_api_key)
        self.job_batch_size = job_batch_size
        self.scroll_batch_size = scroll_batch_size
        self._special_char_pattern = r"[-!@#$%^&*()\[\]{};:,./<>?|`~=+\s]"
        self._silence_alerts = silence_alerts  # when True, it will silence all info alerts

    @property
    def special_char_pattern(self):
        return self._special_char_pattern

    @special_char_pattern.setter
    def special_char_pattern(self, pattern):
        # Ensure the pattern is a valid regular expression
        try:
            re.compile(pattern)
            self._special_char_pattern = pattern
        except re.error:
            self.logger.error("Invalid regular expression pattern provided.")

    def _get_variables(self):
        load_dotenv()
        """Gets the required variables by fetching the environment variables."""
        self.shooju_api_key = os.environ["SHOOJU_KEY"]
        self.shooju_api_user_name = os.environ["SHOOJU_USER"]
        self.shooju_api_server = os.environ["SHOOJU_SERVER"]

    def _get_JOB_batch_size(self, kwargs_dictionary: dict = None) -> int:
        """
        Uses the kwargs from a function to either extract the job's batch_size or set it as the class' pre-defined batch
        size

        Args:
            kwargs_dictionary: A dictionary

        Returns:
            The Job's batch size
        """
        if kwargs_dictionary is None:
            kwargs_dictionary = {}
        if bool(kwargs_dictionary):
            if "job_batch_size" in kwargs_dictionary.keys():
                job_batch_size = kwargs_dictionary["job_batch_size"]
            elif "batch_size" in kwargs_dictionary.keys():
                job_batch_size = kwargs_dictionary["batch_size"]
            else:
                raise ValueError("Could not find the key job_batch_size in the keyword arguements")
        else:
            job_batch_size = self.job_batch_size
        return job_batch_size

    def _get_SCROLL_batch_size(self, kwargs_dictionary: dict = None) -> int:
        """
        Uses the kwargs from a function to either extract the scroller's batch_size or set it as the class' pre-defined
        batch size

        Args:
            kwargs_dictionary: A dictionary

        Returns:
            The Scroller's batch size
        """
        if kwargs_dictionary is None:
            kwargs_dictionary = {}
        if bool(kwargs_dictionary):
            if "scroll_batch_size" in kwargs_dictionary.keys():
                scroll_batch_size = kwargs_dictionary["scroll_batch_size"]
            else:
                raise ValueError("Could not find the key scroll_batch_size in the keyword arguements")
        else:
            scroll_batch_size = self.scroll_batch_size
        return scroll_batch_size

    @staticmethod
    def _environment_variables():
        """Performs a check to make sure all environment variables needed for this class are given."""
        assert "SHOOJU_KEY" in os.environ.keys(), "The environment variable SHOOJU_KEY was not provided"
        assert "SHOOJU_USER" in os.environ.keys(), "The environment variable SHOOJU_USER was not provided"
        assert "SHOOJU_SERVER" in os.environ.keys(), "The environment variable SHOOJU_SERVER was not provided"

    @staticmethod
    def pd_series_to_sj_points(series: pd.Series) -> List[Point]:
        """Transform the series to points, in which format it can be saved in Shooju."""
        return [Point(dt, value) for dt, value in series.items()]

    @staticmethod
    def to_structured_query(sid):
        if not sid.startswith(("sid=", "=F.", "={{")):
            sid = "sid=\"" + sid + "\""
        return sid

    @staticmethod
    def time_elapsed_message(current_index: int, total_number: int, start_time: float):
        end_dt = time.time()
        msg = f"Moved {current_index + 1} out of {total_number}. \n"
        f"{round((current_index + 1) / total_number * 100, 2)} % Completed. \n"
        f"Total time elapsed: {round(end_dt - start_time, 0)}s. \n"
        "Expected Completion Time: "
        f"{datetime.fromtimestamp(start_time + ((end_dt - start_time) / ((current_index + 1) / total_number)))}"
        return msg

    @staticmethod
    def split_df(chunk_size, df):
        splits = np.ceil(df.shape[0] / chunk_size)
        if splits > 1:
            data = [
                df.loc[df.series_id_name.isin(sids)] for sids in np.array_split(df.series_id_name.to_numpy(), splits)
            ]
            return data
        return [df]


    def retry_on_already_exists_shooju_conflict(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            retry_count = 3
            attempt = 0
            while attempt < retry_count:
                try:
                    return func(self, *args, **kwargs)
                except shooju.ShoojuApiError as e:
                    if e.message.startswith('already_exists'):
                        attempt += 1
                        recent_job_id = e.message.split('job ')[1].split(')')[0]
                        recent_job = RemoteJob(conn=self.sj, job_id=int(recent_job_id),batch_size=100)
                        self.logger.info(f"Finishing recent job {recent_job_id}")
                        recent_job.finish()
                        original_job_name = kwargs.get("job_name") or kwargs.get("name")
                        alt_job_name = f'{original_job_name} job upload retry {datetime.now().strftime("%H%M")}'
                        self.logger.info(f"Attempt {attempt} - Retrying upload with different job name: {alt_job_name}")
                        # Modify the job name argument for the retry
                        if "job_name" in kwargs:
                            kwargs["job_name"] = alt_job_name
                        elif "name" in kwargs:
                            kwargs["name"] = alt_job_name
                        if attempt == retry_count:
                            self.logger.warning("Max retry attempts reached for job conflict error. Raising error.")
                            raise
                    else:
                        raise

        return wrapper

    @staticmethod
    def conv_points_to_dict(df: pd.DataFrame) -> dict:
        """
        Returns a dataframe with columns date, value, sid to a dictionary with the sid as the key and the value as the
        series of date/value points.

        Returns:
            A dictionary with the sid as the key and the series of date/value points as the value.
        """
        result = {}
        df = df[["date", "value", "sid"]].set_index("sid")
        for _sid, group in df.groupby(level=0):
            result[_sid] = group.set_index("date")["value"]
        return result


    def _log_conflict_error(self, error_message, series_id, current_job_id):
        if error_message.startswith('already_exists'):
            point_dt = error_message.split('date ')[1].split(' was')[0]
            recent_job_id = error_message.split('job ')[1].split(')')[0]
            recent_job = self.sj.raw.get(f'/jobs/{recent_job_id}')['job']
            self.logger.error(
                f'you are using an older job to write one datapoint that has already been written by a more recent job, creating a write conflict. For {series_id} at {point_dt} you are currently trying to write with job {current_job_id} (https://energyaspects.shooju.com/#jobs/{current_job_id}) but it was already written by {recent_job_id} (https://energyaspects.shooju.com/#jobs/{recent_job_id}) created by {recent_job["user"]}. Consider registering a new job and/or coordinating parallel jobs to avoid conflicts.')

    def get_active_jobs(self, query, max_pages: int = 2):
        params = {
            'query': query,
            'per_page': 10,
            'page': 1,
            'fields': 'id,description,created_at,user',
            'sort': 'created_at desc'
        }

        jobs = {}

        while True:
            response = self.sj.raw.get('/jobs', params=params)
            jobs.update({
                _job['id']: {
                    'description': _job['description'],
                    'created_at': _job['created_at'],
                    'user': _job["user"]
                } for _job in response['results']
            })
            if len(jobs) == response['total']:
                break
            if params['page'] == max_pages:
                break
            params['page'] += 1

        return jobs

    def register_and_check_job(self, job_name: str, batch_size: int = None, no_history: bool = False):
        batch_size = batch_size if batch_size else 100
        if no_history:
            return self.sj.register_job(description=job_name, batch_size=batch_size, no_history=no_history)
        query = f"description:\"{job_name}\"* created_at>=0db"
        # getting all jobs with a similar description that were created within the current date
        jobs = self.get_active_jobs(query=query)
        if jobs:
            jobs = OrderedDict(sorted(jobs.items(), key=lambda _job: _job[1]["created_at"], reverse=True))
            # order jobs based on the timestamp they were created
            latest_job = jobs.popitem(last=False)
            # get the latest job no matter who the user is
            if latest_job[1]["user"] == self.sj.user:
                return RemoteJob(conn=self.sj, job_id=int(latest_job[0]), batch_size=batch_size)
            else:
                return self.sj.register_job(description=job_name, batch_size=batch_size)
        else:
            return self.sj.register_job(description=job_name, batch_size=batch_size)

    def delete_sj_sids(self, query: Union[str, list], from_list: bool = False, job: shooju.RemoteJob = None, **kwargs):
        r"""
        Deletes the series in shooju based on a query or a list of sids.

        Args:
            query: The query containing the sids that you want to delete or a list with the sids you want to delete.
            from_list: Pass True when you pass a list of sids to delete.
            job: A registered job that will be used instead of a newly created one if passed

        Keyword Args:
            batch_size: The size of the batch for each job that you want to use for deleting sids.

        Examples:
            1. From List

            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> sj = ShoojuTools()
            >>> list_of_sids = ["sid=series_name_1",
            ...                 "sid=series_name_2",
            ...                 "sid=series_name_3"]
            >>> sj.delete_sj_sids(
            ...     query=list_of_sids,
            ...     from_list=True
            ... )

            2. Using a multi query

            >>> from helper_functions_ea import ShoojuTools
            >>> sj = ShoojuTools()
            >>> from_query: r"sid=users\\constantinos.spanachis\\eur\\* AND source=NationalGrid"
            >>> sj.delete_sj_sids(
            ...     query=list_of_sids,
            ...     from_list=False
            ... )

            3. Passing a job

            >>> from helper_functions_ea import ShoojuTools
            >>> sj = ShoojuTools()
            >>> from_query: r"sid=users\\constantinos.spanachis\\eur\\* AND source=NationalGrid"
            >>> job = sj.register_and_check_job("Deleting sids", batch_size=100)
            >>> sj.delete_sj_sids(
            ...     query=list_of_sids,
            ...     from_list=False,
            ...     job=job
            ... )

        """
        if not self._silence_alerts:
            ShoojuTools.logger.info("Deleting sids")
        if not from_list:
            total_series = self.get_total_number_of_sids(query=query)
            # shit together and give me a solution to getting the number of sids under a query
        else:
            total_series = len(query)
            list_of_sids = [sid.replace("sid=", "") for sid in query]
            query = "sid=(\"" + "\",\"".join(list_of_sids) + "\")"
        if int(total_series) == 0:
            ShoojuTools.logger.info("There are no series to delete")
        else:
            ShoojuTools.logger.info(f"Deleting {total_series}...!!!")
            time.sleep(10)
            if not job:
                job = self.register_and_check_job(job_name='delete_sids' + str(datetime.utcnow().timestamp()),
                                                  batch_size=self._get_JOB_batch_size(kwargs))
            job.delete_series(query=query, one=False)
            job.submit()

    def shooju_write(self, sid: str, series: pd.Series = None, metadata: dict = None, name: str = None,
                     job: shooju.RemoteJob = None, remove_others: str = None, **kwargs) -> int:
        r"""
        Writes both points and metadata in a defined series id.
        Args:
            sid: The series id you want to store the data with.
            series: The pandas Series containing your data.
            metadata: The dictionary containing the metadata for the current sid.
            name: The name of the job, for if a job isn't passed in. otherwise ignored.
            job (optional): A registered job that will be used instead of a newly created one if passed
            remove_others: If you want to delete pre-existing points, fields or both when you upload, without deleting
                the sid. The available options are: None, fields, points, all

        Keyword Args:
            job_batch_size: Enter batch size if you want it set differently than the one you initiated the class with.

        Examples:
            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> sj = ShoojuTools()
            >>> series
                index value
            0   datetime() 3.1415
            1   datetime() 1.6100
            2   datetime() 2.5921
            >>> metadata
            {
            "field_1": "value_of_field_1",
            "field_2": "value_of_field_2",
            "field_3": "vaLue_of_field_3"
            }
            >>> sj.shooju_write(
            ...     sid=r"tests\helper_functions\shooju_write_sid",
            ...     series=series,
            ...     metadata=metadata
            ... )

        Notes:
        1. Field names or values should not contain special characters.
        2. Field values should not contain spaces.
        3. Field names cannot be smaller than 4 characters.
        """

        if not job:
            if not name:
                name = f"Uploading {sid}"
            job = self.register_and_check_job(
                f"{name} {datetime.today().strftime('%Y-%m-%d')}",
                batch_size=self._get_JOB_batch_size(kwargs)
            )

        with job:
            try:
                sid = ShoojuTools.to_structured_query(sid=sid)
                points = None
                if not series.empty:
                    points = self.pd_series_to_sj_points(series=series)
                job.write(series_query=sid, fields=metadata, points=points,
                          remove_others=remove_others)
            except Exception as e:
                raise e
            job_id = job.job_id
            job.submit()
            return job_id

    def update_fields_from_query(self, query: str, metadata: dict, job_name: str = None, job: shooju.RemoteJob = None,
                                 **kwargs) -> None:
        r"""
        A method which removes\/changes fields defined within the metadata from a bulk query.

        Args:
            query: This is the query which contains all the series that you want to alter.
            metadata: A dictionary with the updated metadata.
            job: A registered job that will be used instead of a newly created one if passed
            job_name: The name of the job, for if a job isn't passed in. otherwise ignored.

        Keyword Args:
            job_batch_size: Enter batch size if you want it set differently than the one you initiated the class with.
            scroll_batch_size: Enter batch size if you want it set differently than the one you initiated the class with

        Examples:
            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> sj = ShoojuTools()
            >>> metadata = {
            ...     "field_1": "field_value_1",  # the new value you want to store for field_1
            ...     "field_2": "field_value_2", # the new value you want to store for field_2
            ...     "field_3": None  # if you want to remove a field pass None as it's value
            ...     }
            >>> metadata
            {
                 field_1: "field_value_1",
                 field_2: "field_value_2",
                 field_3: None
            }
            >>> query="source=GRTGaz country=France region=EUR unit=MWh"
            >>> sj.update_fields_from_query(
            ...     query = query,
            ...     metadata = metadata
            ... )

        Notes:
            If you wish to remove a field, just pass the field value as None.
        """
        if job_name is None:
            job_name = f"Updating fields on {datetime.today().strftime('%Y-%m-%d')}"

        if not job:
            job = self.register_and_check_job(
                job_name=job_name,
                batch_size=self._get_JOB_batch_size(kwargs)
            )

        i = 0
        for sid_results in self.sj.scroll(query, fields=["series_id"], batch_size=self._get_SCROLL_batch_size(kwargs)):
                series_id = ShoojuTools.to_structured_query(sid_results["series_id"])
                job.write(series_query=series_id, fields=metadata)
                i += 1

        job.submit()
        if not self._silence_alerts:
                ShoojuTools.logger.info(f"Updating fields on {query} was successful.")


    def get_points_from_sid_into_series(self, sid: str, date_start: Union[datetime, str],
                                        date_finish: Union[datetime, str],
                                        number_of_points: int = -1) -> pd.Series:
        r"""
        A function which returns a single series back as a pandas Series object.
        Args:
            sid: The series id you want to extract.
            date_start: The date from which you want your data returned. Format should be "%Y-%m-%d" or MIN if you want to
            extract from the beginning of this series. You can also pass dynamic dates (see
            date_finish: The date up to which you want your data returned. Format should be "%Y-%m-%d" or MAX if you want to
            extract until the end of this series.
            number_of_points (optional): The amount of points you want returned. Set to -1 so that it returns all the data.

        Returns:
            A pandas Series with the queried points.

        Examples:
            1. Extracting all the data from a given series.
            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> class_ = ShoojuTools()
            >>> series = class_.get_points_from_sid_into_series(
            ...                                sid="users\\constantios.spanachis\\eur\\national_grid\\407_1034",
            ...                                date_start="MIN",
            ...                                date_finish="MAX",
            ...                                number_of_points=-1
            ...                                )

            2. Extracting the data from a given series between two given dates.
            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> class_ = ShoojuTools()
            >>> series = class_.get_points_from_sid_into_series(
            ...                                sid="users\\constantios.spanachis\\eur\\national_grid\\407_1034",
            ...                                date_start="2019-01-01",
            ...                                date_finish="2020-01-01",
            ...                                number_of_points=-1
            ...                                )

            3. Extracting the data using dynamic fields.
            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> class_ = ShoojuTools()
            >>> series = class_.get_points_from_sid_into_series(
            ...                                sid="users\\constantios.spanachis\\eur\\national_grid\\407_1034",
            ...                                date_start="-1yb",
            ...                                date_finish="-1db",
            ...                                number_of_points=-1
            ...                                )
        """

        # check if query is structured
        sid = ShoojuTools.to_structured_query(sid)
        series = self.sj.get_series(series_query=sid,
                                    df=date_start,
                                    dt=date_finish,
                                    max_points=number_of_points,
                                    serializer=shooju.pd_series)["points"].sort_index()
        return series

    def get_fields_from_sid(self, sid: str, fields: list = ["*"]):
        """
        Get fields from query
        Args:
            sid: The sid you are interested in.
            fields: The fields you want to get

        Returns:
            A pandas series containing the fields you have requsted.
        """
        sid = ShoojuTools.to_structured_query(sid)
        metadata = self.sj.get_series(series_query=sid,
                                      fields=fields)["fields"]
        return metadata

    def sj_facet(self, query: str, fields: list = ["*"], max_facet_values: int = 500):
        """
        Creates the SJ Facet request
        Args:
            query: The query containing the sids you want to extract the fields for.
            fields: The fields you would like to retrieve.
            max_facet_values: The number of facets you want to return

        Returns:
            Dictionary with the unique metadata fields/values and the counts the occurrence for each unique meta value
        """
        facets = self.sj.raw.get("/series", params={
            "query": query,
            "per_page": 0,
            "facets": ",".join(fields),
            "max_facet_values": max_facet_values
        })
        return facets

    def get_unique_field_values(self, query: str, fields: list = ["*"], max_facet_values: int = 500):
        """
        Get unique values for fields from query
        Args:
            query: The query containing the sids you want to extract the fields for.
            fields: The fields you would like to retrieve.
            max_facet_values: The number of facets you watn to return

        Returns:
            A dictionaty with fields as keys and list of unique values as values

        Examples:
            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> query = r'sid=tests\\us_prod_model\\default_user\\*'
            >>> fields = ['energy_product', 'scenario_id']
            >>> result = ShoojuTools().get_unique_field_values(query=query, fields=fields)
            {'energy_product': ['crude_oil', 'natural_gas'],
             'scenario_id': ['base_1674668494.877542', 'new_scenario_1674725967.580367']}

        """
        facets = self.sj_facet(query, fields, max_facet_values)
        return {field: sorted([val.get("term") for val in values.get("terms", [{}]) if val.get("term") is not None])
                for field, values in facets.get("facets", {}).items()}

    def get_fields_from_query(self, query: str, fields: list = ["*"]) -> pd.DataFrame:
        """
        Collects the fields from a query, and returns in long format.

        Args:
            query: The query containing the sids you want to extract the fields for.
            fields: The fields you would like to retrieve.

        Returns:
            A pandas dataframe that contains all the selected fields for each series retrieved in a long format.
        """
        if fields == ["*"]:
            fields_df = self.sj.get_df(query=query, fields=fields)
            fields_df.index = fields_df.series_id.copy()
            fields_df.index.name = None
            return fields_df

        else:
            scroller = self.sj.scroll(query, fields=fields, batch_size=self._get_SCROLL_batch_size())
            request_dir = {}
            for s in scroller:
                request_dir[s["series_id"]] = [s["fields"][i] for i in fields]
            return pd.DataFrame.from_dict(request_dir, orient="index", columns=fields)

    def move_container_sids(self, query, container_from, container_to, move_metadata=True, delete_previous_sids=False):
        """
        A bulk change of sid prefixes.

        Args:
            query: The initial queried data that you want to rename.
            container_from: The initial prefix of those data.
            container_to: The prefix you want to replace with.
            move_metadata (optional): If True this function will move all metadata to the new folder as well.
            delete_previous_sids (optional): If TRUE it will remove all previous sids. Set to False
        """
        scroller = self.sj.scroll(query, fields=["*"], max_points=-1,
                                  batch_size=self._get_JOB_batch_size(),
                                  serializer=shooju.points_serializers.pd_series)
        total_series = len([i for i in self.sj.scroll(query, fields=["series_id"])])
        i = 0
        with self.register_and_check_job(
                job_name='Moving data' + datetime.today().strftime('%Y%m%d'),
                batch_size=10) as job:
            if not self._silence_alerts:
                ShoojuTools.logger.info(f"Moving {total_series} series from {container_from} to {container_to}. ")
            start_time = time.time()
            container_from, container_to = container_from.lower(), container_to.lower()
            for s in scroller:
                series = s["points"]
                metadata = s["fields"]
                old_sid = s["series_id"].lower()
                if old_sid.startswith(container_from):
                    new_sid = old_sid.replace(container_from, container_to)
                else:
                    raise ValueError(f"Please check your parameters. The sid returned from the query was {old_sid}")
                if not move_metadata:
                    metadata = None
                new_sid = ShoojuTools.to_structured_query(new_sid)
                job.write(series_query=new_sid,
                          fields=metadata,
                          points=[Point(idx, value) for idx, value in series.to_dict().items()])
                if delete_previous_sids:
                    old_sid = ShoojuTools.to_structured_query(old_sid)
                    job.delete_series(old_sid)
                if not self._silence_alerts:
                    ShoojuTools.logger.info(
                        ShoojuTools.time_elapsed_message(
                            current_index=i, total_number=total_series, start_time=start_time)
                    )
                i += 1
        job.submit()

    def change_sid_name(self, sid_from, sid_to, date_start="MIN", date_finish="MAX", delete_previous_sids=False,
                        remove_others: str = None):
        """
        Changing a series id and deleting if requested.

        Args:
            sid_from: The series that you want to rename.
            sid_to: The new name of that series.
            date_start (optional): The data period from which you want the data to be moved. Defaulted at MIN.
            date_finish: The data period up to which you want the data to be moved. Defaulted at MAX.
            delete_previous_sids (optional): If true then it removes the previous sid. Defaulted at false.
            remove_others: If you want to delete pre-existing points, fields or both when you upload, without deleting
                the sid. The available options are: None, fields, points, all

        """
        with self.register_and_check_job(job_name='Moving data' + datetime.today().strftime('%Y%m%d'),
                                         batch_size=10) as job:
            sid_from = ShoojuTools.to_structured_query(sid_from)
            series = self.sj.get_series(series_query=sid_from,
                                        fields="*",
                                        df=date_start,
                                        dt=date_finish,
                                        max_points=-1)
            points = None
            metadata = None
            if series.get("points"):
                points = self.pd_series_to_sj_points(series=series["points"])
            if series.get("fields"):
                metadata = series["fields"]
            if points or metadata:
                if delete_previous_sids:
                    job.delete_series(sid_from)
            else:
                raise RuntimeError(f"No points or metadata were retrieved for {sid_from}, please check your query.")
            sid_to = ShoojuTools.to_structured_query(sid_to)
            job.write(series_query=sid_to, fields=metadata, points=points,
                      remove_others=remove_others)
            job.submit()

    def download_shooju_data_multiple_query(self, query: str,
                                            fields: Union[str, list],
                                            date_start: Union[datetime, str] = "MIN",
                                            date_finish: Union[datetime, str] = "MAX",
                                            max_points: int = -1,
                                            **kwargs) -> pd.DataFrame:
        r"""
        Downloads a query as a long format dataframe.

        Args:
            query: The query containing your sids
            fields: The fields you want to query in either a string if it is one option or a list of strings if many.
            date_start(optional): The date from which you want your data returned. This is set to "MIN"
            date_finish(optional): The date up to which you want your data returned. This is set to MAX
            max_points (optional): The amount of points you want returned. Set to -1 so that it returns all the data.

        Keyword Args:
            scroll_batch_size: Enter batch size if you want it set differently from the initiated value.

        Returns:
            A pandas dataframe containing your query in long format.

        Notes:
            Date Format should be "%Y-%m-%d" or MIN\/Max if you want to extract from the beginning/end of this series.
            You can also pass dynamic dates (see `Date Input Notation <http://docs.shooju.com/date-input-notation/>`_ .
            This applies for the date_start and date_finish entries.

        Examples:
            1. Query data using metadata fields.

            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> sj = ShoojuTools()
            >>> query = r"source=GRTGaz country=France economic_property=demand"
            >>> fields = ["source", "country", "series_id", "energy_product", "description"]
            >>> date_start = "MIN"
            >>> date_finish = "0yb"
            >>> max_points = -1
            >>> sj.download_shooju_data_multiple_query(
            ...     fields=fields,
            ...     query=query,
            ...     date_start=date_start,
            ...     date_finish=date_finish,
            ...     max_points=-1)
                               value  ... country  description
            date                      ...
            2012-01-01  1.802399e+07  ...  France  Consumption PIRR North L
            2012-01-02  1.802399e+07  ...  France  Consumption PIRR North L
            2012-01-03  1.802399e+07  ...  France  Consumption PIRR North L
            2012-01-04  1.802399e+07  ...  France  Consumption PIRR North L
            2012-01-05  1.802399e+07  ...  France  Consumption PIRR North L
                              ...  ...                               ...
            2019-12-28  1.588388e+09  ...  France  Total consumption GRTgaz
            2019-12-29  1.768963e+09  ...  France  Total consumption GRTgaz
            2019-12-30  2.007812e+09  ...  France  Total consumption GRTgaz
            2019-12-31  2.060860e+09  ...  France  Total consumption GRTgaz
            2020-01-01  1.904791e+09  ...  France  Total consumption GRTgaz

            2. Download using a sid prefix

            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> sj = ShoojuTools()
            >>> query = r"sid:teams\natural_gas\eur\gazprom_esp_sales"
            >>> fields = ["source", "country", "series_id", "energy_product", "description"]
            >>> date_start = "-2yb"
            >>> date_finish = "MAX"
            >>> max_points = -1
            >>> sj.download_shooju_data_multiple_query(
            ...     fields=fields,
            ...     query=query,
            ...     date_start=date_start,
            ...     date_finish=date_finish,
            ...     max_points=-1)
                           value  ...  country
            date                  ...
            2018-04-01    0.0000  ...      NaN
            2018-05-01    0.0000  ...      NaN
            2018-06-01    0.0000  ...      NaN
            2018-07-01    0.0000  ...      NaN
            2018-08-01    0.0000  ...      NaN
                          ...  ...      ...
            2019-09-01   66.6200  ...  Germany
            2019-10-01   53.3367  ...  Germany
            2019-11-01   66.3067  ...  Germany
            2019-12-01  115.5367  ...  Germany
            2020-01-01    0.5880  ...  Germany
        """
        scroller = self.sj.scroll(query=query,
                                  fields=fields,
                                  max_points=max_points,
                                  df=date_start,
                                  dt=date_finish,
                                  serializer=shooju.pd_series,
                                  batch_size=self._get_SCROLL_batch_size(kwargs)
                                  )
        list_of_df = []
        for i, series in enumerate(scroller):
            data = series.get('points', pd.Series(index=pd.DatetimeIndex(data=[]), dtype="float64")).to_frame(
                name="value")
            if not data.empty:
                data.index.names = ["date"]
                data["number"] = i
                data = data.reset_index().set_index(['number', 'date'])
                if 'fields' in series:
                    fields = pd.DataFrame(series['fields'], index=[i])
                    fields.index.names = ["number"]
                    data = data.join(fields, how='left')
                list_of_df.append(data)
        try:
            data = pd.concat(list_of_df, join='outer')
            if not self._silence_alerts:
                ShoojuTools.logger.info(f'Data found for the query: "{query}"')
            return data.reset_index('number', drop=True)
        except Exception as e:
            ShoojuTools.logger.error(f'No data found for the query: "{query}" The error was: {e}')
            raise Exception("Empty Dataframe") from e

    @retry_on_already_exists_shooju_conflict
    def df_upload_long(self, job_name: str, df: pd.DataFrame, sid_prefix: str, remove_others: str = None,
                       job: shooju.RemoteJob = None, repdate: Union[datetime, date] = None,
                       **kwargs) -> int:
        r"""
        General method for exporting long format DataFrames. the input DataFrame must have the following shape

        Args:
            job_name: Job name for Shooju
            df: DataFrame to upload
            sid_prefix: root_folder for upload
            remove_others: If you want to delete pre-existing points, fields or both when you upload, without deleting
                the sid. The available options are: None, fields, points, all
            job (optional): A registered job that will be used instead of a newly created one if passed
            repdate (optional): It is the reported date with which you would like to snapshot the data. If None, nothing
                will be snapshotted

        Keyword Args:
            job_batch_size: Enter batch size if you want it set differently than the one you initiated the class with.

        Returns:
            Shooju job id

        Examples:
            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> sj = ShoojuTools()
            >>> df
                index date value series_id_name field_1 field_2
            1   datetime() 3.1415 seriesname\1\ f1 f2
            2   datetime() 1.6100 seriesname\1\ f1 f2
            3   datetime() 100000 seriesname\2\ f1 f3
            >>> sj.df_upload_long(
            ...     df=df,
            ...     sid_prefix=r"tests\helper_functions\df_upload_long",
            ...     job_name="Uploading Long Dataframe")

        Notes:
            1. The index does not matter. It will not be saved.
            2. If you don't want to store a field for a specific sid, then pass that as None.
            3. Make sure that your field names are more than 3 letters.
            4. Do not add \\ at the end of your prefix.
        """
        assert not (sid_prefix.startswith('"') or sid_prefix.startswith("'")), "sid prefix should not start with a quote"
        cols = ('date', 'value', 'series_id_name')

        if not job:
            job = self.register_and_check_job(
                f"{job_name} {datetime.today().strftime('%Y-%m-%d')}",
                batch_size=self._get_JOB_batch_size(kwargs)
            )

        msg = "one column is missing from ('date','value','series_id_name')"
        assert all(elem in list(df.columns) for elem in cols), msg
        chunk_size = 2 * 10 ** 6
        if repdate:
            chunk_size = 1 * 10 ** 6
            assert isinstance(repdate, (datetime, date)), "Please provide repdate as a datetime instance"
        if not self._silence_alerts:
            ShoojuTools.logger.info(f'Uploading data for the job: {job_name}')
        for _df in self.split_df(chunk_size=chunk_size, df=df):
            fields = _df.columns.drop(['value', 'date']).values.tolist()
            if len(fields) == 1:
                fields = fields[0]
                # Avoiding future warning from pandas. See here:
                # https://stackoverflow.com/questions/75478267/how-to-use-pandas-groupby-in-a-for-loop-futurewarning
            _df.loc[:, fields] = _df.loc[:, fields].astype(str)
            grouped = _df.groupby(fields)
            series_id = ""
            try:
                for name, group in grouped:
                    name_ = list(name) if isinstance(name, (list, tuple)) else [name]  # Avoid reading strings as lists
                    name_ = [None if i in self.null_value_list else i for i in name_]
                    fields_ = list(fields) if isinstance(fields, (list, tuple)) else [fields]
                    meta_fields = dict(zip(fields_, name_))
                    sid = re.sub(self.special_char_pattern, "", meta_fields['series_id_name'])
                    meta_fields['series_id_name'] = sid
                    series_id = f'sid="{sid_prefix}\\{sid}"' if (sid_prefix and sid_prefix != "") else f'sid="{sid}"'
                    data = group.set_index('date')['value']
                    points = self.pd_series_to_sj_points(data)
                    job.write(series_query=series_id, fields=meta_fields, points=points, remove_others=remove_others)
                    if repdate:
                        job.write_reported(series_query=series_id, fields=meta_fields, points=points, reported_date=repdate)
                job.submit()
            except shooju.ShoojuApiError as error:
                self._log_conflict_error(error.message, series_id, job.job_id)
                raise
        self.get_job_status(int(job.job_id))
        return int(job.job_id)  # Forcing the job id to be an integer

    @retry_on_already_exists_shooju_conflict
    def df_upload_wide(self, sid_pefix: str, df: pd.DataFrame, metadata_dictionary: dict, name: str,
                       remove_others: str = None, job: shooju.RemoteJob = None,
                       repdate: Union[datetime, date] = None, **kwargs) -> int:
        r"""
        Uploads the values within a dataframe under a specific container which begins with the specified sid prefix.
        Uses the columns as extension to the sid prefix. Removes any special characters and spaces from the column names
        and then uploads each column in Shooju.

        Args:
            sid_pefix: A prefix under which all points will be stored with.
            df: A dataframe with the columns specified by the sid addition to the prefix. Index should be in datetime
                format
            name: The name with which this job will be registered as.
            metadata_dictionary: The dictionary containing the metadata.
            remove_others: If you want to delete pre-existing points, fields or both when you upload, without deleting
                the sid. The available options are: None, fields, points, all
            job (optional): A registered job that will be used instead of a newly created one if passed
            repdate (optional): It is the reported date with which you would like to snapshot the data. If None, nothing
                will be snapshotted
        Keyword Args:
            job_batch_size: Enter batch size if you want it set differently than the one you initiated the class with.

        Returns:
            Shooju job id

        Examples:
         >>> from helper_functions_ea import ShoojuTools, check_env
         >>> check_env()
         >>> sj = ShoojuTools()
         >>> df
                index column_name_1 column_name_2 column_name_3
            0   datetime() 3.14152 5.584215 8952.25521
            1   datetime() 5.11252 8.32154 82541.0003
            2   datetime() 9.12673 0.56482 9852895.85
            3   datetime() 1.08951 8.54851 456421.455
            metadata = {
            ...     "column_name_1": {
            ...            "field_1": "value_of_field_1",
            ...            "field_2": "value_of_field_2",
            ...            "field_3": "vaLue_of_field_3"
            ...            },
            ...     "column_name_2": {
            ...            "field_1": "value_of_field_1",
            ...            "field_2": "value_of_field_2",
            ...            "field_3": "vaLue_of_field_3"
            ...            },
            ...     "column_name_3": {
            ...            "field_1": "value_of_field_1",
            ...            "field_2": "value_of_field_2",
            ...            "field_3": "vaLue_of_field_3"
            ...            }
            ... }
            >>> sj.df_upload_wide(
            ...    df=df,
            ...    sid_prefix=r"tests\helper_functions\df_upload_wide",
            ...    metadata_dictionary=metadata_dictionary,
            ...    job_name="Uploading Wide dataframe"
            ...  )

        Notes:
            1. Field names or values should not contain special characters.
            2. Field values should not contain spaces.
            3. Field names cannot be smaller than 4 characters.
            4. Column names should be the same ones as the df
        """
        assert not (sid_pefix.startswith('"') or sid_pefix.startswith("'")), "sid prefix should not start with a quote"
        differences = set(df.columns).difference(set(metadata_dictionary.keys()))
        assert len(differences) == 0, \
            f"The columns you provided do not match the keys of the dictionary.\n" \
            f"The values that are wrong are:\n {differences}"
        chunk_size = 1.8 * 10 ** 6
        if repdate:
            chunk_size = 0.9 * 10 ** 6
            assert isinstance(repdate, (datetime, date)), "Please provide repdate as a datetime instance"
        if not job:
            job = self.register_and_check_job(
                job_name=f"{name} {datetime.today().strftime('%Y-%m-%d')}",
                batch_size=self._get_JOB_batch_size(kwargs))
        splits = np.ceil(df.size / chunk_size)
        sid=""
        try:
            for columns in np.array_split(df.columns, splits, axis=0):
                for column in columns:
                    if sid_pefix.endswith("\\"):
                        sid = sid_pefix + re.sub(self.special_char_pattern, "", column)
                        # makes sure that all special characters are removed
                    elif sid_pefix == "":
                        sid = re.sub(self.special_char_pattern, "", column)
                    else:
                        sid = sid_pefix + "\\" + re.sub(self.special_char_pattern, "", column)
                    series = df[column]
                    metadata = metadata_dictionary[column]
                    points = self.pd_series_to_sj_points(series=series)
                    sid = ShoojuTools.to_structured_query(sid)
                    job.write(sid, fields=metadata, points=points, remove_others=remove_others)
                    if repdate:
                        job.write_reported(series_query=sid, fields=metadata, points=points, reported_date=repdate)
                job.submit()
        except shooju.ShoojuApiError as error:
            self._log_conflict_error(error.message, sid, job.job_id)
            raise
        self.get_job_status(int(job.job_id))
        return int(job.job_id)  # Forcing the job id to be an integer

    def remove_values_from_sids(self, query: str, date: Union[str, datetime], onwards: bool):
        r"""
        Given a sid_query, remove all values from a certain date (not inclusive) onwards or backwards.

        Args:
            query: sid query from which all the data wrong data would be removed
            date: date (not inclusive) from which data points will be removed from the series. Date can be datetime()
                , date(), 'MAX', 'MIN', or relative date format
            onwards: anything from or up to the specified date will be removed

        Example:
            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> from datetime import datetime
            >>> check_env()
            >>> sj = ShoojuTools()
            >>> sj.remove_values_from_sids(sid_query=r'sid:users\mohsin.zafar\test2',
            ...                         date=datetime(2021, 2, 1 ),
            ...                         onwards=True)
        """
        try:
            if not self._silence_alerts:
                ShoojuTools.logger.info(f"Removing data for {query} in shooju")
            # onwards check
            if onwards:
                series_list = list(self.sj.scroll(query, max_points=-1, dt=date))
            else:
                series_list = list(self.sj.scroll(query, max_points=-1, df=date))

            total_series = len(series_list)

            i = 0

            start_time = time.time()

            with self.register_and_check_job(job_name=f"Updating {query}",
                                             batch_size=self._get_JOB_batch_size()) as job:
                if not self._silence_alerts:
                    ShoojuTools.logger.info(f"Updating {total_series} series.")
                for series in series_list:
                    # if the series does not have points then skip
                    if "points" not in series.keys():
                        ShoojuTools.logger.warning(
                            f"""There were no points found for the series {series["series_id"]}, """
                            f"within the range that was provided"
                        )
                        continue

                    time_series = series["points"]
                    sid = series['series_id']
                    sid = f'sid="{sid}"'
                    # update the series
                    job.write(series_query=sid,
                              points=time_series,
                              remove_others="points")
                    if not self._silence_alerts:
                        ShoojuTools.logger.info(
                            ShoojuTools.time_elapsed_message(
                                current_index=i, total_number=total_series, start_time=start_time
                            )
                        )
                    i += 1

        except Exception as e:
            raise RuntimeError(f"Did not remove values from {query} The error was {e}") from e
        job.submit()

    def get_list_of_sids_into_df(self,
                                 list_of_sids: Union[list, tuple, set],
                                 fields: List = ["*"],
                                 date_start: Union[datetime, str] = "MIN",
                                 date_finish: Union[datetime, str] = "MAX",
                                 max_points: int = -1,
                                 operator: str = ""
                                 ) -> pd.DataFrame:
        r"""
        A method that returns a long format dataframe using a list of sids and a list of fields between two specified
        dates.

        Args:
            list_of_sids: An iterable of sid names
            fields: A list of fields.
            date_start: The date from which you want to extract the data
            date_finish: The date to which you want to extract the data
            max_points: Number of points to return
            operator: The operator you would like to act on the list of sids.
                See `Shooju Operators <http://docs.shooju.com/operators/>`

        Returns:
            A long format dataframe with the points for the provided list of sids

        Examples:

            1. Simple list of sids that gets extracted

                >>> from helper_functions_ea import check_env
                >>> check_env()
                >>> ls_of_sids = [
                ...    r"teams\external_data\reuters\temp\forwards\forward_curve\BRTDTDMc",
                ...    r"teams\external_data\reuters\temp\forwards\forward_curve\LCOc",
                ...    r"teams\external_data\reuters\temp\forwards\forward_curve\DUBSGSWMc",
                ...    r"teams\external_data\reuters\temp\forwards\forward_curve\CLc"
                ...    ]
                >>> sj = ShoojuTools()
                >>> df = sj.get_list_of_sids_into_df(list_of_sids=ls_of_sids)

            2. List of sids with a complex operator

                >>> from helper_functions_ea import check_env
                >>> check_env()
                >>> ls_of_sids = [
                ...    r"teams\external_data\reuters\temp\forwards\forward_curve\BRTDTDMc",
                ...    r"teams\external_data\reuters\temp\forwards\forward_curve\LCOc",
                ...    r"teams\external_data\reuters\temp\forwards\forward_curve\DUBSGSWMc",
                ...    r"teams\external_data\reuters\temp\forwards\forward_curve\CLc"
                ...    ]
                >>> operator = "@A:M@S:y"
                >>> sj = ShoojuTools()
                >>> df = sj.get_list_of_sids_into_df(list_of_sids=ls_of_sids)
        """
        list_of_sids = [sid.replace("sid=", "") for sid in list_of_sids]

        operator = f"@{operator}" if not operator.startswith("@") and operator != "" else operator

        query = "sid=(\"" + "\",\"".join(list_of_sids) + "\")" + operator
        if not self._silence_alerts:
            ShoojuTools.logger.info(f"Extracting {query} from Shooju between {date_start} and {date_finish}")

        df = self.sj.get_df(query=query,
                            fields=fields,
                            series_axis="rows",
                            df=date_start,
                            dt=date_finish,
                            max_points=max_points)

        return df

    def df_upload_long_reported(self, job_name: str, df: pd.DataFrame, sid_prefix: str,
                                repdate: datetime = datetime.today(), job: shooju.RemoteJob = None,
                                **kwargs) -> int:
        r"""
        General method for exporting long format DataFrames as repdate versions in SJ.
        The input DataFrame must have the following shape

        Args:
            job_name: Job name for Shooju
            df: DataFrame to upload
            sid_prefix: root_folder for upload
            repdate: The date for which you want to write repdate series
            job (optional): A registered job that will be used instead of a newly created one if passed

        Keyword Args:
            job_batch_size: Enter batch size if you want it set differently than the one you initiated the class with.

        Returns:
            job_id: The job id that was used to upload the data

        Examples:
            >>> from helper_functions_ea import ShoojuTools, check_env
            >>> check_env()
            >>> sj = ShoojuTools()
            >>> df
                index date value series_id_name field_1 field_2
            1   datetime() 3.1415 seriesname\1\ f1 f2
            2   datetime() 1.6100 seriesname\1\ f1 f2
            3   datetime() 100000 seriesname\2\ f1 f3
            >>> sj.df_upload_long(
            ...     df=df,
            ...     sid_prefix=r"tests\helper_functions\df_upload_long",
            ...     job_name="Uploading Long Dataframe")

        Notes:
            1. The index does not matter. It will not be saved.
            2. If you don't want to store a field for a specific sid, then pass that as None.
            3. Make sure that your field names are more than 3 letters.
            4. Do not add \\ at the end of your prefix.
        """
        assert not (sid_prefix.startswith('"') or sid_prefix.startswith("'")), "sid prefix should not start with a quote"
        cols = ('date', 'value', 'series_id_name')
        msg = "one column is missing from ('date','value','series_id_name')"
        assert all(elem in list(df.columns) for elem in cols), msg

        if not job:
            job = self.register_and_check_job(job_name=job_name, batch_size=self._get_JOB_batch_size(kwargs))
        if not self._silence_alerts:
            ShoojuTools.logger.info(f'Uploading data for the job: {job_name}')
        for _df in self.split_df(chunk_size=2 * 10 ** 6, df=df):
            fields = _df.columns.drop(['value', 'date']).values.tolist()
            _df.loc[:, fields] = _df.loc[:, fields].astype(str)
            grouped = _df.groupby(fields)
            for name, group in grouped:
                name_ = list(name) if isinstance(name, (list, tuple)) else [
                    name]  # Avoid reading strings as lists
                name_ = [None if i in self.null_value_list else i for i in name_]
                fields_ = list(fields) if isinstance(fields, (list, tuple)) else [fields]
                meta_fields = dict(zip(fields_, name_))
                sid = re.sub(self.special_char_pattern, "", meta_fields['series_id_name'])
                meta_fields['series_id_name'] = sid
                series_id = f'sid="{sid_prefix}\\{sid}"' if (sid_prefix and sid_prefix != "") else f'sid="{sid}"'
                data = group.set_index('date')['value']
                job.write_reported(series_query=series_id, fields=meta_fields, points=self.pd_series_to_sj_points(data),
                                   reported_date=repdate)
        return int(job.job_id)  # Forcing the job id to be an integer

    def df_upload_wide_reported(self, sid_pefix: str, df: pd.DataFrame, metadata_dictionary: dict, name: str,
                                repdate: datetime = datetime.today(), job: shooju.RemoteJob = None,
                                **kwargs) -> int:
        r"""
        Uploads repdate versions of series within a dataframe under a specific container which begins with the specified
        sid prefix. Uses the columns as extension to the sid prefix. Removes any special characters and spaces from the
        column names and then uploads each column in Shooju.

        Args:
            sid_pefix: A prefix under which all points will be stored with.
            df: A dataframe with the columns specified by the sid addition to the prefix. Index should be in datetime
                format
            name: The name with which this job will be registered as.
            metadata_dictionary: The dictionary containing the metadata.
            repdate: If you want to delete pre-existing points, fields or both when you upload, without deleting
                the sid. The available options are: None, fields, points, all
            job (optional): A registered job that will be used instead of a newly created one if passed

        Keyword Args:
            job_batch_size: Enter batch size if you want it set differently than the one you initiated the class with.

        Returns:
            job_id: The job id that was used to upload the data

        Examples:
         >>> from helper_functions_ea import ShoojuTools, check_env
         >>> check_env()
         >>> sj = ShoojuTools()
         >>> df
                index column_name_1 column_name_2 column_name_3
            0   datetime() 3.14152 5.584215 8952.25521
            1   datetime() 5.11252 8.32154 82541.0003
            2   datetime() 9.12673 0.56482 9852895.85
            3   datetime() 1.08951 8.54851 456421.455
            metadata = {
            ...     "column_name_1": {
            ...            "field_1": "value_of_field_1",
            ...            "field_2": "value_of_field_2",
            ...            "field_3": "vaLue_of_field_3"
            ...            },
            ...     "column_name_2": {
            ...            "field_1": "value_of_field_1",
            ...            "field_2": "value_of_field_2",
            ...            "field_3": "vaLue_of_field_3"
            ...            },
            ...     "column_name_3": {
            ...            "field_1": "value_of_field_1",
            ...            "field_2": "value_of_field_2",
            ...            "field_3": "vaLue_of_field_3"
            ...            }
            ... }
            >>> sj.df_upload_wide(
            ...    df=df,
            ...    sid_prefix=r"tests\helper_functions\df_upload_wide",
            ...    metadata_dictionary=metadata_dictionary,
            ...    name="Uploading Wide dataframe"
            ...  )

        Notes:
            1. Field names or values should not contain special characters.
            2. Field values should not contain spaces.
            3. Field names cannot be smaller than 4 characters.
            4. Column names should be the same ones as the df
        """
        differences = set(df.columns).difference(set(metadata_dictionary.keys()))
        assert len(differences) == 0, \
            f"The columns you provided do not match the keys of the dictionary.\n" \
            f"The values that are wrong are:\n {differences}"
        chunk_size = 1.8 * 10 ** 6
        if repdate:
            assert isinstance(repdate, (datetime, date)), "Please provide repdate as a datetime instance"
        if not job:
            job = self.register_and_check_job(
                job_name=f"{name} {datetime.today().strftime('%Y-%m-%d')}",
                batch_size=self._get_JOB_batch_size(kwargs))
        splits = np.ceil(df.size / chunk_size)
        for columns in np.array_split(df.columns, splits, axis=0):
            for column in columns:
                if sid_pefix.endswith("\\"):
                    sid = sid_pefix + re.sub(self.special_char_pattern, "", column)
                    # makes sure that all special characters are removed
                elif sid_pefix == "":
                    sid = re.sub(self.special_char_pattern, "", column)
                else:
                    sid = sid_pefix + "\\" + re.sub(self.special_char_pattern, "", column)
                try:
                    series = df[column]
                    metadata = metadata_dictionary[column]
                    points = self.pd_series_to_sj_points(series=series)
                    sid = ShoojuTools.to_structured_query(sid)
                    job.write_reported(series_query=sid, fields=metadata, points=points, reported_date=repdate)
                except Exception as e:
                    raise e
            job.submit()
        return int(job.job_id)  # Forcing the job id to be an integer

    def get_revisions_details(self, sid: str, date_start: Union[datetime, str] = None, jobs_limit: int = 300):
        r"""
        A function which returns a pandas Dataframe showing job id's and their descriptions, for job id's that have had revisions.
        Currently, Shooju only stores the last 100 jobs per manipulation ((last 100 where data was changed, last 100 where data was added, and last 100 where data was deleted),
        so will be bound by a maximum of 300 jobs in any case.

        Args:
            sid: The series id you want to extract.
            date_start: The date from which you want your job id's returned (date_start inclusive). Format should be %Y-%m-%d and can be datetime or string object
            jobs_limit: The max number of job id's that had revisions you want returned between date_start and today (max of 300 currently in Shooju).

        Returns:
             A pandas Dataframe that returns the job id's, descriptions, sources, and the update dates for the job's with revisions during the requested period.
             If the query is wrong or if the series doesn't exist, there will be no revisions and the return will be an empty DataFrame
        Examples:
             Extracting all revisions from 2021-01-01

        >>> from helper_functions_ea import check_env, ShoojuTools

        >>> check_env()

        >>> sj = ShoojuTools()
        >>> sid = r'sid=teams\crude\price_forecast\daily\brent'
        >>> revisions_df = sj.get_revisions_details(sid=sid,
        ...                     date_start='2021-01-01')

        """

        if date_start is None:
            date_start = datetime.now() - relativedelta(years=1)
        if type(date_start) is str:
            date_start = datetime.strptime(date_start, "%Y-%m-%d")
        date_start = date_start.timestamp() * 1000

        query = f"""
            (stats.new_series>0 or stats.new_points>0 or stats.changed_points>0 or stats.deleted_points>0) and
            updated_at>{date_start}
            """
        sort = 'updated_at desc'
        query_result = self.sj.raw.get('/jobs',
                                       params={'query': query,
                                               'sort': sort,
                                               'filter': sid,
                                               'per_page': jobs_limit})
        results_dict = {}
        for job in query_result['results']:
            if (job['filtered_series']['changed'] > 0 or job['filtered_series']['addedto'] > 0 or
                    job['filtered_series']['delfrom'] > 0):
                results_dict.update({job['id']: [job['description'], job['source'], job['updated_at']]})
        try:
            results_df = pd.DataFrame(results_dict)
            results_df = results_df.T.reset_index().rename(
                columns={'index': 'id', 0: 'description', 1: 'source', 2: 'updated_at'})
            results_df['id'] = results_df['id'].astype(int)
            results_df['updated_at'] = pd.to_datetime(results_df['updated_at'], unit='ms')
        except KeyError:
            ShoojuTools.logger.warning(f"Query {sid} did not return any jobs from the date {date_start}. \n"
                                       f"This could be either due to no jobs during that period, or a wrong query. \n"
                                       f"Please check.")
            # there are no revisions found, either series doesn't exist or query is wrong
            results_df = pd.DataFrame()
        return results_df

    def get_points_from_job_ids_into_df(self, sid: str, jobs_ids: Union[list, int], max_points: int = -1):
        r"""
        A function which returns a pandas Dataframe with job id's as columns, dates as rows, and series values as values.

        Args:
            sid: The series id you want to extract.
            jobs_ids: The job id's of a query you want to extract a series for.
            max_points: The amount of points you want returned. Set to -1 so that it returns all the data

        Returns:
            A pandas Dataframe with the queried values
            If a job id doesn't exist, it will be ignored
            If a query is wrong / series does not exist or if none of the job ids exist for that series, return will be empty DataFrame

        Example: Using the job id's from get_revisions_details() as inputs:
           >>> from helper_functions_ea import check_env, ShoojuTools

           >>> check_env()

            >>> sj = ShoojuTools()
            >>> sid = r'sid=teams\crude\price_forecast\daily\brent'
            >>> revisions_df = sj.get_revisions_details(sid=sid,
            ...                         date_start='2021-01-01')

            >>> jobs_ids = revisions_df['id']
            >>> df = sj.get_points_from_job_ids_into_df(sid=sid, jobs_ids=jobs_ids)
        """
        if type(jobs_ids) is int:
            jobs_ids = [jobs_ids]

        df_data_to_series_list = []
        error_list = []
        for job_id in jobs_ids:
            series_dict = self.sj.get_series(
                f'{sid}@asof:j{job_id}',
                max_points=max_points,
                serializer=shooju.points_serializers.pd_series)

            if series_dict is None:
                # There was no data returned, either the query is wrong or the series doesn't exist
                error_list.append(job_id)

            elif 'points' in series_dict:
                # if the dict has no points key, the job_id doesn't exist
                series_df = pd.DataFrame(series_dict).reset_index()
                series_df = series_df.loc[:, ['index', 'points']].rename(columns={"points": job_id}).set_index(
                    ['index'])
                df_data_to_series_list.append(series_df)
            else:
                error_list.append(job_id)

        try:
            points_data_df = pd.concat(df_data_to_series_list, axis=1)
            if len(error_list) > 0:
                ShoojuTools.logger.warning(f"Query {sid} did not return any series for job_id(s): {error_list}. \n"
                                           f"This is because the series doesn't exist for this job_id."
                                           f"Please check")
        except ValueError:
            ShoojuTools.logger.warning(f"Query {sid} did not return any jobs.\n"
                                       f"This could be either due to no jobs during that period, or a wrong query. \n"
                                       f"Please check.")
            # no jobs were found or query is wrong
            points_data_df = pd.DataFrame()
        return points_data_df

    def cleanup_sids_from_query(self, query: str, remove_others: str, job_name: str,
                                job: shooju.RemoteJob = None, **kwargs) -> int:
        r"""
        A function that removes all points from a query between two dates

        Args:
            query: The query you want to remove points from
            remove_others: If you want to delete pre-existing points, fields or both when you upload, without deleting
                the sid. The available options are: fields, points, all
            job: A registered job that will be used instead of a newly created one if passed
            job_name: The name of the job

        Returns:
            job_id: The Shooju remote job id that was used to clean-up the sids

        Example:
            >>> from helper_functions_ea import check_env, ShoojuTools

            >>> check_env()

            >>> sj = ShoojuTools()
            >>> query = r'sid=tests\crude\price_forecast\daily\brent'
            >>> sj.cleanup_sids_from_query(query=query, job_name="Removing all points from query", remove_others="points")

        """
        if not job:
            job = self.register_and_check_job(
                f"{job_name} {datetime.today().strftime('%Y-%m-%d')}",
                batch_size=self._get_JOB_batch_size(kwargs)
            )
        sids = self.sj.scroll(query, fields=["series_id"])
        for sid in sids:
            _sid = self.to_structured_query(sid['series_id'])
            job.write(series_query=_sid, remove_others=remove_others)
        job.submit()
        return int(job.job_id)  # Forcing the job id to be an integer

    def get_total_number_of_sids(self, query) -> int:
        """
        Returns the total number of SIDs in a query that has been provided by the user.

        Args:
            query: The query you want to extract the total number of SIDs from

        Returns:
            total: The total number of SIDs in the query

        Example:
            1. Single sid
                >>> from helper_functions_ea import check_env, ShoojuTools

                >>> check_env()

                >>> sj = ShoojuTools()
                >>> query = r'sid=teams\crude\price_forecast\daily\brent'
                >>> sj.get_total_number_of_sids(query=query)

            2. Prefix

                >>> from helper_functions_ea import check_env, ShoojuTools

                >>> check_env()

                >>> sj = ShoojuTools()
                >>> query = r'sid:oilx'
                >>> sj.get_total_number_of_sids(query=query)

            3. Metadata

                >>> from helper_functions_ea import check_env, ShoojuTools

                >>> check_env()

                >>> sj = ShoojuTools()
                >>> query = r'country_iso=US energy_product=crude economic_property=demand'
                >>> sj.get_total_number_of_sids(query=query)

        """
        total = self.sj.raw.get('/series', params={
            'query': query,
            'per_page': 0,
        })['total']
        return total

    def upload_only_difference_points(
            self, query: str, prefix: str, upload_data: pd.DataFrame, df: str = "MIN", dt: str = "MAX",
            job_name: str = "Uploading difference in points between Shooju and DataFrame only",
            job: shooju.RemoteJob = None, **kwargs
    ) -> int:
        """
        Uploads only the points that are different between the Shooju and the DataFrame.

        Args:
            query: The query you want to extract the sids from in shooju
            prefix: The prefix of the series you are uploading and comparing with Shooju
            upload_data: The DataFrame that contains the data. Must contain the columns series_id_name, date, and value
            df: The date from which you want to extract the data
            dt: The date to which you want to extract the data
            job_name: The name of the job.
            job: A registered job that will be used instead of a newly created one if passed

        Keyword Args:
            job_batch_size: Enter batch size if you want it set differently than the one you initiated the class with.
            scroll_batch_size: Enter batch size if you want it set differently than the one you initiated the class with

        Returns:
            job_id: The job id that was used to upload the data

        Example:
            >>> from helper_functions_ea import check_env, ShoojuTools

            >>> check_env()

            >>> sj = ShoojuTools()
            >>> prefix = r"teams\crude\price_forecast\daily\brent"
            >>> query = f'sid:{prefix}'
            >>> upload_df = pd.DataFrame({
            ...    'series_id_name': ['brent', 'brent', 'brent'],
            ...    'date': [datetime(2021, 1, 1), datetime(2021, 1, 2), datetime(2021, 1, 3)],
            ...    'value': [100, 200, None]
            ... })
            >>> sj.upload_only_difference_points(
            ...     query=query,
            ...     prefix=prefix,
            ...     upload_data=upload_df,
            ...     job_name="Uploading difference in points between Shojou and DataFrame only"
            ... )
        """
        assert {"series_id_name", "date", "value"}.issubset(set(upload_data.columns)), \
            "The DataFrame must contain the columns series_id_name, date, and value"

        assert query != "", "Please provide a query to extract the sids from Shooju"
        scroller = self.sj.scroll(
            query=query, df=df, dt=dt, max_points=-1, batch_size=self._get_SCROLL_batch_size(kwargs),
            serializer=pd_series
        )

        if scroller.raw_response["total"] > upload_data["series_id_name"].nunique():
            diff = scroller.raw_response["total"] - upload_data["series_id_name"].nunique()

            assert diff < 1500, f"The difference in the number of sids is {diff}. Please check the query"

        upload_data["sid"] = upload_data["series_id_name"].str.lower().to_numpy()
        if prefix != "":
            upload_data["sid"] = prefix.lower() + "\\" + upload_data["sid"].to_numpy()
        meta_cols = upload_data.columns.difference(["date", "value"])
        upload_data.loc[:, meta_cols] = upload_data.loc[:, meta_cols].applymap(lambda x: None if pd.isna(x) else x)
        metadata_dict = upload_data.loc[:, meta_cols].set_index("sid").drop_duplicates().to_dict(orient="index")

        points = self.conv_points_to_dict(upload_data[["sid", "date", "value"]])

        del upload_data

        if not job:
            job = self.register_and_check_job(job_name, batch_size=self._get_JOB_batch_size(kwargs))

        self.logger.info("Uploading series differences to Shooju")
        for sid in scroller:
            _sid_name = sid.get("series_id").lower()
            sj_ts = sid.get("points", pd.Series(dtype=float))
            new_ts = points.get(_sid_name, pd.Series(dtype=float))
            fields = None
            if not new_ts.empty:
                diff = pd.concat([new_ts.to_frame("new"), sj_ts.to_frame("old")], axis=1)
                mask = diff["new"] != diff["old"]
                diff = diff[mask]
                diff["points"] = None
                mask = diff["new"].notna() & (diff["old"] != diff["new"])
                diff.loc[mask, "points"] = diff.loc[mask, "new"]
                diff = diff["points"]
                fields = metadata_dict.get(_sid_name)
                del points[_sid_name]
                del metadata_dict[_sid_name]
            else:
                if sj_ts.empty:
                    continue
                sj_ts[:] = None
                diff = sj_ts.copy()
            if diff.empty:
                job.write(series_query=f'sid="{_sid_name}"', fields=fields)
            else:
                job.write(series_query=f'sid="{_sid_name}"', points=diff, fields=fields)
        self.logger.info(f"Uploading {len(points.keys())} new series")
        if points:
            for _sid, new_ts in points.items():
                fields = metadata_dict.get(_sid)
                job.write(series_query=f'sid="{_sid}"', points=new_ts, fields=fields)
        job.submit()
        return job.job_id


    def _process_series_for_compare(self, origianl_series, compare_series, extra_ignore_fields):
        self.logger.info(f"Checking for {origianl_series['series_id']} & {compare_series['series_id']}")


        difference = DeepDiff(origianl_series, compare_series, ignore_order=True)
        ignore_fields = ["series_id_name","sid"]
        if extra_ignore_fields:
            ignore_fields.extend(extra_ignore_fields)

        item_added_remove_ignore = {f"root['fields']['{field}']" for field in ignore_fields}
        values_changed = difference.get("values_changed", {})
        for key in item_added_remove_ignore:
            values_changed.pop(key, None)

        values_changed.pop("root['series_id']")

        if not difference.get("values_changed", {}):
            difference.pop("values_changed", None)

        list_item_added = difference.get("dictionary_item_added", [])
        for key in item_added_remove_ignore:
            if key in list_item_added:
                list_item_added.remove(key)

        if not difference.get("dictionary_item_added", []):
            difference.pop("dictionary_item_added", None)


        list_item_removed = difference.get("dictionary_item_removed", [])
        for key in item_added_remove_ignore:
            if key in list_item_removed:
                list_item_removed.remove(key)
        if not difference.get("dictionary_item_removed", []):
            difference.pop("dictionary_item_removed", None)

        return difference

    def compare_sids(self, query:str, compare_series_query: str, df: str = "MIN", dt: str = "MAX",
                     diff_type :str ="both", report_type :str ="detailed", ignore_fields :list =None, extra_filter :str = "") -> dict:
        """
        Compare a given sid with another sid and return the comparison results.

        The function checks for differences in field data and point data between the given sid query & prefix\\query
        It returns a dict report, depending on input report_type, dict structure is changed

        Args:
            query (str): The main query to compare.
            compare_series_query (str): The compare series for which the series comparison will be done
                                        comparison sid will be done based on query prefix replace by compare_series_query prefix
            df (str, optional): Start date for comparison. Defaults to "MIN".
            dt (str, optional): End date for comparison. Defaults to "MAX".
            diff_type(str,Optional) : Whether to find difference only for points, metadata or both
                possible values : points : Check difference for Points
                                  metadata : Check difference for metadata
                                  both: Check difference for both points & metadata. Default Value
            report_type(str, Optional) : Whether report will be details & overview
                possible values: detailed: Gives detailed list of the difference found in series
                                 overview: Gives count of different series found
            ignore_fields(list,Optional) : List of extra field data to ignore

            extra_filter str: extra filter to add in shooju query for both series

        Return:
            The dictionary consists of:
            - 'Total Original Series' (int): Total number of series in the original query.
            - 'Total Compared Series' (int): Total number of series in the compared query.
         - If report_type == "detailed":
            - "List of new series" (list): Series present in the compared dataset but missing in the original.
            - "List of missing series" (list): Series present in the original dataset but missing in the compared dataset.
            - "List of series where only points are not matching" (list): Series where metadata is identical, but point values differ.
            - "List of series where only metadata is not matching" (list): Series where point values are identical, but metadata differs.
            - "List of series where both points and metadata are not matching" (list): Series where both metadata and point values differ.
        - Otherwise:
            - "Number of new series" (int): Count of new series in the compared dataset.
            - "Number of missing series" (int): Count of missing series in the original dataset.
            - "Number of series where only points are not matching" (int): Count of series where metadata is identical, but point values differ.
            - "Number of series where only metadata is not matching" (int): Count of series where point values are identical, but metadata differs.
            - "Number of series where both points and metadata are not matching" (int): Count of series where both metadata and point values differ.
        Raises:
            ValueError: If `diff_type` or `report_type` is not one of the allowed values.

        Example:
            >>> from helper_functions_ea import check_env, ShoojuTools

            >>> check_env()
            for all series under a prefix
            >>> sj = ShoojuTools()
            >>> query = "ceic"
            >>> compare_series_query = "users\\xyz\\ceic"
            Comparing will be done between all series of ceic\\ & corresponding series in users\\xyz\\ceic\\"
            >>> result = sj.compare_sids(query,compare_series_query,ignore_fields=["airflow_dag","github_repo_url]")
            OR single series
            >>> query = "ceic\\68909501"
            >>> compare_series_query = "users\\xyz\\ceic\\68909501"
            Comparing will be done between series ceic\\68909501 & users\\xyz\\ceic\\68909501"
            >>> result = sj.compare_sids(query,compare_series_query,extra_filter="meta.dates_max>2025-01-01")
            >>> print(result)

        """
        assert diff_type in {"points", "metadata", "both"}, f"Invalid diff_type: {diff_type}. Must be 'points', 'metadata', or 'both'."
        assert report_type in {"detailed", "overview"}, f"Invalid report_type: {report_type}. Must be 'detailed' or 'overview'."
        query = query.rstrip("\*").lower()
        compare_series_query = compare_series_query.rstrip("\*").lower()
        self.logger.info(f"Fetching series for {query}")
        fields = ['*']
        max_points = -1
        if diff_type == "points":
            fields = None
        if diff_type == "metadata":
            max_points = 0


        orig_series_list = self.sj.scroll(query=f"sid:{query} {extra_filter}",fields=fields,max_points=max_points,df=df,dt=dt)
        original_series_map = {s['series_id'].lower(): s for s in orig_series_list}

        compare_series_list = self.sj.scroll(query=f"sid:{compare_series_query} {extra_filter}", fields=fields, max_points=max_points,df=df,dt=dt)
        compare_series_map = {s['series_id'].lower(): s for s in compare_series_list}
        total_original_series = len(original_series_map)


        compared_series_count = 0
        matching_series_count = 0
        missing_series = []
        points_mismatch = []
        metadata_mismatch = []
        both_mismatch = []
        future_to_series = {}
        with ThreadPoolExecutor(max_workers=os.environ.get("MAX_WORKER",4)) as executor:

            for series_id, original_series in original_series_map.items():
                compare_series_id = str(series_id).replace(query,compare_series_query)
                compare_series = compare_series_map.get(compare_series_id, None)
                if compare_series is None:
                    self.logger.warn(f"Compare series ID {compare_series_id} does not exist")
                    missing_series.append(series_id)
                else:
                    future = executor.submit(self._process_series_for_compare, original_series, compare_series,ignore_fields)
                    future_to_series[future] = series_id

        for future in as_completed(future_to_series):
            series_id = future_to_series[future]
            compare_series_id = str(series_id).replace(query,compare_series_query)
            result = future.result()
            compared_series_count += 1
            if not result:  # If no difference, it's a perfect match
                matching_series_count += 1
                self.logger.info(f"Series {series_id} & {compare_series_id} are identical")
            else :

                values_changed = result.get("values_changed", {})
                dict_added = result.get("dictionary_item_added", {})
                dict_removed = result.get("dictionary_item_removed", {})
                item_removed = result.get("iterable_item_removed", [])

                has_metadata_mismatch = any("fields" in key for key in values_changed) or \
                                        any("fields" in key for key in dict_added) or \
                                        any("fields" in key for key in item_removed) or \
                                        any("fields" in key for key in dict_removed)

                has_points_mismatch = any("points" in key for key in values_changed) or \
                                      any("points" in key for key in dict_added) or \
                                      any("points" in key for key in item_removed) or \
                                      any("points" in key for key in dict_removed)

                if has_metadata_mismatch and has_points_mismatch:
                    both_mismatch.append(series_id)
                elif has_metadata_mismatch:
                    metadata_mismatch.append(series_id)
                elif has_points_mismatch:
                    points_mismatch.append(series_id)

        new_series = [
            sid for sid in compare_series_map
            if sid.replace(compare_series_query,query) not in original_series_map
        ]

        if report_type == "detailed":
            report = {
                "Total current series": total_original_series,
                "Total series compared": compared_series_count,
                "List of new series": new_series,
                "List of missing series": missing_series,
                "Number of series matching": matching_series_count,
                "List of series where only points are not matching": points_mismatch,
                "List of series where only metadata is not matching": metadata_mismatch,
                "List of series where both points and metadata are not matching": both_mismatch
            }
        else:
            report = {
                "Total current series": total_original_series,
                "Total series compared": compared_series_count,
                "Number of new series": len(new_series),
                "Number of missing Series": len(missing_series),
                "Number of series matching": matching_series_count,
                "Number of series where only points are not matching": len(points_mismatch),
                "Number of series where only metadata is not matching": len(metadata_mismatch),
                "Number of series where both points and metadata are not matching": len(both_mismatch)
            }

        return report

    @staticmethod
    def _analyze_points(points):
        status_map = {"c": "changed", "d": "deleted"}
        changes, deleted, changed = [], False, False

        for (timestamp, value, reference_value, status) in points:
            if status in status_map:
                changes.append(
                    {'timestamp': timestamp, 'status': status_map[status], 'value': value, 'ref_value': reference_value})
                deleted |= status == "d"
                changed |= status == "c"
        return changes, deleted, changed

    def get_job_status(self, job_id:int = None):
        """
         Retrieves the status of a given job by analyzing changes in series data.
         This function fetches series associated with the specified job ID, compares field
         and point data to detect deletions and modifications, and generates a summary report
         of the changes.

         Args:
             job_id (int, optional): The unique identifier of the job whose status is to be retrieved.

         Returns:
             dict: A report containing:
                 - "Number of Series where points got deleted" (int): Count of series with deleted points.
                 - "Number of Series where field data got deleted" (int): Count of series with deleted fields.
                 - "Number of Series where points data got changed" (int): Count of series with changed points.
                 - "Number of Series where field data got changed" (int): Count of series with changed fields.
                 - "List of Series where points got deleted" (list): List of series IDs with deleted points.
         """
        xcom_file_path = '/airflow/xcom/return.json'
        xcom_dir = os.path.dirname(xcom_file_path)
        if not os.path.exists(xcom_dir):
            self.logger.info("xcom not enabled, will not proceed for checking data diff report")
            return None
        params = {
            "max_facet_values": 0,
            "date_format": "milli",
            "df": "MIN",
            "dt": "MAX",
            "include_timestamp": "y",
            "include_job": "y",
            "sort": "sid asc",
            "facets": "",
            "fields": "*,meta.asofbytes_num,meta.dates_max,meta.dates_min,meta.fieldsbytes_num,"
                      "meta.freq,meta.levels,meta.points_count,meta.points_max,meta.points_min,"
                      "meta.processor,meta.repdatebytes_num,meta.repfdates_max,meta.repfdates_min,"
                      "meta.reppdates_max,meta.reppdates_min,meta.trash_sid,meta.updated_at,"
                      "meta.updated_by,meta.upload_preset,meta.usage_obj,meta.usage_obj.byuser_obj,"
                      "meta.usage_obj.byuser_obj.reads_num,meta.usage_obj.byuser_obj.user,"
                      "meta.usage_obj.reads_num,meta.usage_obj.users_num,sid",
            "sid": r'{{sid if sid else sid}}',
            "query": "sid={}",
            "max_points": -1,
            "diff_points_only": "y",
            "location_type": "sjviews",
            "location": "_series_new"
        }
        num_points_deleted = 0
        num_fields_deleted = 0
        num_points_changed = 0
        num_fields_changed = 0
        series_with_deleted_points = []
        report = {}

        series_list = self.sj.scroll(f"meta.job:{job_id}")

        for series in series_list:
            params['query'] = f"sid={series['series_id']}"
            params['diff_job_id'] = job_id
            response = self.sj.raw.get('/series', params=params)
            series_data = response["series"][0]
            field_diffs = DeepDiff(series_data["fields_previous"], series_data["fields"], ignore_order=True).to_dict()
            points_changes, deleted, changed = self._analyze_points(series_data.get("points", []))
            if 'dictionary_item_removed' in field_diffs:
                num_fields_deleted += 1
            if 'values_changed' in field_diffs or 'type_changes' in field_diffs:
                num_fields_changed += 1
            if deleted:
                num_points_deleted += 1
                series_with_deleted_points.append(series['series_id'])
            if changed:
                num_points_changed += 1

        report_data = {
            "Number of Series where points got deleted": num_points_deleted,
            "Number of Series where field data got deleted": num_fields_deleted,
            "Number of Series where points data got changed": num_points_changed,
            "Number of Series where field data got changed": num_fields_changed,
            "List of Series where points got deleted": series_with_deleted_points
        }

        if num_points_deleted > 0 or num_fields_deleted > 0 or num_points_changed > 0 or num_fields_changed > 0:
            self.logger.info(f"report for job id {job_id} is {json.dumps(report_data)}")

        ############## air flow xcom data update #########################
            report = {key: value for key, value in report_data.items() if isinstance(value, int) and value > 0}

            if report and "KUBERNETES_SERVICE_HOST" in os.environ:  ## KUBERNETES_SERVICE_HOST to identify whether running from airflow
                if os.path.exists(xcom_file_path):
                    with open(xcom_file_path, 'r') as f:
                        try:
                            existing_data = json.load(f)   ## withing same dag task instance incase multiple time load were called.
                        except json.JSONDecodeError:
                            existing_data = {}
                else:
                    existing_data = {}

                if "job_status" not in existing_data:
                    existing_data["job_status"] = {}

                for key, value in report.items():
                    if isinstance(value, int):
                        existing_data["job_status"][key] = existing_data["job_status"].get(key, 0) + value

                with open(xcom_file_path, 'w') as f:  ### pushing to xcom
                    json.dump(existing_data, f)


        ############################ end airflow xcom #######################

        return report

