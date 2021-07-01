# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import fnmatch
import logging
import itertools

from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import List, Set
from shutil import copyfile, move
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.models import BaseOperator
from datetime import datetime

# You can also make this format a parameter in the Operator, for example
# if you expect that you work with different intervals than "@daily".
# Then you can introduce time components to have a finer grain for file storage.
DATE_FORMAT = '%Y%m%d'


@dataclass_json
@dataclass
class FileTransfer:
    source_filenames: List[str]


class FileToPredictableLocationOperator(BaseOperator):
    """
    Picks up a file from somewhere and lands this in a predictable location elsewhere
    """
    template_fields = ('file_mask',)

    # @apply_defaults
    def __init__(self,
                 src_conn_id: str,
                 dst_conn_id: str,
                 file_mask: str,
                 *args,
                 **kwargs):
        """
        :param src_conn_id: Hook with a conn id that points to the source directory.
        :type src_conn_id: string
        :param dst_conn_id: Hook with a conn id that points to the destination directory.
        :type dst_conn_id: string
        :param file_mask: file mask to filter source directory files.
        :type file_mask: string
        """
        super(FileToPredictableLocationOperator,
              self).__init__(*args, **kwargs)
        self.src_conn_id = src_conn_id
        self.dst_conn_id = dst_conn_id
        self.file_mask = file_mask

    def execute(self, context):
        """
        Picks up all files from a source directory and dumps them into a root directory system,
        organized by dagid, taskid and execution_date
        """
        execution_date = context['execution_date'].strftime(DATE_FORMAT)
        src_hook = FSHook(conn_id=self.src_conn_id)
        source_dir = src_hook.get_path()

        dest_hook = FSHook(conn_id=self.dst_conn_id)
        dest_root_dir = dest_hook.get_path()

        dag_id = self.dag.dag_id
        task_id = self.task_id
        dest_dir = os.path.join(dest_root_dir, dag_id, task_id, execution_date)
        os.makedirs(dest_dir, exist_ok=True)

        logging.info("Now searching for files like {0} in {1}".format(
            self.file_mask, source_dir))
        source_names = fnmatch.filter(os.listdir(source_dir), self.file_mask)
        for file_name in source_names:
            full_path = os.path.join(source_dir, file_name)
            logging.info("Now creating path structure {0}".format(dest_dir))
            dest_file_name = os.path.join(
                dest_dir, os.path.basename(file_name))
            logging.info("Now moving {0} to {1}".format(
                full_path, dest_file_name))
            # copyfile(full_path, dest_file_name)
            move(full_path, dest_file_name)

        # pushing source files which were moved to destination
        if len(source_names) != 0:
            logging.info(f"Pushing file names {source_names}")
            task_instance = context['task_instance']
            task_instance.xcom_push(
                'file_transfer',
                FileTransfer(source_names).to_dict()
            )


class PredictableLocationToFinalLocationOperator(BaseOperator):
    """
    Picks up a file from predictable location storage and loads/transfers the results to 
    a target system (in this case another directory, but it could be anywhere).
    """

    def __init__(self,
                 src_conn_id,
                 dst_conn_id,
                 src_task_id,
                 *args,
                 **kwargs):
        """
        :param src_conn_id: Hook with a conn id that points to the source directory.
        :type src_conn_id: string
        :param dst_conn_id: Hook with a conn id that points to the destination directory.
        :type dst_conn_id: string
        :param src_task_id: Source task that produced the file of interest
        :type src_task_id: string
        """
        super(PredictableLocationToFinalLocationOperator,
              self).__init__(*args, **kwargs)
        self.src_conn_id = src_conn_id
        self.dst_conn_id = dst_conn_id
        self.src_task_id = src_task_id

    def execute(self, context):
        """
        Picks up all files from a source directory and dumps them into a root directory system,
        organized by dagid, taskid and execution_date
        """
        execution_date = context['execution_date'].strftime(DATE_FORMAT)
        src_hook = FSHook(conn_id=self.src_conn_id)
        dest_hook = FSHook(conn_id=self.dst_conn_id)
        dest_dir = dest_hook.get_path()

        dag_id = self.dag.dag_id

        source_dir = os.path.join(
            src_hook.get_path(), dag_id, self.src_task_id, execution_date)
        if os.path.exists(source_dir):
            for file_name in os.listdir(source_dir):
                full_path = os.path.join(source_dir, file_name)
                dest_file_name = os.path.join(dest_hook.get_path(), file_name)
                logging.info("Now moving {0} to final destination {1}".format(
                    full_path, dest_file_name))
                copyfile(full_path, dest_file_name)


class CheckReceivedFileOperator(BaseOperator):
    """
    Check that all required files are in destination folder
    """
    template_fields = ('file_mask',)

    def __init__(self,
                 file_mask: str,
                 file_prefixes: Set[str],
                 dst_conn_id: str,
                 *args,
                 **kwargs):
        super(CheckReceivedFileOperator, self).__init__(*args, **kwargs)
        self.file_mask = file_mask
        self.file_prefixes = file_prefixes
        self.dst_conn_id = dst_conn_id

    def get_file_transfers(self, context) -> List[FileTransfer]:
        task = context['task_instance']
        file_transfers = []
        for id in self.upstream_task_ids:
            ft = task.xcom_pull(id, key='file_transfer')
            if ft:
                file_transfers.append(
                    FileTransfer.from_dict(ft)  # type: ignore
                )
        return file_transfers

    def destination_file_names(self, context) -> List[str]:
        dest_hook = FSHook(conn_id=self.dst_conn_id)
        dest_root_dir = dest_hook.get_path()
        dag_id = self.dag.dag_id
        execution_date = context['execution_date'].strftime(DATE_FORMAT)

        dest_dirs = [os.path.join(dest_root_dir, dag_id, task_id, execution_date)
                     for task_id in self.upstream_task_ids]
        dest_names = [fnmatch.filter(os.listdir(
            dest_dir), self.file_mask) for dest_dir in dest_dirs]
        return list(itertools.chain(*dest_names))

    def execute(self, context):
        file_transfers = self.get_file_transfers(context)
        logging.info(f"file_transfers: {file_transfers}")

        if len(file_transfers) != 0:
            dest_names = destination_file_names(context)
            logging.info(f"Destination file names: {dest_names}")

            source_names = [ft.source_filenames for ft in file_transfers]
            source_names = list(itertools.chain(*source_names))
            logging.info(f"Source file names: {source_names}")

            joined_names = set(source_names + dest_names)
            logging.info(
                f"Source and destination file names with file mask '{self.file_mask}'':\n {joined_names}")
            found_prefixes = set(map(lambda n: n.split("_")[0], joined_names))

            if self.file_prefixes == found_prefixes:
                print("All files recieved!")
