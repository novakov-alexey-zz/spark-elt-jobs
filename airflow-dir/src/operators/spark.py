import os
import subprocess
from typing import Optional, Any, Dict

from airflow.exceptions import AirflowException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator, SparkSubmitHook


# Custom SparkSubmitHook to pass returncode to XCom if user wants to skip it
class SparkSubmitHookCustom(SparkSubmitHook):

    def __init__(self,
                 skip_exit_code: Optional[int] = None,
                 conf=None,
                 conn_id='spark_default',
                 files=None,
                 py_files=None,
                 archives=None,
                 driver_class_path=None,
                 jars=None,
                 java_class=None,
                 packages=None,
                 exclude_packages=None,
                 repositories=None,
                 total_executor_cores=None,
                 executor_cores=None,
                 executor_memory=None,
                 driver_memory=None,
                 keytab=None,
                 principal=None,
                 proxy_user=None,
                 name='default-name',
                 num_executors=None,
                 status_poll_interval=1,
                 application_args=None,
                 env_vars=None,
                 verbose=False,
                 spark_binary=None):
        super().__init__(conf=conf, conn_id=conn_id, files=files, py_files=py_files, archives=archives,
                         driver_class_path=driver_class_path, jars=jars, java_class=java_class, packages=packages,
                         exclude_packages=exclude_packages, repositories=repositories,
                         total_executor_cores=total_executor_cores, executor_cores=executor_cores,
                         executor_memory=executor_memory, driver_memory=driver_memory, keytab=keytab,
                         principal=principal, proxy_user=proxy_user, name=name, num_executors=num_executors,
                         status_poll_interval=status_poll_interval, application_args=application_args,
                         env_vars=env_vars, verbose=verbose, spark_binary=spark_binary)
        self.skip_exit_code = skip_exit_code

    def submit(self, application: str = "", **kwargs: Any) -> int:
        """
        Remote Popen to execute the spark-submit job

        :param application: Submitted application, jar or py file
        :type application: str
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        spark_submit_cmd = self._build_spark_submit_command(application)

        if self._env:
            env = os.environ.copy()
            env.update(self._env)
            kwargs["env"] = env

        # pylint: disable=consider-using-with
        self._submit_sp = subprocess.Popen(
            spark_submit_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=-1,
            universal_newlines=True,
            **kwargs,
        )

        self._process_spark_submit_log(
            iter(self._submit_sp.stdout))  # type: ignore
        returncode = self._submit_sp.wait()

        if (returncode or (self._is_kubernetes and self._spark_exit_code != 0)) and returncode != self.skip_exit_code:
            if self._is_kubernetes:
                raise AirflowException(
                    "Cannot execute: {}. Error code is: {}. Kubernetes spark exit code is: {}".format(
                        self._mask_cmd(
                            spark_submit_cmd), returncode, self._spark_exit_code
                    )
                )
            else:
                raise AirflowException(
                    "Cannot execute: {}. Error code is: {}.".format(
                        self._mask_cmd(spark_submit_cmd), returncode
                    )
                )

        self.log.debug("Should track driver: %s",
                       self._should_track_driver_status)

        # We want the Airflow job to wait until the Spark driver is finished
        if self._should_track_driver_status:
            if self._driver_id is None:
                raise AirflowException(
                    "No driver id is known: something went wrong when executing " +
                    "the spark submit command"
                )

            # We start with the SUBMITTED status as initial status
            self._driver_status = "SUBMITTED"

            # Start tracking the driver status (blocking function)
            self._start_driver_status_tracking()

            if self._driver_status != "FINISHED":
                raise AirflowException(
                    "ERROR : Driver {} badly exited with status {}".format(
                        self._driver_id, self._driver_status
                    )
                )

        return returncode


class SparkSubmitReturnCode(SparkSubmitOperator):
    def __init__(self,
                 skip_exit_code: Optional[int] = None,
                 application='',
                 conf=None,
                 conn_id='spark_default',
                 files=None,
                 py_files=None,
                 archives=None,
                 driver_class_path=None,
                 jars=None,
                 java_class=None,
                 packages=None,
                 exclude_packages=None,
                 repositories=None,
                 total_executor_cores=None,
                 executor_cores=None,
                 executor_memory=None,
                 driver_memory=None,
                 keytab=None,
                 principal=None,
                 proxy_user=None,
                 name='arrow-spark',
                 num_executors=None,
                 status_poll_interval=1,
                 application_args=None,
                 env_vars=None,
                 verbose=False,
                 spark_binary=None,
                 **kwargs):
        super().__init__(
            application=application,
            conf=conf,
            conn_id=conn_id,
            files=files,
            py_files=py_files,
            archives=archives,
            driver_class_path=driver_class_path, jars=jars, java_class=java_class, packages=packages,
            exclude_packages=exclude_packages, repositories=repositories, total_executor_cores=total_executor_cores,
            executor_cores=executor_cores, executor_memory=executor_memory, driver_memory=driver_memory,
            keytab=keytab, principal=principal, proxy_user=proxy_user, name=name, num_executors=num_executors,
            status_poll_interval=status_poll_interval, application_args=application_args, env_vars=env_vars,
            verbose=verbose, spark_binary=spark_binary,
            **kwargs
        )
        self.skip_exit_code = skip_exit_code
        self._hook: Optional[SparkSubmitHook] = None

    def execute(self, context: Dict[str, Any]) -> int:
        if self._hook is None:
            self._hook = self._get_hook()
        return_code = self._hook.submit(self._application)
        task_instance = context['task_instance']
        task_instance.xcom_push(
            'return_code',
            return_code
        )
        return return_code

    def _get_hook(self) -> SparkSubmitHookCustom:
        return SparkSubmitHookCustom(
            skip_exit_code=self.skip_exit_code,
            conf=self._conf,
            conn_id=self._conn_id,
            files=self._files,
            py_files=self._py_files,
            archives=self._archives,
            driver_class_path=self._driver_class_path,
            jars=self._jars,
            java_class=self._java_class,
            packages=self._packages,
            exclude_packages=self._exclude_packages,
            repositories=self._repositories,
            total_executor_cores=self._total_executor_cores,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            keytab=self._keytab,
            principal=self._principal,
            proxy_user=self._proxy_user,
            name=self._name,
            num_executors=self._num_executors,
            status_poll_interval=self._status_poll_interval,
            application_args=self._application_args,
            env_vars=self._env_vars,
            verbose=self._verbose,
            spark_binary=self._spark_binary,
        )
