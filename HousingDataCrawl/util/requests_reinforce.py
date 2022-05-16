"""
@Time    : 2022-05-13 10:38
@Author  : zhangrui
@FileName: requests_reinforce.py
@Software: PyCharm
requests模块加强版
"""
import logging
from logging import Logger
import backoff
import requests
from requests.exceptions import HTTPError
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def fatal_code(e):
    """
    定义一个异常实例（通过code判断）
    :param e:
    :return:
    """
    return 400 <= e.response.status_code < 500


# 禁用代理和忽略SSL检查
NO_PROXY = {"http": None, "https": None}
COMMON_REQUESTS_PARAMS = {"verify": False, "proxies": NO_PROXY}

BACKOFF_RETRY_ON_EXCEPTION_PARAMS = {
    # expo: [1, 2, 4, 8, etc.] https://github.com/litl/backoff/blob/master/backoff/_wait_gen.py#L6
    "wait_gen": backoff.expo,
    # HTTPError raised by raise_for_status()
    # HTTPError code list: https://github.com/psf/requests/blob/master/requests/models.py#L943
    "exception": (HTTPError,),
    "max_tries": 4,
    "max_time": 60,  # nginx closes a session at 60' second by default
    "giveup": fatal_code,
}


@backoff.on_exception(**BACKOFF_RETRY_ON_EXCEPTION_PARAMS)
def request_with_retry(
        should_log: bool = False,
        logger: Logger = logging.getLogger(),
        logger_level: str = "info",
        **request_params
):
    full_params = COMMON_REQUESTS_PARAMS | request_params
    requests_params_keys_to_log = ["data", "json", "params"]
    if should_log:
        params_message = ""
        for key in requests_params_keys_to_log:
            if key in request_params:
                params_message += " with {} {}".format(key, request_params[key])
        log_message = "[{}] {} with params{}.".format(
            full_params["method"], full_params["url"], params_message
        )
        getattr(logger, logger_level.lower())(log_message)
    response = requests.request(**full_params)
    response.raise_for_status()
    return response

#
# if __name__ == '__main__':
#     request_params_test = {"method": "get", "url": "https://nj.lianjia.com/", "timeout": 5}
#     response_test = request_with_retry(**request_params_test)
#     print(response_test.status_code)
