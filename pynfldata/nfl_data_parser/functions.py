import urllib3
import time


# helper function to get the xml and ensure that status 200 is returned
def download_xml(path: str, timeout_secs: int = 2):
    http = urllib3.PoolManager()
    r = http.request('GET', path)
    assert r.status == 200
    time.sleep(timeout_secs)
    return r.data
