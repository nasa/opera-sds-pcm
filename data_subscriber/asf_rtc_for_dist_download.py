from data_subscriber.download import DaacDownload
from util.conf_util import SettingsConf

class AsfDaacRtcForDistDownload(DaacDownload):

    def __init__(self, provider):
        super().__init__(provider)

    def run_download(self, args, token, es_conn, netloc, username, password, cmr,
                     job_id, rm_downloads_dir=True):
        provider = args.provider  # "ASF-RTC"
        settings = SettingsConf().cfg
