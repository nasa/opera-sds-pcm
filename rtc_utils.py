rtc_granule_regex = (
    r'(?P<id>'
    r'(?P<project>OPERA)_'
    r'(?P<level>L2)_'
    r'(?P<product_type>RTC)-'
    r'(?P<source>S1)_'
    r'(?P<burst_id>\w{4}-\w{6}-\w{3})_'
    r'(?P<acquisition_ts>(?P<acq_year>\d{4})(?P<acq_month>\d{2})(?P<acq_day>\d{2})T(?P<acq_hour>\d{2})(?P<acq_minute>\d{2})(?P<acq_second>\d{2})Z)_'
    r'(?P<creation_ts>(?P<cre_year>\d{4})(?P<cre_month>\d{2})(?P<cre_day>\d{2})T(?P<cre_hour>\d{2})(?P<cre_minute>\d{2})(?P<cre_second>\d{2})Z)_'
    r'(?P<sensor>S1A|S1B)_'
    r'(?P<spacing>30)_'
    r'(?P<product_version>v\d+[.]\d+)'
    r')'
)

rtc_product_file_regex = (
    rtc_granule_regex + ''
    r'(_'
    r'(?P<pol>VV|VH|HH|HV|VV\+VH|HH\+HV)|_BROWSE|_mask)?'
    r'[.]'
    r'(?P<ext>tif|tiff|h5|png|iso\.xml)$'
)

rtc_product_file_revision_regex = rtc_product_file_regex[:-1] + r'-(?P<revision>r\d+)$'

rtc_relative_orbit_number_regex = r"t(?P<relative_orbit_number>\d+)"

if __name__ == "__main__":
    pass