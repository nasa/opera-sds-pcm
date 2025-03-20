import argparse
import json

# Render the updated geojson as an image with the map in the background using folium
import folium

with open("opera_disp_s1_hist_status-pst.geojson") as f:
    data = json.load(f)

m = folium.Map(location=[0, 0], zoom_start=2)
for feature in data['features']:
    frame_id = feature['properties']['frame_id']

    if not feature.get('processing_status'):
        feature['processing_status'] = {'color': 'gray'}
        feature['properties']['completion_percentage'] = "N/A"
        feature['properties']['sensing_datetimes_triggered'] = "N/A"
        feature['properties']['possible_sending_datetimes_to_trigger'] = "N/A"
        feature['properties']['sensing_datetime_count'] = "N/A"
        feature['properties']['last_triggered_sensing_datetime'] = "N/A"
    else:
        status = feature['processing_status']
        if status['completion_percentage'] > 99:
            status['color'] = 'green'
        elif status['completion_percentage'] == 0:
            status['color'] = 'gray'
        else:
            status['color'] = 'yellow'
        feature['properties']['completion_percentage'] = status['completion_percentage']
        feature['properties']['sensing_datetimes_triggered'] = status['sensing_datetimes_triggered']
        feature['properties']['possible_sending_datetimes_to_trigger'] = status['possible_sending_datetimes_to_trigger']
        feature['properties']['sensing_datetime_count'] = status['sensing_datetime_count']
        feature['properties']['last_triggered_sensing_datetime'] = status['last_triggered_sensing_datetime']

popup = folium.GeoJsonPopup(fields=['frame_id', 'completion_percentage', 'sensing_datetimes_triggered', 'possible_sending_datetimes_to_trigger', 'sensing_datetime_count', 'last_triggered_sensing_datetime'],
                            aliases=['Frame ID', 'Completion Percentage', 'Sensing Datetimes Triggered', 'Possible Sensing Datetimes to Trigger', 'Sensing Datetime Count', 'Last Triggered Sensing Datetime'],
                            localize=True,
                            style="width: 300px")

folium.GeoJson(data, style_function=
    lambda x:
    {'fillColor': x['processing_status']['color'],
     'line_opacity': 0.01,
     "fillOpacity": 0.8,
     'color': x['processing_status']['color'],
     'weight': 1},
     popup= popup
               ).add_to(m)
filename = "opera_disp_s1_hist_status-pst.html"
m.save(filename)


import pdfkit
import fitz
options = {'javascript-delay': 500, 'page-size': 'Letter', 'margin-top': '0.0in', 'margin-right': '0.0in', 'margin-bottom': '0.0in', 'margin-left': '0.0in', 'encoding': "UTF-8", 'custom-header': [('Accept-Encoding', 'gzip')]}
pdfkit.from_file(filename + '.html',  (filename + '.pdf'), options=options)
pdf_file = fitz.open(filename + '.pdf')
page = pdf_file.load_page(0)
pixels = page.get_pixmap()
pixels.save(filename + '.png')
pdf_file.close()
