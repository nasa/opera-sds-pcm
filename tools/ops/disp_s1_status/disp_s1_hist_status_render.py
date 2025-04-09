import argparse
import json
import sys

# Render the updated geojson as an image with the map in the background using folium
import folium
import branca

colormap = branca.colormap.LinearColormap(
    vmin=0,
    vmax=100,
    colors=["red", "orange", "lightblue", "green", "darkgreen"],
    caption="DISP-S1 Processing Completion (%)",
)

with open(sys.argv[1]) as f:
    data = json.load(f)

with open(sys.argv[1]) as f:
    asc_data = json.load(f)
asc_data['features'] = [] # We use this so that we can skip the unprocessed frames from cluttering the map and layer control

with open(sys.argv[1]) as f:
    des_data = json.load(f)
des_data['features'] = [] # We use this so that we can skip the unprocessed frames from cluttering the map and layer control

m = folium.Map(location=[0, 0], zoom_start=2)
for feature in data['features']:
    frame_id = feature['properties']['frame_id']

    if not feature.get('processing_status'):
        feature['processing_status'] = {'color': 'gray', 'fillOpacity': 0.1}
        feature['properties']['completion_percentage'] = "N/A"
        feature['properties']['sensing_datetimes_triggered'] = "N/A"
        feature['properties']['possible_sending_datetimes_to_trigger'] = "N/A"
        feature['properties']['sensing_datetime_count'] = "N/A"
        feature['properties']['last_triggered_sensing_datetime'] = "N/A"
    else:
        status = feature['processing_status']
        if status['completion_percentage'] > 99:
            status['color'] = 'green'
            status['fillOpacity'] = 0.8
        elif status['completion_percentage'] == 0:
            status['color'] = 'gray'
            status['fillOpacity'] = 0.1
        else:
            status['color'] = 'yellow'
            status['fillOpacity'] = 0.8
        feature['properties']['completion_percentage'] = status['completion_percentage']
        feature['properties']['completion_percentage_str'] = str(status['completion_percentage'])+"%"
        feature['properties']['sensing_datetimes_triggered'] = status['sensing_datetimes_triggered']
        feature['properties']['possible_sending_datetimes_to_trigger'] = status['possible_sending_datetimes_to_trigger']
        feature['properties']['sensing_datetime_count'] = status['sensing_datetime_count']
        feature['properties']['last_triggered_sensing_datetime'] = status['last_triggered_sensing_datetime']

        if feature['properties']['orbit_pass'] == "DESCENDING":
            des_data['features'].append(feature)
        elif feature['properties']['orbit_pass'] == "ASCENDING":
            asc_data['features'].append(feature)
        else:
            raise ValueError(f"Unknown orbit pass: {feature['properties']['orbit_pass']}")

    feature['properties']['frame_id'] = str(feature['properties']['frame_id']) # So that we get rid of the comma in the number

popup = folium.GeoJsonTooltip(fields=['frame_id', 'completion_percentage_str', 'sensing_datetimes_triggered', 'possible_sending_datetimes_to_trigger', 'sensing_datetime_count', 'last_triggered_sensing_datetime'],
                            aliases=['Frame ID', 'Completion Percentage', 'Sensing Datetimes Triggered', 'Possible Sensing Datetimes to Trigger', 'Sensing Datetime Count', 'Last Triggered Sensing Datetime'],
                            localize=True,
                            style="width: 300px")

popup2 = folium.GeoJsonTooltip(fields=['frame_id', 'completion_percentage_str', 'sensing_datetimes_triggered', 'possible_sending_datetimes_to_trigger', 'sensing_datetime_count', 'last_triggered_sensing_datetime'],
                            aliases=['Frame ID', 'Completion Percentage', 'Sensing Datetimes Triggered', 'Possible Sensing Datetimes to Trigger', 'Sensing Datetime Count', 'Last Triggered Sensing Datetime'],
                            localize=True,
                            style="width: 300px")

folium.GeoJson(asc_data, name = "ASCENDING", style_function=
    lambda x:
    {'fillColor': colormap(x["properties"]["completion_percentage"]),
     'line_opacity': 0.01,
     "fillOpacity": x['processing_status']['fillOpacity'],
     'color': x['processing_status']['color'],
     'opacity': 0.01,
     'weight': 1},
     tooltip= popup
               ).add_to(m)

folium.GeoJson(des_data, name="DESCENDING",  style_function=
    lambda x:
    {'fillColor': colormap(x["properties"]["completion_percentage"]),
     'line_opacity': 0.01,
     "fillOpacity": x['processing_status']['fillOpacity'],
     'color': x['processing_status']['color'],
     'opacity': 0.01,
     'weight': 1},
     tooltip= popup2
               ).add_to(m)

folium.LayerControl().add_to(m)
colormap.add_to(m)

filename = sys.argv[2]
m.save(filename)