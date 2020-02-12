
from geetools import batch
import ee

def download(image):
    image = image.visualize(bands = ['B4', 'B3', 'B2'], max =0.5)
    task = ee.batch.Export.image.toDrive(image=image,
                                         description = 'median_test',
                                         region = geometry,
                                         scale = 10)
    task.start()

ee.Initialize()

x = -43.937127
y = -19.934303
side_size = 0.0086

# Squared centered at x,y
geometry = [[x - side_size, y - side_size], [x - side_size, y + side_size],
            [x + side_size, y + side_size], [x + side_size, y - side_size]]


# Use Sentinel 2 surface reflectance data.
satellite = ee.ImageCollection("COPERNICUS/S2")

initial_date = '2018-01-01'
final_date = '2018-12-31'

dataset = satellite.filterDate(initial_date, final_date).filterBounds(ee.Geometry.Polygon(geometry))
#help(batch.imagecollection.toDrive)


task = batch.imagecollection.toDrive(dataset,
                                         'collection_test',
                                         region = geometry,
                                         scale = 10)
task.start() 