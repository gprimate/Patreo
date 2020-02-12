import ee

ee.Initialize()

landsat = ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_123032_20140515').select(['B4', 'B3', 'B2'])

# To have an RGB image
landsat = landsat.visualize(bands = ['B4', 'B3', 'B2'], max =0.5)

geometry = ee.Geometry.Rectangle([116.2621, 39.8412, 116.4849, 40.01236])

task = ee.batch.Export.image.toDrive(image=landsat,
        description = '01_image_predefined',
        region = geometry,
        scale = 30)

task.start()