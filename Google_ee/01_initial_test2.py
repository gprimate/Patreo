import ee

def download(image):
    image = image.visualize(bands = ['B4', 'B3', 'B2'], max =0.5)
    task = ee.batch.Export.image.toDrive(image=image,
                                         description = 'With_funcition',
                                         folder = 'teste_download_data_coordenada',
                                         region = geometry,
                                         scale = 10)
    task.start()

ee.Initialize()

landsat = ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_123032_20140515').select(['B4', 'B3', 'B2'])

# To have an RGB image
landsat = landsat.visualize(bands = ['B4', 'B3', 'B2'], max =0.5)

geometry = ee.Geometry.Rectangle([116.2621, 39.8412, 116.4849, 40.01236])

download(landsat)