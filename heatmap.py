#Cancemi Damiano - W82000075

#PER INSTALLARE mpi4py:    "/Users/damianocancemi/anaconda/bin/pip install mpi4py"
#RUN:                      "python heatmap.py"

import gmplot

files = ["result", "result_final"]

if __name__ == '__main__':
    for file in files:
        latitude = []
        longitude = []

        with open('/Users/damianocancemi/PycharmProjects/HPC/'+file+'.txt', 'r') as f:
            for line in f:
                coords = line.split(",")
                latitude.append(float(coords[0]))
                longitude.append(float(coords[1]))

        gmap = gmplot.GoogleMapPlotter(latitude[0], longitude[0], 4)
        gmap.heatmap(latitude, longitude)
        if file == "result_final":
            gmap.marker(38.300541, -92.527408, title="Center in MISSOURI (38.300541,-92.527408)")
        gmap.draw(file+".html")