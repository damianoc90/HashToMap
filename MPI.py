# Cancemi Damiano - W82000075

# PER INSTALLARE mpi4py:    "/Users/damianocancemi/anaconda/bin/pip install mpi4py"
# RUN:                      "mpiexec -n 4 python MPI.py"

from mpi4py import MPI
from math import radians, cos, sin, asin, sqrt

def haversine(lat1, lon1, radius):
    lat2 = 38.300541
    lon2 = -92.527408

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6367 * c
    if r <= radius: return True
    return False

result_local = []
RADIUS = 800

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    print "Running process", rank, "of", size

    # SCATTER DATA: FROM MASTER (rank=0) TO ALL OHTER PROCESSES
    if rank == 0:
        data = open('result.txt').read().split('\n')

        # split array in chuncks
        chunks = [[] for _ in range(size)]
        for i, chunk in enumerate(data): chunks[i % size].append(chunk)
    else:
        data = None
        chunks = None

    data = comm.scatter(chunks, root=0)

    # MPI PORTION CODE
    for item in data:
        if item:
            coords_tmp = map(float, item.split(','))
            if haversine(coords_tmp[0], coords_tmp[1], RADIUS):
                result_local.append(str(coords_tmp[0]) + "," + str(coords_tmp[1]))

    # GATHER RESULTS
    gather = comm.gather(result_local, root=0)

    if rank == 0:
        count = 0
        output = open('result_final.txt', 'w')
        for item in gather:
            for i in item:
                count += 1
                output.write("%s\n" % i)
        output.close()
        if count == 0: print "No tweets found. Adjust RADIUS."

    MPI.Finalize()
