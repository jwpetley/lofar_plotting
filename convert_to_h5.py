import h5py
import casacore.tables as ct
import numpy as np
import tempfile

# Creating script to convert radio Measurement Set to hdf5 format
# Load the measurement set and then convert each table to h5py group

def ms_to_h5(ms, h5):

    with ct.table(ms) as t:
        with h5py.File(h5,'w') as f:
            colnames = t.colnames()
            for name in colnames:

                t.setmaxcachesize(name, 5012)

                desc = t.coldesc(name)
                print(desc)

                if t.isscalarcol(name):
                    print("Scalar Column")
                    continue


                type = t.colarraytype(name)
                print(type)


                if type == "Direct, fixed sized arrays":

                    f[name] = t.getcol(name)

                else:
                    f[name] = t.getvarcol(name)

if __name__ == "__main__":
    ms = "L721964_SB359_uv.MS"
    ms_to_h5(ms, 'test.h5')
