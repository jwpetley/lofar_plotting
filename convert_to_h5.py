import h5py
import casacore.tables as ct
import numpy as np
import tempfile

# Creating script to convert radio Measurement Set to hdf5 format
# Load the measurement set and then convert each table to h5py group

def ms_to_h5(ms, h5):

    with ct.table(ms) as t:
        with h5py.File(h5,'w') as f:
            table_size = t.nrows()
            colnames = t.colnames()
            for name in colnames:

                t.setmaxcachesize(name, 1024)

                desc = t.coldesc(name)
                print(desc)

                if t.isscalarcol(name):
                    print("Scalar Column")
                    f[name] = t.getcol(name)

                    continue


                type = t.colarraytype(name)
                print(type)

                if name == 'DATA' or name == 'WEIGHT_SPECTRUM':
                    step_size = 1000000
                    
                    for i in range(0, table_size, step_size):
                        if i == 0:
                            shape = desc['desc']['shape']
                            print(shape)
                            tmp = t.getcol(name, startrow = i, nrow = step_size)
                            f.create_dataset(name, data = tmp, maxshape = (None,shape[0], shape[1]),)
                            
                        else:
                            tmp = t.getcol(name, startrow = i, nrow = step_size)
                            f[name].resize((f[name].shape[0] + tmp.shape[0]), axis=0)
                            f[name][-step_size:] = tmp
                        print("Processed %s out of %s rows"%(i + step_size,table_size))
                    continue

                if type == "Direct, fixed sized arrays":

                    f[name] = t.getcol(name)

                else:
                    print(t.isvarcol(name))

if __name__ == "__main__":
    ms = "L721964_SB359_uv.MS"
    ms_to_h5(ms, 'test.h5')
