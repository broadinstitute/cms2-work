import pandas as pd

with pd.HDFStore('default.all_component_stats.h5', mode='r') as store:
    hapset_data = store['hapset_data']
    hapset_data.info(verbose=True, show_counts=True)
    hapset_data.index.to_frame().info(verbose=True, show_counts=True)

    hapset_metadata = store['hapset_metadata']
    hapset_metadata.info(verbose=True, show_counts=True)
    hapset_metadata.index.to_frame().info(verbose=True, show_counts=True)

    print(hapset_metadata.describe())
    print(hapset_metadata)
    
    #print('\n'.join(hap.columns)+'\n')


    
