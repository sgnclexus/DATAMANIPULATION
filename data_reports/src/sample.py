import pandas as pd

df_strings = pd.DataFrame({
        'string_id': [1, 2, 3],
        'strings': ['AAABBBBCCCCCC', 'DDDEEEEFFFFFF', 'GGGHHHHIIIIII']
    })

print(df_strings)

    # Sample DataFrame containing split definitions with IDs
df_splits = pd.DataFrame({
    'string_id': [1, 1, 1, 2, 2, 3, 3],
    'split_name': ['split1', 'split2', 'split3', 'split1', 'split2', 'split1', 'split3'],
    'start': [0, 3, 7, 0, 3, 0, 7],
    'length': [3, 4, 6, 3, 4, 3, 6]
})

print(df_splits)