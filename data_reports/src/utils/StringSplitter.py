import pandas as pd
import multiprocessing as mp
import traceback

class StringSplitter:
    
    # def __init__(self, df_splits):
    #     self.df_splits = df_splits

    # def split_string(self, string):
    #     result = {}

    #     for _, row in self.df_splits.iterrows():

    #         split_name = row['VARI_NOMBRE']
    #         start = row['VARI_INICIO']
    #         length = row['VARI_ACUMULADO']            
    #         result[split_name] = string[start:length]
            

    #     return result

    # def process_chunk(self, chunk):        
    #     return [self.split_string(string) for string in chunk]

    # def parallel_split(self, df_strings):
    #     num_chunks = mp.cpu_count()
    #     print("Trozos : ", num_chunks)
    #     chunks = [df_strings['APRD_CADENA'][i::num_chunks] for i in range(num_chunks)]
    #     print("La longitud es : ", len(chunks[19]))

    #     with mp.Pool(num_chunks) as pool:
    #         results = pool.map(self.process_chunk, chunks)
        
    #     split_results = [item for sublist in results for item in sublist]        
    #     df_split_results = pd.DataFrame(split_results)
    #     # df_merged = pd.concat([df_strings, df_split_results], axis=1)
    #     df_merged = df_split_results
        
    #     return df_merged

    def __init__(self, df_splits, chunk_size = 1000):
        self.df_splits = df_splits
        self.chunk_size = chunk_size

    def split_string(self, string, string_id):
        
        result = {}
        splits = self.df_splits[self.df_splits['INCO_ID'] == string_id]
        
        for _, row in splits.iterrows():
            split_name = row['VARI_NOMBRE']
            start = row['VARI_INICIO']
            length = row['VARI_ACUMULADO']            
            result[split_name] = string[start:length]

        
        return result

    # def process_chunk(self, chunk, string_ids, chunk_index, total_chunk):
    def process_chunk(self, args):        
        
        chunk, string_ids, chunk_index, total_chunk = args

        try:
            #result = [self.split_string(string, string_id) for string, string_id in zip(chunk, string_ids)]
            result = [self.split_string(string, string_id) for string, string_id in zip(chunk, string_ids)]
            print(f"Completed chunk {chunk_index + 1} of {total_chunk}")
            return result
        except Exception as e:
            print(f"Error in process_chunck: {e}")
            traceback.print_exc()
            return []
        
    # def update_progress(self):
    #     self.counter.value += 1        
    #     print(f"Completed chunk {self.counter.value} of {self.total_chunks}")
        

    def parallel_split(self, df_strings):

        # num_chunks = mp.cpu_count()
        num_chunks = min(mp.cpu_count(), len(df_strings) // self.chunk_size + 1)
        total_chunks = num_chunks
        # self.counter = mp.Value('i', 0)
        print(f"\nNumber of chunks (CPU cores available): {num_chunks}")


        #chunks = [df_strings.iloc[i*self.chunk_size:(i+1)*self.chunk_size,1] for i in range(num_chunks)]  
        #string_ids = [df_strings['INCO_ID'].iloc[i*self.chunk_size:(i+1)*self.chunk_size] for i in range(num_chunks)]

        chunks = [df_strings.iloc[i::num_chunks,1] for i in range(num_chunks)]  
        string_ids = [df_strings['INCO_ID'].iloc[i::num_chunks] for i in range(num_chunks)]
        #print("Estos son los strings_id : ", string_ids)
        print(f"Data split into {len(chunks)} chunks.")

        args_list = [(chunks[i], string_ids[i], i, total_chunks) for i in range(num_chunks)]

        with mp.Pool(num_chunks) as pool:
            # print(chunks[1])
            #print(string_ids[1])
            # results = [pool.apply_async(self.process_chunk, args=(chunks[i], string_ids[i], i, total_chunks)) for i in range(num_chunks)]
            split_results = list(pool.imap(self.process_chunk, args_list))


            # pool.close()
            # pool.join()
            # print(results[1])
            #results = [result.get() for result in results]
            #print(f"Results collected from each chunk: {len(results)}")

        #split_results = [item for sublist in results for item in sublist]
        split_results = [item for sublist in split_results for item in sublist]
        print(f"Total split results: {len(split_results)}")

        #df_split_results = pd.DataFrame(split_results)
        #df_merged = pd.concat([df_strings.reset_index(drop=True), df_split_results], axis=1)

        df_split_results = pd.DataFrame(split_results)
        # df_merged = pd.concat([df_strings, df_split_results], axis=1)
        df_merged = df_split_results

        return df_merged