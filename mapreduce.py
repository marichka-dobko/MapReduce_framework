import os, json
from multiprocessing import Process
from handlefiles import HandleFiles


class MapReduce(object):
    def __init__(self, input_dir, output_dir, extension='.txt', num_mappers=3, num_reducers=2, combiner=True):
        """
        input_dir: input files directory
        output_dir: output files directory
        combiners : True or False, use or not to use combiners, default=True
        n_mappers: number of mapper threads, default=3
        n_reducers: number of reducer threads, default=2
        extension: file extension '.txt' or '.csv'
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.combiner = combiner
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.extension = extension

    def run(self, join=False):
        """Runs map-reduce algorithm"""

        # create new small files into new directory input_dir + 'map'
        hf = HandleFiles(self.input_dir, self.extension)

        split_res = hf.split(self.num_mappers)
        if split_res != 0:
            map_procs, reduce_procs = [], []

            # Map
            for thread_id in range(self.num_mappers):
                proc_map = Process(target=self.run_map, args=(thread_id,))
                proc_map.start()
                map_procs.append(proc_map)
            [proc_res.join() for proc_res in map_procs]

            # Reduce
            for thread_id in range(self.num_reducers):
                proc_rdc = Process(target=self.run_reduce, args=(thread_id,))
                proc_rdc.start()
                map_procs.append(proc_rdc)
            [rdc_res.join() for rdc_res in reduce_procs]
        else:
            "input_dir should be a file"

    def combine(self):
        pass

    def read_dir(self, index, ext='txt'):
        if not (self.input_dir is None):
            return self.input_dir + '_map/' + str(index) + ext
        return 0

    def get_map_file(self, id, reduce_id, ext='txt'):
        return self.output_dir + "/map_files" + str(id) + "-" + str(reduce_id) + ext

    def run_map(self, id):
        """Runs mapper"""
        mapper = Mapper(self.read_dir(id))
        mapper_res = mapper.map()

        if self.combiner:
            # TODO: implement combiner function
            pass

        for reduce_id in range(self.num_reducers):  # because reducers will use the results in these files
            temp_map_file = open(self.get_map_file(id, reduce_id), "w")
            json.dump([(key, value) for (key, value) in mapper_res if self.shuffle(key, reduce_id)]
                      , temp_map_file)
            temp_map_file.close()

    def shuffle(self, key, reduce_id):
        return reduce_id == (hash(key) % self.num_reducers)

    def run_reduce(self, id):
        """ Runs reducer """
        key_values_map = {}

        for mapper_id in range(self.num_mappers):
            temp_map_file = open(self.get_map_file(mapper_id, id), "r")
            mapper_results = json.load(temp_map_file)
            for (key, value) in mapper_results:
                if not (key in key_values_map):
                    key_values_map[key] = []
                try:
                    key_values_map[key].append(value)
                except Exception as e:
                    pass

            temp_map_file.close()
            os.unlink(self.get_map_file(mapper_id, id))  # deleting temporary mapper files

        kv_list = Reducer(key_values_map).reduce()

        output = open(self.output_dir + "/reduce_file_" + str(id) + self.extension, "w+")
        json.dump(kv_list, output)
        output.close()


class Mapper():
    """This class should be implemented by user."""

    def __init__(self, input_file_name):
        self.input_file_name = input_file_name

    def map(self):
        input_file = open(self.input_file_name, 'r')
        key = input_file.readline()
        value = input_file.read()
        input_file.close()

        # TODO: write your code here!

        return []


class Reducer():
    """This class should be implemented by user."""

    def __init__(self, key_values_map):
        self.key_values_map = key_values_map

    def reduce(self):
        kv_list = []
        for key in self.key_values_map:
            kv_list.append((key, self.key_values_map[key]))

        # TODO: write your code here!

        return kv_list
