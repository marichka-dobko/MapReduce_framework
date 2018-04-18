import os, sys


class HandleFiles(object):
    """To work with input and output files"""

    def __init__(self, input_dir, extension):
        self.input_dir = input_dir
        self.extension = extension

    def split(self, n_split):
        if not os.path.exists('./temp'):
            os.makedirs('./temp')

        if self.extension == '.txt':
            return self.split_txt(n_split)
        elif self.extension == '.csv':
            return self.split_csv(n_split)

    def split_txt(self, n_split):
        # from one file create self.input_dir + '_map/' + str(index) + ext

        if not os.path.isfile(self.input_dir):
            return 0
        file = open(self.input_dir, "r")
        data = file.read()
        file.close()

        small_f_size = os.path.getsize(self.input_dir) / n_split + 1
        index, cur_split_index = 1, 1

        current_split_unit = self.start_split(cur_split_index, index)
        for character in data:
            current_split_unit.write(character)
            if index > small_f_size * cur_split_index + 1 and character.isspace():
                current_split_unit.close()
                cur_split_index += 1
                current_split_unit = self.start_split(cur_split_index, index)
            index += 1
        current_split_unit.close()

        return

    def split_csv(self, n_split):
        pass

    def start_split(self, spl_id, id):
        file_split = open('./temp' + "/file_" + str(spl_id - 1) + self.extension, "w")
        file_split.write(str(id) + "\n")

        return file_split

# hf = HandleFiles('./test_file.txt', '.txt')
# hf.split(3)
