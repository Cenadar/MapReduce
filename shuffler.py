import tempfile


def extract_key_for_comparator(x):
    return x[0]


def decode_line(line):
    key, value = line.rstrip('\n').split(';')
    return key, value


def convert(v):
    return extract_key_for_comparator(decode_line(v))


class FileMerger:
    def __init__(self, pair):
        self.one = pair[0]
        self.two = pair[1]

    def merge(self):
        self.res = tempfile.TemporaryFile(mode='w+')
        self.one.seek(0)
        self.two.seek(0)

        self.it_one = iter(self.one)
        self.it_two = iter(self.two)

        self.next_one = next(self.it_one, None)
        self.next_two = next(self.it_two, None)

        def put_two():
            self.res.write(self.next_two)
            self.next_two = next(self.it_two, None)

        def put_one():
            self.res.write(self.next_one)
            self.next_one = next(self.it_one, None)

        while self.next_one or self.next_two:
            if not self.next_one:
                put_two()
            elif not self.next_two:
                put_one()
            else:
                if convert(self.next_one) < convert(self.next_two):
                    put_one()
                else:
                    put_two()

        return self.res


class Shuffler:
    lines_per_file = 1000

    def __init__(self, file_name):
        self.file_name = file_name

    def split_huge(self):
        temp_files = []

        lines_written = Shuffler.lines_per_file
        with open(self.file_name, 'r') as file:
            for line in file:
                if lines_written >= Shuffler.lines_per_file:
                    temp_files.append(tempfile.TemporaryFile(mode='w+'))
                    lines_written = 0

                temp_files[-1].write(line)
                lines_written += 1

        return temp_files

    def merge_couples(self, temp_files):
        lists = [[], []]
        for no, file in enumerate(temp_files):
            lists[no % 2].append(file)

        if len(lists[1]) < len(lists[0]):
            lists[1].append(tempfile.TemporaryFile('w+'))

        return [FileMerger(pair).merge() for pair in zip(lists[0], lists[1])]

    def sort_little(self, file):
        file.seek(0)
        lines = [(decode_line(line)[0], line) for line in file]

        file.seek(0)
        file.truncate()
        for line in sorted(lines, key=extract_key_for_comparator):
            file.write(line[1])

    def sort(self):
        temp_files = self.split_huge()

        for file in temp_files:
            self.sort_little(file)
            file.seek(0)

        while len(temp_files) > 1:
            temp_files = self.merge_couples(temp_files)

        temp_files[0].seek(0)
        with open(self.file_name, 'w') as file:
            for line in temp_files[0]:
                file.write(line)

    def __dump_file(self, file):
        file.seek(0)
        print(file)
        for line in file:
            print(line, end='')
