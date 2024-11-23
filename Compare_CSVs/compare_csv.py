import csv
import os
import queue


curPath = os.path.dirname(os.path.abspath(__file__))
file1 = os.path.join(curPath, 'source1.txt')
file2 = os.path.join(curPath, 'source2.txt')
destination = os.path.join(curPath, 'result.txt')

output = queue.Queue()
def read_csv(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        return list(reader)

def find_common_lines(file1, file2):
    data1 = read_csv(file1)
    data2 = read_csv(file2)

    # Create a dictionary for quick lookup of rows by the first column
    data1_dict = {row[0]: row for row in data1}
    data2_dict = {row[0]: row for row in data2}

    # Find common keys
    common_keys = set(data1_dict.keys()).intersection(data2_dict.keys())

    # List common lines as original lines from data2_dict
    common_lines = [data2_dict[key] for key in common_keys]

    return common_lines

def main():
    common_lines = find_common_lines(file1, file2)
    with open(destination, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        for line in common_lines:
            # Add double quotes around the first and last columns
            # line[0] = f'"{line[0]}"'
            # line[-1] = f'"{line[-1]}"'
            writer.writerow(line)
def write_to_file(queue,destination):
    queueSize = queue.qsize()
    with open(destination, 'a') as d:
        for i in range(queueSize):
            d.write(str(queue.get()))

if __name__ == "__main__":
    main()
    write_to_file(output, destination)