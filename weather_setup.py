import sys
import os
import glob

def remove_legend_create_temp(directory):
    tempdir = 'tempdir'

    try:
        os.makedirs(os.path.dirname('{}/'.format(tempdir)))
    except Exception as e:
        print(e)

    headers = None
    column_names = None

    for filename in glob.glob('{}/*.csv'.format(directory)):
        with open(filename) as f:
            tempf = open('{}/{}'.format(tempdir, os.path.basename(f.name)), 'w+')
            f_lines = f.readlines()
            tempf.writelines(f_lines[17:])
            headers = f_lines[0:16]
            column_names = f_lines[16]

    return headers, [i.strip()[1:-1] for i in column_names.split(',') ], tempdir


def main(in_directory, out_path):
    headers, column_names, directory = remove_legend_create_temp(in_directory)
    with open('schema', 'w+') as schema_out:
        schema_out.writelines([i + '\n' for i in column_names])
    with open('header', 'w+') as header:
        header.writelines(headers)


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_path = sys.argv[2]
    main(in_directory, out_path)
