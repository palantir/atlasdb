#! /usr/bin/python

from datetime import datetime, timedelta
from dateutil import parser as date_parser
import argparse
import re
import os
import sys
import unittest


def accept_line(line):
    return not line.startswith('!')


def timestamp_from_string(string):
    def localize(d):
        return d.replace(tzinfo=None)

    try:
        d = datetime.strptime(string, "%Y-%m-%d %H:%M:%S,%f")
        return localize(d)
    except:
        d = date_parser.parse(string, fuzzy=True)
        return localize(d)


def extract_timestamp(line):
    pattern = re.compile('.*?\[([^]]+)\].*')
    match = pattern.match(line)

    if match:
        timestamp_string = match.groups()[0]
        line_without_timestamp = line.replace('[{}]'.format(timestamp_string), '')
        try:
            timestamp = timestamp_from_string(timestamp_string)
            return timestamp, line_without_timestamp
        except ValueError as e:
            return None, line
    return None, line


def remove_common_prefix_from_strings(strings):
    def is_substr(find, data):
        if len(data) < 1 and len(find) < 1:
            return False
        for i in range(len(data)):
            if find not in data[i]:
                return False
        return True

    def long_substr(data):
        substr = ''
        if len(data) > 1 and len(data[0]) > 0:
            for i in range(len(data[0])):
                for j in range(len(data[0])-i+1):
                    if j > len(substr) and is_substr(data[0][i:i+j], data):
                        substr = data[0][i:i+j]
        return substr
    substr = long_substr(strings)
    return [s.replace(substr, '') for s in strings]


class TestParser(unittest.TestCase):
    _test_line_1 = 'INFO  [2016-11-29 13:18:35,918] io.atomix.copycat.server.state.FollowerState: n1/10.0.0.2:8700 - Polling members [ServerMember[type=ACTIVE, status=AVAILABLE, serverAddress=n2/10.0.0.3:8700, clientAddress=n2/10.0.0.3:8700], ServerMember[type=ACTIVE, status=AVAILABLE, serverAddress=n3/10.0.0.4:8700, clientAddress=n3/10.0.0.4:8700], ServerMember[type=ACTIVE, status=AVAILABLE, serverAddress=n4/10.0.0.5:8700, clientAddress=n4/10.0.0.5:8700], ServerMember[type=ACTIVE, status=AVAILABLE, serverAddress=n5/10.0.0.6:8700, clientAddress=n5/10.0.0.6:8700]]'
    _test_line_2 = 'INFO  [2016-11-30 16:17:19,081] com.palantir.atlasdb.timelock.TimeLockServer: I think the group is [597eca15-6d1a-45ed-b68f-82f7362e97da, 444debad-708c-4514-a86a-c8caf21bc46a, 375cff01-9b21-43e1-8d5e-ba98fcd41ca7, 39f13da7-5cd1-46c4-8b4f-079957095f1d, 16be3ab7-c2c3-407f-9fba-c488a74da7bc]'

    def test_line_1(self):
        value, _ = extract_timestamp(self._test_line_1)
        expected = datetime(2016, 11, 29, 13, 18, 35, 918000)

        self.assertEqual(expected, value)

    def test_line_2(self):
        value, _ = extract_timestamp(self._test_line_2)
        expected = datetime(2016, 11, 30, 16, 17, 19, 81000)

        self.assertEqual(expected, value)


def run_tests_silently():
    """ Returns: a boolean of whether the tests passed """
    runner = unittest.TextTestRunner(stream=open(os.devnull, 'w'))
    result = runner.run(unittest.makeSuite(TestParser))
    return not(result.failures or result.errors)


def main(argv):
    parser = argparse.ArgumentParser(
        description=('Combines many log files together by parsing each line '
                     'for a timestamp and printing all lines in chronological '
                     'order. Any lines starting with ! are presumed to be '
                     'stacktraces and will be ommitted.'))
    parser.add_argument('file', type=argparse.FileType('r'), nargs='*',
                        help='Input log files on disk')
    parser.add_argument('--tests-only', action='store_true',
                        help='Just run unit tests, then exit')
    args = parser.parse_args(argv[1:])

    # run tests
    if not run_tests_silently():
        sys.stderr.write('Tests failed. Fix them or run with --skip-tests\n')
        return 1
    if args.tests_only:
        print('Tests passed')
        return 0

    # make the filenames shorter
    filenames = [f.name for f in args.file]
    shorter_filenames = remove_common_prefix_from_strings(filenames)

    # parse the incoming files
    parsed_lines = []
    for file, short_filename in zip(args.file, shorter_filenames):
        for line in file.readlines():
            line = line.replace('\n', '')
            last_timestamp = datetime.fromtimestamp(0) + timedelta(milliseconds=1)
            if accept_line(line):
                # try to read a timestamp from the line
                timestamp, line_without_timestamp = extract_timestamp(line)

                # if you couldn't read it, just use the last one you could read
                if timestamp is None:
                    timestamp = last_timestamp

                # update last timestamp for next time
                last_timestamp = timestamp

                # push back on to array
                parsed_lines.append((timestamp,
                                     short_filename,
                                     line_without_timestamp))

    # sort line by timestamp and print
    sorted_parsed_lines = sorted(parsed_lines, key=lambda x: x[0])
    last_file = None
    for t, file, line in sorted_parsed_lines:
        output = '{}: {} {}'.format(str(file), t, line)
        if file != last_file:
            print('-------------------------------')
        print(output)
        last_file = file


if __name__ == '__main__':
    sys.exit(main(sys.argv))
