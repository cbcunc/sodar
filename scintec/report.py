"""
Check Sodar main data files for incongruities.

Usage: python report.py [/path/to/main/data/monthly/directories]

Output is copied to report.log.
"""

import os
import sys
import logging
import traceback
from datetime import datetime, timedelta
from glob import glob


def segment(lines):
    """Divide main data file into segments."""

    segment = []
    segments = [segment]
    for line in lines:
        line = line.strip()
        if line:
           segment.append(line)
        elif segment:
           segment = []
           segments.append(segment)
    if not segment:
        segments.pop()
    return segments


class ParseError(ValueError):
    pass

def parse0(segment, mnd, augmented_mnds):
    """Parse first segment for start time stamp."""

    # parse lines
    file_format = segment[0]
    start_date, start_time, file_count = segment[1].split()
    start_year, start_month, start_day = start_date.split('-')
    start_hour, start_min, start_sec = start_time.split(':')
    instrument_type = segment[2]
    comment_count, variable_count, bin_count = segment[3].split()

    # parse timestamp info
    mnd_dir, mnd_file = mnd.split(os.sep)[-2:]
    dir_year = mnd_dir[:4]
    dir_month = mnd_dir[-2:]
    file_year = mnd_file[:2]
    file_month = mnd_file[2:4]
    file_day = mnd_file[4:6]
    file_extra = mnd_file[6]
    file_extension = mnd_file[-3:]

    # string matches
    if file_format != 'FORMAT-1':
        raise ParseError('File format {0} is incorrect'.format(file_format))
    if dir_year[-2:] != file_year:
        raise ParseError('Directory year {0} is not same as file year {1}'.format(dir_year, file_year))
    elif dir_month != file_month:
        raise ParseError('Directory month {0} is not same as file month {1}'.format(dir_month, file_month))
    elif dir_year != start_year:
        raise ParseError('Directory year {0} is not same as start year {1}'.format(dir_year, start_month))
    elif dir_month != start_month:
        raise ParseError('Directory month {0} is not same as start month {1}'.format(dir_month, start_month))
    elif file_day != start_day:
        raise ParseError('File day {0} is not same as start day {1}'.format(file_day, start_day))
    elif instrument_type != 'SFAS':
        raise ParseError('Instrument type {0} is incorrect'.format(instrument_type))
    elif file_extension != 'mnd':
        raise ParseError('File extension {0} is incorrect'.format(file_extension))

    # number conversions
    start_year = int(start_year)
    start_month = int(start_month)
    start_day = int(start_day)
    start_hour = int(start_hour)
    start_min = int(start_min)
    start_sec = int(start_sec)
    file_count = int(file_count)
    comment_count = int(comment_count)
    variable_count = int(variable_count)
    bin_count = int(bin_count)

    # range checks
    if not 2008 < start_year < 2014:
        raise ParseError('Start year {0} out of range'.format(start_year))
    elif not 0 < start_month < 13:
        raise ParseError('Start month {0} out of range'.format(start_month))
    elif not 0 < start_day < 32:
        raise ParseError('Start day {0} out of range'.format(start_day))
    elif not 0 <= start_hour < 25:
        raise ParseError('Start hour {0} out of range'.format(start_hour))
    elif not 0 <= start_min < 61:
        raise ParseError('Start minute {0} out of range'.format(start_min))
    elif not 0 <= start_sec < 61:
        raise ParseError('Start second {0} out of range'.format(start_sec))

    if file_extra != os.extsep:
        augmented_mnds.append(mnd)

    return {'start_stamp':datetime(start_year, start_month, start_day, start_hour, start_min, start_sec),
            'file_count':file_count,
            'comment_count':comment_count,
            'variable_count':variable_count,
            'bin_count':bin_count,
           }


def parse1(segment, mnd, segment0):
    """Parse second segment for file-wide constants."""

    # initial comments
    isegment = iter(segment)
    line = isegment.next()
    while line.startswith('#'):
        line = isegment.next()

    # file-wide constants
    while not line.startswith('#'):
        if line.startswith('antenna azimuth angle [deg] : '):
            angle = int(line.split()[-1])
        elif line.startswith('height above ground [m]     : '):
            elevation = int(line.split()[-1])
        elif line.startswith('height above sea level [m]  : '):
            height = int(line.split()[-1])
        line = isegment.next()

    # more comments
    while line.startswith('#'):
        line = isegment.next()
    if not line.startswith('Main Data'):
        raise ParseError('Main data declaration misplaced')
    else:
        line = isegment.next()
    while line.startswith('#'):
        line = isegment.next()

    # variable names
    variables = []
    for variable in range(segment0['variable_count']):
        variables.append(line.split(' # ')[1])
        line = isegment.next()
    if variables[0] != 'z':
        raise ParseError('First variable is not z')

    if not line.startswith('error code #'):
        raise ParseError('Error code declaration misplaced')
    else:
        variables.append('error')
        line = isegment.next()

    return {'angle':angle,
            'elevation':elevation,
            'height':height,
            'variables':variables,
           }


def _parse2(index, segment, mnd, segment0, segment1):
    """Parse data segment for consistency."""

    start_date, start_time, sample_interval = segment[0].split()
    start_year, start_month, start_day = start_date.split('-')
    start_hour, start_min, start_sec = start_time.split(':')
    sample_hour, sample_min, sample_sec = sample_interval.split(':')

    # number conversions
    start_year = int(start_year)
    start_month = int(start_month)
    start_day = int(start_day)
    start_hour = int(start_hour)
    start_min = int(start_min)
    start_sec = int(start_sec)
    sample_hour = int(sample_hour)
    sample_min = int(sample_min)
    sample_sec = int(sample_sec)

    # range checks
    if not 0 <= sample_hour < 25:
        raise ParseError('Sample hour {0} out of range'.format(sample_hour))
    elif not 0 <= sample_min < 61:
        raise ParseError('Sample minute {0} out of range'.format(sample_min))
    elif not 0 <= start_sec < 61:
        raise ParseError('Sample second {0} out of range'.format(sample_sec))

    # timestamp check
    start_stamp = datetime(start_year, start_month, start_day, start_hour, start_min, start_sec)
    sample_interval = timedelta(hours=sample_hour, minutes=sample_min, seconds=sample_sec)
    if start_stamp != (segment0['start_stamp'] + (index * sample_interval)):
        raise ParseError('Mismatched file start time {0} and sample start time {1} (sample = {2}, interval = {3})'.format(
                             segment0['start_stamp'].isoformat(),
                             start_stamp.isoformat(),
                             index,
                             sample_interval.seconds))

    # variables check
    variables = segment[1].split()[1:]
    if variables != segment1['variables']:
        raise ParseError('Mismatched file variable list {0} and first sample variable list {1}'.format(
                             str(segment1['variables']),
                             str(variables)))

    # gather data
    data = []
    for line in segment[2:]:
        data.append(line.split())
    min_elevation = int(data[0][0])
    max_elevation = int(data[-1][0])

    # check data
    bin_count = len(data)
    if bin_count != segment0['bin_count']:
        raise ParseError('Mismatched file elevation count {0!s} and first sample elevation count{1!s}'.format(
                             segment0['bin_count'].
                             bin_count))
    bin_height = (max_elevation - min_elevation) / (bin_count - 1)
    for prev,next in zip(data[:-1],data[1:]):
        if (int(next[0]) - int(prev[0])) != bin_height:
            raise ParseError('Uneven elevation interval {0} to {1} compared to bin height {2!s}'.format(
                                 prev[0], next[0], bin_height))

    return {'sample_interval':sample_interval,
            'min_elevation':min_elevation,
            'max_elevation':max_elevation,
            'bin_height':bin_height,
            'bin_count':bin_count,
           }


def parse2(segments, mnd, segment0, segment1, inconsistent_segments):
    """Parse data segments for consistency."""

    segment2 = _parse2(0, segments[2], mnd, segment0, segment1)
    for index, segment in enumerate(segments[3:]):
        segmentx = _parse2(index+1, segment, mnd, segment0, segment1)
        if segmentx != segment2:
            inconsistent_segments.append((index+1, mnd))
            break

    return segment2

def log_grid(segment2):
    """Log the grid dictionary values"""

    logging.info('   Sample interval: {0} seconds'.format(segment2['sample_interval'].seconds))
    logging.info('   Minimum elevation: {0!s} meters'.format(segment2['min_elevation']))
    logging.info('   Maximum elevation: {0!s} meters'.format(segment2['max_elevation']))
    logging.info('   Bin height: {0!s} meters'.format(segment2['bin_height']))
    logging.info('   Bin count: {0!s}'.format(segment2['bin_count']))
    return None


def main(mnd_path='/seacoos/data/nccoos/level0/billymitchell/sodar1/mnd'):
    """Report on incongruities in main data files."""

    # Set up logging to file and console
    LOGFILE = os.path.splitext(os.path.abspath(sys.argv[0]))[0] + '.log'
    LEVEL = logging.DEBUG
    FORMAT = '%(asctime)s:%(levelname)s:%(message)s'
    DATEFMT = '%m/%d/%Y %I:%M:%S %p'
    logging.basicConfig(filename=LOGFILE,
                        filemode='w',
                        level=LEVEL,
                        format=FORMAT,
                        datefmt=DATEFMT)
    console = logging.StreamHandler()
    console.setLevel(LEVEL)
    console.setFormatter(logging.Formatter(fmt=FORMAT,
                                           datefmt=DATEFMT))
    logging.getLogger().addHandler(console)
    logging.info('Starting {0}'.format(sys.argv[0]))

    # Parse files
    irregular_segments = []
    insufficient_segments = []
    inconsistent_segments = []
    augmented_mnds = []
    incongruent_mnds = []
    exceptional_mnds = []
    prev_mnd = None
    prev_segment0 = None
    prev_segment1 = None
    prev_segment2 = None
    paths = glob(os.path.join(mnd_path,'*'))
    paths.sort()
    for path in paths:
        mnds = glob(os.path.join(mnd_path, path, '*'))
        mnds.sort()
        for mnd in mnds:
            logging.info('Examining {0}'.format(mnd))
            with open(mnd) as handle:
                try:
                    lines = handle.readlines()
                    segments = segment(lines)
                    if len(segments) != 50:
                        irregular_segments.append((mnd, len(segments)))
                    elif len(segments) < 3:
                        insufficient_segments.append(mnd)
                        continue
                    else:
                        segment0 = parse0(segments[0], mnd, augmented_mnds)
                        segment1 = parse1(segments[1], mnd, segment0)
                        segment2 = parse2(segments, mnd, segment0, segment1, inconsistent_segments)
                        if prev_mnd:
                            if segment2 != prev_segment2:
                                incongruent_mnds.append((mnd, segment2))
                        else:
                            incongruent_mnds.append((mnd, segment2))
                        prev_mnd = mnd
                        prev_segment0 = segment0
                        prev_segment1 = segment1
                        prev_segment2 = segment2
                except:
                    exceptional_mnds.append(mnd)
                    for tline in traceback.format_exc().splitlines():
                        logging.critical(tline)

    # Spit out reports
    logging.info('***Begin irregular segments report***')
    if irregular_segments:
        for mnd, count in irregular_segments:
            logging.info('Irregular number of segments in {0} = {1}'.format(mnd, count))
    else:
        logging.info('No irregular segments.')
    logging.info('***End irregular segments report***')

    logging.info('***Begin insufficient segments report***')
    if insufficient_segments:
        for mnd in insufficient_segments:
            logging.info('Insuficient segments in {0}'.format(mnd))
    else:
        logging.info('No insufficient segments.')
    logging.info('***End insufficient segments report***')

    logging.info('***Begin inconsistent segments report***')
    if inconsistent_segments:
        for index, mnd in inconsistent_segments:
            logging.info('Inconsistent segment at sample {0} in {1}'.format(index, mnd))
    else:
        logging.info('No inconsistent segments.')
    logging.info('***End inconsistent segments report***')

    logging.info('***Begin augmented main data files report***')
    if augmented_mnds:
        for mnd in augmented_mnds:
            logging.info('Augmented main data file in {0}'.format(mnd))
    else:
        logging.info('No augmented main data files.')
    logging.info('***End augmented main data files report***')

    logging.info('***Begin incongruent main data files report***')
    if incongruent_mnds:
        segment2 = incongruent_mnds[0][1]
        logging.info('Initial gridding:')
        log_grid(segment2)
        for mnd, segment2 in incongruent_mnds[1:]:
            logging.info('Inconguity occurred at {0}'.format(mnd))
            log_grid(segment2)
    else:
        logging.info('No inconguent main data files.')
    logging.info('***End inconguent main data files report***')

    logging.info('***Begin exceptional main data files report***')
    if exceptional_mnds:
        for mnd in exceptional_mnds:
            logging.info('Exceptional main data file {0}'.format(mnd))
    else:
        logging.info('No exceptional main data files.')
    logging.info('***End exceptional main data files report***')

    logging.info('Exiting {0}'.format(sys.argv[0]))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()
