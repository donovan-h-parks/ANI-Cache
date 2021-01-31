###############################################################################
#                                                                             #
#    This program is free software: you can redistribute it and/or modify     #
#    it under the terms of the GNU General Public License as published by     #
#    the Free Software Foundation, either version 3 of the License, or        #
#    (at your option) any later version.                                      #
#                                                                             #
#    This program is distributed in the hope that it will be useful,          #
#    but WITHOUT ANY WARRANTY; without even the implied warranty of           #
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the            #
#    GNU General Public License for more details.                             #
#                                                                             #
#    You should have received a copy of the GNU General Public License        #
#    along with this program. If not, see <http://www.gnu.org/licenses/>.     #
#                                                                             #
###############################################################################

import os
import sys
import logging
import ntpath
import time
import datetime
import csv
import sqlite3

from ani_cache.fastani import FastANI
from ani_cache.utils import check_file_exists, make_sure_path_exists


class OptionsParser():
    """Validate input and execute command line interface."""

    def __init__(self):
        """Initialize."""

        self.logger = logging.getLogger('timestamp')

    def _input_files(self, input_str, file_ext, validate_genome_files):
        """Identify genomes files.

        Parameters
        input : str
            File or directory specifying input files to process.
        file_ext : str
            Extension of files to process.

        Returns
        -------
        list
            Name of files to process.
        """

        input_files = []
        if os.path.isfile(input_str):
            for line in open(input_str):
                input_file = line.strip().split('\t')[0]
                if validate_genome_files and not os.path.exists(input_file):
                    self.logger.error(
                        f'Specified input file does not exist: {input_file}')
                    sys.exit(-1)
                input_files.append(input_file)

            if not input_files:
                self.logger.warning(
                    f'No genomes found in file: {input_str}. Check that the file has the correct format.')
                sys.exit(-1)
        elif os.path.isdir(input_str):
            for f in os.listdir(input_str):
                if f.endswith(file_ext):
                    input_files.append(os.path.join(input_str, f))

            if not input_files:
                self.logger.warning(
                    f'No genomes found in directory: {input_str}. Check the --file_ext flag used to identify genomes.')
                sys.exit(-1)
        else:
            self.logger.error(f'Specified input file or directory does not exists: {input_str}')
            sys.exit(-1)

        return input_files

    def _genome_comparisons(self, query_files, ref_files, ref_to_query):
        """Determine desired genomes comparisons."""

        gid_pairs = []
        genome_files = {}
        for qf in query_files:
            qid = ntpath.basename(qf)
            genome_files[qid] = qf
            for rf in ref_files:
                rid = ntpath.basename(rf)
                genome_files[rid] = rf

                gid_pairs.append([qid, rid])
                if ref_to_query:
                    gid_pairs.append([rid, qid])

        return gid_pairs, genome_files

    def fastani(self, args):
        """Run FastANI and cache results."""

        query_files = self._input_files(args.query_genomes, args.file_ext, args.validate_genome_files)
        ref_files = self._input_files(args.ref_genomes, args.file_ext, args.validate_genome_files)
        make_sure_path_exists(args.output_dir)

        self.logger.info('Identified {:,} query and {:,} reference genomes.'.format(
            len(query_files),
            len(ref_files)))

        # get genome pairs to be considered
        gid_pairs, genome_files = self._genome_comparisons(query_files, ref_files, args.ref_to_query)
        self.logger.info('Calculating ANI between {:,} genome pairs.'.format(
            len(gid_pairs)))

        # calculate ANI between genome pairs
        start = time.time()
        fastani = FastANI(args.ani_db_file, args.cpus)

        try:
            ani_af = fastani.pairs(gid_pairs,
                                   genome_files,
                                   report_progress=True,
                                   initial_cache_check=args.initial_cache_check)

            results_file = os.path.join(args.output_dir, 'ani_af.tsv')
        except Exception as e:
            print(e)
            raise
        finally:
            fastani.write_cache()

        fastani.write_ani_af(results_file, ani_af)

        if args.query_genomes == args.ref_genomes:
            # since pairwise calculations were performed
            # also write out results as matrices
            ani_matrix_file = os.path.join(args.output_dir, 'ani_matrix.tsv')
            fastani.write_ani_matrix(ani_matrix_file, ani_af)

            af_matrix_file = os.path.join(args.output_dir, 'af_matrix.tsv')
            fastani.write_ani_matrix(af_matrix_file, ani_af)

        elapsed_time_str = str(datetime.timedelta(seconds=round(time.time() - start)))
        self.logger.info(f'Time to calculate ANI values (h:mm:ss): {elapsed_time_str}')

        self.logger.info('Done.')

    def dump(self, args):
        """Write ANI database to human-readable file."""

        check_file_exists(args.ani_db_file)

        db_conn = sqlite3.connect(args.ani_db_file)
        db_cur = db_conn.cursor()

        delimiter = ','
        if args.format == 'TSV':
            delimiter = '\t'

        self.logger.info(f'Writing database to {args.output_file}.')
        db_cur.execute("select * from ani_table;")
        with open(args.output_file, "w", newline='') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=delimiter)
            csv_writer.writerow([i[0] for i in db_cur.description])
            csv_writer.writerows(db_cur)

        self.logger.info('Done.')

    def parse_options(self, args):
        """Parse user options and call the correct pipeline(s)"""

        if args.subparser_name == 'fastani':
            self.fastani(args)
        elif args.subparser_name == 'dump':
            self.dump(args)
        else:
            self.logger.error(f'Unknown ANI-Cache command: {args.subparser_name}\n')
            sys.exit(-1)
